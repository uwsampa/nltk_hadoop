//
// Compute cosine similarity between incidence matrix stored in HDFS Avro files.
//
// Run with command like "/srun-with-classpath.sh  ./cosine_similarity --json_base=/patents/output/tfidf --result_file=/shared/patents/cosinesim.tsv"
// or compare with Jevin's DB with command like "/srun-with-classpath.sh  ./cosine_similarity --json_base=/patents/output/tfidf --compare_db=/shared/patents/cosine.db"
//

#include <algorithm>
#include <limits>
#include <string>
#include <iostream>
#include <unordered_map>
#include <vector>
#include <tuple>
#include <cstdlib>

#include <boost/iostreams/stream.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Specific.hh>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>

#include <eigen3/Eigen/SparseCore>
#include <eigen3/unsupported/Eigen/SparseExtra>

#include <google/gflags.h>

#include <sqlite3.h>

#include "HDFS.hpp"

DEFINE_string( json_base,   "", "Read incidence matrix triples from files starting with this HDFS path prefix" );

DEFINE_string( input_base,  "", "Read incidence matrix and patent and bigram indices from files starting with this NFS path prefix rather than HDFS" );
DEFINE_string( output_base, "", "Write incidence matrix and patent and bigram indices to files starting with this NFS path prefix" );

DEFINE_string( result_file, "", "Write cosine similarity results to this NFS path" );
DEFINE_string( result_format, "tsv", "Write cosine similarity results in this format (one of {tsv,tsjson})" );

DEFINE_string( patent_field, "key.uid",     "Full name of JSON field containing patent ID" );
DEFINE_string( ngram_field,  "key.ngram",   "Full name of JSON field containing n-gram" );
DEFINE_string( tfidf_field,  "value.tfidf", "Full name of JSON field containing TF-IDF value" );

DEFINE_string( compare_db, "", "Compare cosine similarity results with those in this database" );

using namespace std;
namespace pt = boost::property_tree;


enum class ResultFormat { TSV, TSJSON };

int main( int argc, char * argv[] ) {
  google::ParseCommandLineFlags( &argc, &argv, true);
  google::SetUsageMessage( R"usage(
  This program computes cosine similarity between TF-IDF vectors of patents.
  You must set one of --json_base or --input_base to load data for computation.
  You must set --result_file to save the result.
)usage" );

  // make sure we have some input to work with
  if( FLAGS_json_base == "" && FLAGS_input_base == "" ) {
    google::ShowUsageWithFlags( argv[0] );
    exit(1);
  }

  ResultFormat result_format;
  std::transform( FLAGS_result_format.begin(),  FLAGS_result_format.end(),  FLAGS_result_format.begin(), ::toupper );
  if( FLAGS_result_format == "TSV" )    result_format = ResultFormat::TSV;
  if( FLAGS_result_format == "TSJSON" ) result_format = ResultFormat::TSJSON;

  int retval;
  size_t record_count = 0;

  // translate patent ID's to matrix indices
  std::unordered_map< std::string, int > patentMap;
  std::vector< std::string > inversePatentMap;
  int current_patent_index = 0;

  // translate bigrams to matrix indices
  std::unordered_map< std::string, int > bigramMap;
  int current_bigram_index = 0;

  // store nonzeros entries of matrix before matrix construction
  std::vector< Eigen::Triplet< double > > triplets;
  triplets.reserve( 1 << 23 ); // guess at 8M nonzeros to start

  Eigen::SparseMatrix<double, Eigen::RowMajor> incidence_matrix;

  // load TF-IDF triplets from HDFS
  if( FLAGS_input_base == "" ) {
    std::cerr << "Loading incidence matrix triples from HDFS...." << std::endl;

    // connect to HDFS
    HDFS::Connection conn("hadoop.sampa");

    // for each file in path
    for( auto fileInfo : HDFS::FileInfoIterator( conn, FLAGS_json_base ) ) {
      // process file if it looks worthwhile
      if( fileInfo.mKind == kObjectKindFile &&    // only read files, not directories
          fileInfo.mSize > 0 ) {                  // skip empty files
        std::cerr << "Reading file " << fileInfo.mName << std::endl;

        // open file in HDFS as stream; read as Avro "DataFile" containing string records
        boost::iostreams::stream< HDFS::Source > in( conn, fileInfo.mName );
        avro::DataFileReader< std::string > reader( in );

        std::string s;
        while( reader.read(s) ) {
          // parse JSON in string
          std::stringstream ss(s);
          boost::property_tree::ptree pt;
          boost::property_tree::read_json(ss, pt);

          std::string patent_id, bigram;
          double tfidf;
          try {
            patent_id = pt.get<std::string>( FLAGS_patent_field );
            bigram = pt.get<std::string>( FLAGS_ngram_field );
            tfidf = pt.get<double>( FLAGS_tfidf_field );
          } catch( boost::property_tree::ptree_error & e ) {
            std::cerr << "Couldn't find one of patent_field=" << FLAGS_patent_field << ", "
                      << "ngram_field=" << FLAGS_ngram_field << ", "
                      << "tfidf_field=" << FLAGS_tfidf_field << " in: ";
            boost::property_tree::write_json( std::cerr, pt );
            throw e;
          }

          if( patentMap.find( patent_id ) == patentMap.end() ) {
            inversePatentMap.push_back(patent_id);
            patentMap[patent_id] = current_patent_index++;
          }

          if( bigramMap.find( bigram ) == bigramMap.end() ) {
            bigramMap[bigram] = current_bigram_index++;
          }

          triplets.push_back( Eigen::Triplet< double >( patentMap[patent_id], bigramMap[bigram], tfidf ) );

          ++record_count;
        }
      }
    }
    std::cerr << "Done reading. Record count: " << record_count
              << ", with " << current_patent_index << " patents and "
              << current_bigram_index << " bigrams."
              << std::endl;

    std::cerr << "Populating incidence matrix." << std::endl;
    incidence_matrix.resize( current_patent_index, current_bigram_index );
    incidence_matrix.reserve( current_bigram_index * 10 );
    incidence_matrix.setFromTriplets( triplets.begin(), triplets.end() );

    std::cerr << "Formed incidence matrix with " << incidence_matrix.rows() << " rows, "
              << incidence_matrix.cols() << " columns, "
              << incidence_matrix.nonZeros() << " nonzeros."
              << std::endl;
  } else { // load from saved incidence matrix and maps
    auto matrix_in = FLAGS_input_base + "-incidenceMatrix.mtx";
    std::cerr << "Loading incidence matrix from " << matrix_in << std::endl;
    Eigen::loadMarket( incidence_matrix, matrix_in );

    auto patent_in = FLAGS_input_base + "-patentMap.tsv";
    std::cerr << "Loading patent map from " << patent_in << std::endl;
    {
      std::ifstream in( patent_in, std::ios::in );
      std::string patent_id;
      std::string patent_index;
      while( std::getline( in, patent_id, '\t' ) &&
             std::getline( in, patent_index ) ) {
        int index = std::stoi( patent_index );
        patentMap[patent_id] = index;
      }
      inversePatentMap.resize( patentMap.size() );
      for( auto it : patentMap ) {
        inversePatentMap[ it.second ] = it.first;
      }
    }

    auto bigram_in = FLAGS_input_base + "-bigramMap.tsv";
    std::cerr << "Loading bigram map from " << bigram_in << std::endl;
    {
      std::ifstream in( bigram_in, std::ios::in );
      std::string bigram;
      std::string bigram_index;
      while( std::getline( in, bigram, '\t' ) &&
             std::getline( in, bigram_index ) ) {
        bigramMap[bigram] = std::stoi( bigram_index );
      }
    }
  }

  // save incidence matrix and maps
  if( FLAGS_output_base != "" ) {
    auto matrix_out = FLAGS_output_base + "-incidenceMatrix.mtx";
    std::cerr << "Saving incidence matrix to " << matrix_out << std::endl;
    Eigen::saveMarket( incidence_matrix, matrix_out );

    auto patent_out = FLAGS_output_base + "-patentMap.tsv";
    std::cerr << "Saving patent map to " << patent_out << std::endl;
    {
      std::ofstream out( patent_out, std::ios::out | std::ios::trunc );
      for( auto it = patentMap.begin(); it != patentMap.end(); ++it ) {
        out << it->first << "\t" << it->second << std::endl;
      }
    }

    auto bigram_out = FLAGS_output_base + "-bigramMap.tsv";
    std::cerr << "Saving bigram map to " << bigram_out << std::endl;
    {
      std::ofstream out( bigram_out, std::ios::out | std::ios::trunc );
      for( auto it = bigramMap.begin(); it != bigramMap.end(); ++it ) {
        out << it->first << "\t" << it->second << std::endl;
      }
    }
  }

  if( FLAGS_compare_db == "" ) {
    std::cerr << "Computing similarities..." << std::endl;
    bool write_output = FLAGS_result_file != "";
    bool found_bad_self_comparison = false;

    std::ofstream out;
    if( write_output ) {
      out.open( FLAGS_result_file, std::ios::out | std::ios::trunc );
    }

    for( int i = 0; i < incidence_matrix.rows(); ++i ) {
      auto v1 = incidence_matrix.innerVector(i);
#pragma omp parallel
      {
        std::ostringstream os;

#pragma omp for schedule(guided)
        for( int j = i; j < incidence_matrix.rows(); ++j ) {
          auto v2 = incidence_matrix.innerVector(j);
          auto val = v1.dot(v2);

          if( write_output ) {
            switch( result_format ) {
            case ResultFormat::TSV:
              os << inversePatentMap.at(i) << "\t" << inversePatentMap.at(j) << "\t" << val << "\n";
              break;
            case ResultFormat::TSJSON:
              os << "\"[\\\"" << inversePatentMap.at(i) << "\\\", \\\"" << inversePatentMap.at(j) << "\\\"]\"\t" << val << "\n";
              break;
            }
          }

          if( i == j &&
              std::abs( 1.0 - val ) > 0.000001 &&
              found_bad_self_comparison == false) {
            std::cerr << "Warning: found at least one patent whose self-similarity was not 1 ("
                      << inversePatentMap.at(i) << " <dot> " << inversePatentMap.at(i)
                      << " == " << val << ")" << std::endl;
            found_bad_self_comparison = true;
          }
        }

        if( write_output ) {
#pragma omp critical
          out << os.str();
        }
      }

      // note progress
      if( ((i+1) % 10000) == 0 ) {
        std::cerr << i << " patents compared..." << std::endl;
      }
    }

  } else {
    std::cerr << "Comparing similarities with " << FLAGS_compare_db << "..." << std::endl;

    bool write_output = FLAGS_result_file != "";
    sqlite3 * db;

    std::ofstream out;
    if( write_output ) {
      out.open( FLAGS_result_file, std::ios::out | std::ios::trunc );
    }


    int retval = sqlite3_open_v2( FLAGS_compare_db.c_str(), &db, SQLITE_OPEN_READONLY, nullptr );
    if( retval ) {
      std::cerr << "Error opening database " << FLAGS_compare_db << std::endl;
      sqlite3_close(db);
      exit(1);
    }

    const char query[] = "select patentID1, patentID2, cosine_score from cosine;";
    sqlite3_stmt * statement = nullptr;
    char * error_ptr = nullptr;
    retval = sqlite3_prepare_v2( db, query, sizeof(query), &statement, nullptr );
    if( retval != SQLITE_OK ){
      std::cerr << "Error executing sqlite query: " << query
                << std::endl;
    }

    if( write_output ) {
      out.precision(5);
      out.setf( std::ios::fixed, std:: ios::floatfield );
    } else {
      std::cout.precision(5);
      std::cout.setf( std::ios::fixed, std:: ios::floatfield );
    }
          // process results
    while( sqlite3_step( statement ) == SQLITE_ROW ) {
      std::string patentID1{ (const char *) sqlite3_column_text( statement, 0 ) };
      std::string patentID2{ (const char *) sqlite3_column_text( statement, 1 ) };
      auto jevin_score = sqlite3_column_double( statement, 2 );

      // compute our score
      auto v1 = incidence_matrix.innerVector( patentMap[ patentID1 ] );
      auto v2 = incidence_matrix.innerVector( patentMap[ patentID2 ] );
      auto our_score = v1.dot(v2);

      auto error = std::abs( our_score - jevin_score );
      (write_output ? out : std::cout)
        << error << "\t"
        << patentID1 << "\t"
        << patentID2 << "\t"
        << jevin_score << "\t"
        << our_score
        << std::endl;
}

    // done.
    sqlite3_close(db);
  }

  std::cerr << "Done with " << (uint64_t) incidence_matrix.rows() * incidence_matrix.rows() / 2 << " comparisons." << std::endl;
  return 0;
}
