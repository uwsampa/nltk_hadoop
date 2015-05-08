//
// Compute cosine similarity between incidence matrix stored in HDFS Avro files.
//
// Run with command like "/run-with-classpath.sh  ./cosine_similarity /patents/output/tfidf"
//

#include <algorithm>
#include <limits>
#include <string>
#include <iostream>

#include <boost/iostreams/stream.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include <avro/Compiler.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Specific.hh>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>

#include "HDFS.hpp"

using namespace std;
namespace pt = boost::property_tree;

int main( int argc, char * argv[] ) {
  if( argc < 2 ) {
    std::cerr << "Usage: " << argv[0] << " <path in HDFS (Avro file or directory containing Avro files> [<optional NFS output file>]" << std::endl;
    exit(1);
  }
  const char * path = argv[1];
  
  int retval;
  size_t record_count = 0;

  // connect to HDFS
  HDFS::Connection conn("hadoop.sampa");

  // for each file in path (
  for( auto fileInfo : HDFS::FileInfoIterator( conn, path ) ) {
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
        
        auto patent_id = pt.get<std::string>("key.filename");
        auto bigram = pt.get<std::string>("key.word");
        auto tfidf = pt.get<double>("value.tfidf");
        
        std::cout << patent_id << "\t" << bigram << "\t" << tfidf << std::endl;
        ++record_count;
      }
    }
  }
  std::cerr << "Done. Record count: " << record_count << std::endl;
  
  return 0;
}
