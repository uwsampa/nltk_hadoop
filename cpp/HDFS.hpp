#pragma once

#include <iostream>

#include <boost/noncopyable.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/iostreams/concepts.hpp>

#include <hdfs.h>

namespace HDFS {

class Connection : private boost::noncopyable {
private:
  hdfsFS fs_;

public:
  Connection(const char * hostname, int port = 0 )
    : fs_( hdfsConnect(hostname, port) )
  {
    if( !fs_ ) {
      std::cerr << "Error connecting to HDFS." << std::endl;
      exit(1);
    }
  }

  ~Connection() {
    int retval = hdfsDisconnect( fs_ );
    if( retval < 0 ) {
      std::cerr << "Error closing HDFS connection." << std::endl;
      exit(1);
    }
    fs_ = NULL;
  }

  hdfsFS get_fs() const { return fs_; }
};


class Source {
private:
  hdfsFS   fs_;
  hdfsFile file_;
  
public:
  typedef char        char_type;
  struct category
    : boost::iostreams::source_tag
    , boost::iostreams::closable_tag
    , boost::iostreams::multichar_tag
  { };

  Source()
    : fs_(NULL)
    , file_(NULL)
  { }
  
  Source( Connection & conn, const char * path )
    : fs_(conn.get_fs())
    , file_(NULL)
  {
    file_ = hdfsOpenFile( fs_, path, O_RDONLY, 0, 0, 0 );
    if( !file_ ) {
      std::cerr << "Error opening HDFS file " << path << "." << std::endl;
      exit(1);
    }
  }

  void close() {
    if( file_ ) {
      int retval = hdfsCloseFile( fs_, file_ );
      if( retval < 0 ) {
        std::cerr << "Error closing HDFS file." << std::endl;
        exit(1);
      }
      file_ = NULL;
    }
  }

  inline std::streamsize optimal_buffer_size() const { return 0; }

  bool good() const { return file_ != NULL; }
  
  std::streamsize read(char* s, std::streamsize n)
  {
    // Read up to n characters from the underlying data source
    // into the buffer s, returning the number of characters
    // read; return -1 to indicate EOF
    size_t retval = hdfsRead( fs_, file_, s, n );
    if( retval < 0 ) {
      std::cerr << "Error reading from HDFS file." << std::endl;
      exit(1);
    } else if( 0 == retval ) { // EOF
      return -1;
    } else { // return bytes read
      return retval;
    }
  }

};

class FileInfoIterator {
private:
  hdfsFileInfo * base_;
  int numEntries_;

public:
  FileInfoIterator()
    : base_(nullptr)
    , numEntries_(0)
  { }

  FileInfoIterator( Connection & conn, std::string path )
    : base_(nullptr)
    , numEntries_(0)
  {
    base_ = hdfsListDirectory( conn.get_fs(), path.c_str(), &numEntries_ );
    if( !base_ ) {
      std::cerr << "Error getting info for path " << path << std::endl;
      exit(1);
    }
  }

  ~FileInfoIterator() {
    if( base_ ) {
      hdfsFreeFileInfo( base_, numEntries_ );
      base_ = nullptr;
    }
  }

  hdfsFileInfo * begin() const { return &base_[0]; }
  hdfsFileInfo * end()   const { return &base_[numEntries_]; }
};


} // namespace HDFS
