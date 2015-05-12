
OPTFLAGS=-O3

CPPFLAGS=\
	-I/sampa/share/gcc-4.8.2/src/boost_1_55_0 \
	-I/sampa/share/eigen-3.2.4/include \
	-I/sampa/share/gflags/include \
	-I/sampa/share/avro/avrocpp-1.7.7/include \
	-I$(HADOOP_INSTALL)/include

CXXFLAGS=\
	-fopenmp \
	-std=c++11 \
	$(OPTFLAGS) \
	-Wno-deprecated-declarations \
	-g

LDFLAGS=\
	-L/sampa/share/gcc-4.8.2/src/boost_1_55_0/stage/lib \
	-L/sampa/share/gflags/lib \
	-L/sampa/share/avro/avrocpp-1.7.7/lib \
	-L$(HADOOP_INSTALL)/lib/native

LDLIBS=\
	-Wl,-Bstatic  \
	-lavrocpp_s \
	-lgflags      \
	-Wl,-Bdynamic  \
	-lhdfs \
	-lboost_iostreams \
	-lpthread     \
	-lsqlite3     \
	-lcrypto      \
	-lssl

cosine_similarity: cosine_similarity.cpp

