serial:
	mpicxx -std=c++11 -o mr-pr-serial.o mr-pr-mpi-serial.cpp mpi-lib.cpp
part_1:
	g++ mr-pr-cpp.cpp -I include/ /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o mpi-pr-cpp.o

part_2:
	mpicxx -std=c++11 -o mr-pr-mpi.o mr-pr-mpi.cpp mpi-lib.cpp