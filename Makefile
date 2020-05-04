serial:
	mpicxx -std=c++11 -o mr-pr-serial.o mr-pr-mpi-serial.cpp mpi-lib.cpp
friends:
	g++ friends.cpp -I include/ /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o friends.o
prime:
	g++ prime.cpp -I include/ /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o prime.o
part_1:
	g++ mr-pr-cpp.cpp -I include/ /usr/lib/x86_64-linux-gnu/libboost_system.a /usr/lib/x86_64-linux-gnu/libboost_iostreams.a /usr/lib/x86_64-linux-gnu/libboost_filesystem.a -pthread -o mr-pr-cpp.o
part_2:
	mpicxx -std=c++11 -o mr-pr-mpi.o mr-pr-mpi.cpp mpi-lib.cpp

part_3:
	mpic++ -g -O mr-pr-mpi-base.o ../src/libmrmpi_mpicc.a  -o mr-pr-mpi-base

clean:
	rm *.o
