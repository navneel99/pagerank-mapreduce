#ifndef MPILIB_H
#define MPILIB_H

#include "common.h"
#include "mpi.h"

class MapReduce{

    public:
        bool contains(float key,vector<float> array);

        vector<PAIR> primary_map_task(tuple<float*,float*> to_mapper,long long int n);
        vector<PAIR> secondary_map_task(bool* d ,tuple<float*,float*> to_mapper,long long int n);

        vector<PAIR> reduce_task(vector<PAIR> mapped_pairs,long long int n);

};

#endif

