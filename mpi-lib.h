#ifndef MPILIB_H
#define MPILIB_H

#include "common.h"
#include "mpi.h"

class MapReduce{

    public:
        bool contains(float key,vector<float> array);

        // vector<PAIR> primary_map_task(tuple<float*,float*> to_mapper,long long int n);
        void primary_map_task(float* to_mapper,long long int lines,long long int max_num,float* mapped_pairs);

        // vector<PAIR> secondary_map_task(bool* d ,tuple<float*,float*> to_mapper,long long int n);
        // float* secondary_map_task(bool* d ,float* to_mapper,long long int n);
        void secondary_map_task(bool* d ,float* p,long long int n,float* mapped_pairs);


        // vector<PAIR> reduce_task(vector<PAIR> mapped_pairs,long long int n);
        void reduce_task(float* mapped_pairs,long long int l,long long int n,float* a);

};

#endif

