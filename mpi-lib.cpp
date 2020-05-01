#include "mpi-lib.h"

// typedef tuple<float,float> PAIR;


// using namespace std;

// class MapReduce
// {
    // public:
        // vector<PAIR> MapReduce::primary_map_task(tuple<float*,float*> to_mapper,long long int n){ 
        void MapReduce::primary_map_task(float* to_mapper,long long int n,float* mapped_pairs){ 

            // float* keys = get<0>(to_mapper);
            // float* vals = get<1>(to_mapper);

            // vector<PAIR> mapped_pairs;
            // float* mapped_pairs =(float*)malloc(4*n*sizeof(float));

            for (int i=0;i<n;i++){

                // float j = keys[i];
                float j = to_mapper[i*4];
                float pj  = to_mapper[i*4+1];
                float num_links = to_mapper[i*4+2];
                float new_val = pj/num_links;
                if (num_links >0){
                    float link_name = to_mapper[i*4+3]; //At max only one outgoing link
                    // mapped_pairs.push_back(make_tuple(link_name,new_val));
                    mapped_pairs[i*4]=link_name;
                    mapped_pairs[i*4+1]=new_val;    
                }else{
                    mapped_pairs[i*4]=0;
                    mapped_pairs[i*4+1]=0;
                }
                mapped_pairs[i*4+2]=j;
                mapped_pairs[i*4+3]=0;
                // mapped_pairs.push_back(make_tuple(j,0)); //Debug for nodes with no incoming link
            }
        //    return mapped_pairs;
        }

        //To handle the dangling nodes
        // vector<PAIR> MapReduce::secondary_map_task(bool* d ,tuple<float*,float*> to_mapper,long long int n){
        void MapReduce::secondary_map_task(bool* d ,float* to_mapper,long long int n,float* mapped_pairs){

            // float* keys = get<0>(to_mapper);
            // float* vals = get<1>(to_mapper);

            // vector<PAIR> mapped_pairs;
            // float* mapped_pairs=(float*)malloc(2*n*sizeof(float));
            for ( int i=0;i<n;i++){
                if (d[i]==true){
                    mapped_pairs[i*2]= 0;
                    mapped_pairs[i*2+1]=to_mapper[i*4+1];
                    // mapped_pairs.push_back(make_tuple(0,vals[i*3])); //pair of (0,p_j)
                }else{
                    mapped_pairs[i*2]= 0;
                    mapped_pairs[i*2+1]=0;
                }
            }
            // return mapped_pairs;
        }
        bool MapReduce::contains(float key,vector<float> array){
            for (int i=0;i<array.size();i++){
                if (key == array[i]){
                    return true;
                }
            }
            return false;
        }

        // vector<PAIR> MapReduce::reduce_task(vector<PAIR> mapped_pairs,long long int n){
        void MapReduce::reduce_task(float* mapped_pairs,long long int l,long long int n,float*a){
        

            for (int i=0;i<n;i++){
                a[i*2]=i;
                a[i*2+1]=0;
            }

            for (int i=0;i<l;i++){
                // tuple<float,float> c = mapped_pairs[i];
                int tmp = mapped_pairs[2*i];
                a[2*tmp+1]+=mapped_pairs[2*i+1];
            }
            // for (int i=0;i<n;i++){
                // ans.push_back(make_tuple(keys[i],vals[i]));
            // }
            // return a;
        }
// };


// int main(){


//     return 0;
// }
