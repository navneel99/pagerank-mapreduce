#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <sstream>
#include "mpi.h"
#include "mapreduce.h"
#include "keyvalue.h"

using namespace MAPREDUCE_NS;
using namespace std;

vector<vector<int>> outgoing_links(100000);
vector<double> pageranks(100000, 0.0f);
vector<double> pageranks_up(100000, 0.0f);
int num_webpages;
double dp;

void Mapper(int itask, KeyValue *kv, void *ptr);
void update(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr);
void Reducer(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
int Hash(char *key, int keybytes);
void Map(int itask, KeyValue *kv, void *ptr);
void Modify(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr);


int main(int argc, char** argv){
    MPI_Init(&argc,&argv);
    
    int me,nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);    
    
    double alpha = 0.85;
    double conv = 0.0001;
    num_webpages = 0;

	ifstream fopen;
	fopen.open(argv[1]);
    string line;
	while(getline(fopen, line)){
        istringstream iss(line);
        int a, b;

        if(!(iss>>a>>b)){
            cout<<"Error Wrong input.\n";
            exit(1);
        }

		num_webpages = max(num_webpages,max(a,b));
		outgoing_links[a].push_back(b);
	}

	fopen.close();
	num_webpages++;

    for(int i=0; i<num_webpages; i++){
		pageranks[i] = (double)(1.0f/(double)num_webpages);
	}

    int tt = 0;
    while (true){
        dp =0.0f;

        // 1st Mapreduce Segment
        MPI_Barrier(MPI_COMM_WORLD);
        MapReduce *nr = new MapReduce(MPI_COMM_WORLD);
        int words = nr->map(nprocs, Map, NULL);
        nr->gather(1);
        nr->convert();
        int uni = nr->reduce(Reducer, NULL);
        nr->broadcast(0);
        MPI_Barrier(MPI_COMM_WORLD);
        nr->map(nr,Modify,NULL);
        dp = double(dp/num_webpages);
        delete nr;

        // 2nd MapReduce Segment
        MapReduce *mr = new MapReduce(MPI_COMM_WORLD);
        int nwords = mr->map(nprocs,Mapper,NULL);
        mr->gather(1);
        mr->convert();
        int nunique = mr->reduce(Reducer,NULL);
        mr->broadcast(0);
        MPI_Barrier(MPI_COMM_WORLD);
        mr->map(mr,update,NULL);
        delete mr;

        for(int i=0; i<num_webpages; i++){
                pageranks_up[i] = (double)alpha*pageranks_up[i] + alpha*dp + (double)(1-alpha)/num_webpages;
            }

        // Calculation of Mp done checking conversion
        bool converging = true;
        for(int i=0; i<num_webpages; i++){
            if(pageranks[i]-pageranks_up[i]>conv)
                converging=false;
            pageranks[i] = pageranks_up[i];
        }

        if(converging)
            break;

        tt++;
    }

    // double sumer = 0.0;
    // for(int i=0; i<num_webpages && me == 0; i++){
    //     cout << i << " = " << pageranks[i] << endl;
    //     sumer += pageranks[i];
    // }

    // if (me == 0) {
    //     cout << sumer << endl;
    // }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

	return 0;
}


void Mapper(int itask, KeyValue *kv, void *ptr){

  int world_rank, num_procs;
  MPI_Comm_size(MPI_COMM_WORLD,&num_procs);
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  int i = ((world_rank*num_webpages)/num_procs);

  while(true){
    double pgi = 0;
    kv->add((char *) &i,sizeof(int),(char *) &pgi,sizeof(double));
    
    for(int j =0; j<outgoing_links[i].size(); j++){
      double pg = (double)(pageranks[i]/outgoing_links[i].size());
      kv->add((char *) &outgoing_links[i][j],sizeof(int),(char *) &pg,sizeof(double));
	}

      i++;
      if(i >= (((world_rank+1)*num_webpages)/num_procs)){
          break;
      }
  }

}

void update(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr){
  int keyint = *(int *) key;
  double pgrank = *(double *) value;
  pageranks_up[keyint] = pgrank;
}

void Reducer(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr){
  int t = *(int *) key;
  double* s = (double *) multivalue;
  double sum = 0;
  for(int i = 0; i < nvalues; i++){
    sum += s[i];
  }
  kv->add(key,keybytes,(char *) &sum,sizeof(double));
}

void Map(int itask, KeyValue *kv, void *ptr){
    int world_rank, num_procs;
    MPI_Comm_size(MPI_COMM_WORLD,&num_procs);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    int i = ((world_rank*num_webpages)/num_procs);

  while(true){
    if(outgoing_links[i].size() == 0)
        kv->add((char *) &i,sizeof(int),(char *) &pageranks[i],sizeof(double));
    
    i++;
    if(i >= (((world_rank+1)*num_webpages)/num_procs)){
      break;
    }
  }
}

void Modify(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr){
  double pgrank = *(double *) value;
  dp += pgrank;
}
