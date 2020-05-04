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
double dp, alpha;
int me, nprocs;

void Mapper(int itask, KeyValue *kv, void *ptr);
void update(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr);
void Reducer(char *key, int keybytes, char *multivalue, int nvalues, int *valuebytes, KeyValue *kv, void *ptr);
void Map(int itask, KeyValue *kv, void *ptr);
void Modify(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr);
void write_to_file(string fname);
void output();

int main(int argc, char** argv){
    MPI_Init(&argc,&argv);
    
    me,nprocs;
    MPI_Comm_rank(MPI_COMM_WORLD,&me);
    MPI_Comm_size(MPI_COMM_WORLD,&nprocs);    
    
    alpha = 0.85;
    double conv = 0.000001;
    num_webpages = 0;
    double wtime = 0;

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

    if(me == 0){
        wtime = MPI_Wtime();
    }

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
        dp = double(dp/(double)(num_webpages));
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

        // Calculation of Mp done checking if Mp converges or not
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

    if(me == 0){
        // cout <<argv[1]<<endl;
        // cout<<"Time Taken to find the pagerank is: "<<MPI_Wtime() - wtime<<" --> "<<argv[1]<<endl;
        string out_name = argv[3];
        write_to_file(out_name);
        // output();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();

	return 0;
}


void Mapper(int itask, KeyValue *kv, void *ptr){
  int i = ((me*num_webpages)/nprocs);

  while(true){
    double pgi = 0;
    kv->add((char *) &i,sizeof(int),(char *) &pgi,sizeof(double));
    
    for(int j =0; j<outgoing_links[i].size(); j++){
      double pg = (double)(pageranks[i]/outgoing_links[i].size());
      kv->add((char *) &outgoing_links[i][j],sizeof(int),(char *) &pg,sizeof(double));
	}

      i++;
      if(i >= (((me+1)*num_webpages)/nprocs)){
          break;
      }
  }

}

void update(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr){
  int keyint = *(int *) key;
  double pgrank = *(double *) value;
  pageranks_up[keyint] = (double)alpha*pgrank + alpha*dp + (double)(1-alpha)/num_webpages;
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
    int i = ((me*num_webpages)/nprocs);

  while(true){
    if(outgoing_links[i].size() == 0)
        kv->add((char *) &i,sizeof(int),(char *) &pageranks[i],sizeof(double));
    
    i++;
    if(i >= (((me+1)*num_webpages)/nprocs)){
      break;
    }
  }
}

void Modify(uint64_t itask, char *key, int keybytes, char *value, int valuebytes, KeyValue *kv, void *ptr){
  double pgrank = *(double *) value;
  dp += pgrank;
}

void write_to_file(string fname){
    ofstream myfile(fname);
    double sum = 0.0f;
    for(long long int i=0;i<num_webpages;i++){
        myfile<<i<<" = "<<pageranks[i]<<"\n";
        sum += pageranks[i];
    }
    // cout<<sum<<endl;
    myfile<<"s = "<<sum<<"\n";
    myfile.close();
}

void output(){
    double sumer = 0.0;
    int me;
    MPI_Comm_rank(MPI_COMM_WORLD, &me);

    if (me == 0) {
        for(int i=0; i<num_webpages && me == 0; i++){
            cout << i << " = " << pageranks[i] << endl;
            sumer += pageranks[i];
        }
        cout << sumer << endl;
    }
}