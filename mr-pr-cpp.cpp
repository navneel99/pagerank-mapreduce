
#include "mapreduce.hpp"
#include <iostream>
#include <fstream>
#include <chrono>

using namespace std;
using namespace std::chrono;

namespace pagerank {

  class map_task;
  class reduce_task;

  // vector<vector<int> > buf;
  // bool*d,*A;
  float* p;
  int max_n;
  vector<vector<float> > ds;


  template<typename MapTask>
  class file_source 
  {
    public:
      file_source() : sequence_(0)
      {
      }
      bool const setup_key(typename MapTask::key_type &key)
      {
          // std::cout<<"In Setup"<<std::endl;
          key = sequence_++;
          return key < max_n; //check less than equal to
      }

      bool const get_data(typename MapTask::key_type const &key, typename MapTask::value_type &value)
      {
          // cout<<"In get_data"<<endl;
          // for (unsigned loop=0; loop<nm; ++loop)
              // if (is_friend(key,loop))
          // value.push_back(ds[key]);
          swap(value,ds[key]);
          return true;
      }
    private:
      int sequence_;

  };


class map_task 
{
  public:
    typedef int key_type;
    typedef vector<float>  value_type;
    typedef int intermediate_key_type;
    typedef float intermediate_value_type;

    // map_task(job::map_task_runner &runner) : runner_(runner)
    // {
    // }

    // 'value_type' is not a reference to const to enable streams to be passed
    //    key: input filename
    //    value: ifstream of the open file
    template<typename Runtime>
    void operator()(Runtime &runtime,key_type const &key, value_type &value)
    {
        float p_j  = pagerank::p[key];
        // cout<<"p_j= "<<pagerank::p[key]<<endl;
        int num_links = value[1];
        float tmp = p_j/num_links;
        for (int i=2;i<value.size();i++){
          runtime.emit_intermediate((int)value[i],tmp);
        }
        runtime.emit_intermediate(key, 0);
    }


  private:
    // job::map_task_runner &runner_;


};

class reduce_task 
{
  public:
    typedef int key_type;
    typedef float value_type;


    template<typename Runtime, typename It>
    void operator()(Runtime &runtime, key_type const &key, It it, It ite) const
    {
        value_type ans = 0;
        for (; it !=ite;++it){
          ans+=(*it);
        }
        runtime.emit(key,ans);
    }


  private:
    // job::reduce_task_runner &runner_;

};

typedef mapreduce::job<pagerank::map_task,pagerank::reduce_task,mapreduce::null_combiner,pagerank::file_source<pagerank::map_task>> job;


};

vector<vector<int> > file_max_num(string fname){
        ifstream value(fname);
        std::string line;
        int lines=0;
        vector<vector<int>> buf;
        int max_num = -1;
        while (std::getline(value,line))
        {
            // int n1,n2;
            int n1 = stoi(line.substr(0,line.find(" "))),n2 = stoi(line.substr(line.find(" ")+1,line.length()));
            buf.push_back({n1,n2});
            lines++;
            max_num = max(max_num,max(n1,n2));
        }
        max_num++; //to accomodate 0
        buf.push_back({lines,max_num});
        return buf;
}

vector<vector<float> >to_data_source(bool* A,float* p,float* ns,int mn){
  vector<vector<float> > ans;
  for (int i=0;i<mn;i++){
    vector<float> row;
    row.push_back(p[i]);
    row.push_back(ns[i]);

    for(int j=0;j<mn;j++){
      if (A[j*mn+i]){
        row.push_back(j);
      }
    }
    ans.push_back(row);
  }

  return ans;
}

template<typename T>
void print_vector(T* a,int n,int m){
  for (int i=0;i<n;i++){
    cout<<a[i];
    if (i%m==m-1){
      cout<<endl;
    }else{
      cout<<" ";
    }
    // cout<<a[i]<<endl;
  }
}

void print_vector(vector<vector<float>> v){
  for(int i=0;i<v.size();i++){
    for(int j=0;j<v[i].size();j++){
      cout<<v[i][j]<<" ";
    }
    cout<<"\n";
  }
}

template<typename T>
void print_vector(vector<T>v ){
  for (int i=0;i<v.size();i++){
    cout<<v[i]<<endl;
  }
}

void vec_multiply(float*v,long long int n,float mul_fac=1){
    for (int i=0;i<n;i++){
        v[i] = v[i]*mul_fac;
    }
}

void vec_add(float*v1,float*v2,long long int n){
        for(int i=0;i<n;i++){
            v1[i] = v1[i]+v2[i];
        }        
}


float* calculate_dangling_metric(bool* d, float* p,int maxn,float* dp,float l_f=0.85){
  // float *dp = (float*)malloc(maxn*sizeof(float));
  float mul = 0;
  for (int i=0;i<maxn;i++){
    mul += (d[i] * p[i]);
  }
  mul/=maxn;
  for (int j=0;j<maxn;j++){
    dp[j] = mul*l_f;
  }
  // return dp;
}
float sum_vec(float * v, int n){
  float ans=0;
  for(int i=0;i<n;i++){
    ans+=v[i];
  }
  return ans;
}

void output_to_file(string fname,float* p,int n){
  ofstream myfile(fname);
  for(int i=0;i<n;i++){
    myfile<<i<<" = "<<p[i]<<endl;
  }
  myfile<<"s = 1.0"<<endl;
}


int main(int argc, char* argv[]){
      string fname = argv[1];
      string oflag = argv[2];
      string outfname = argv[3];

      float l_f = 0.85;
      float l_r = 1-l_f;
      float prev=0,curr=0;

      auto t1 = high_resolution_clock::now();

      vector<vector<int> > buf = file_max_num(fname);
      int sz = buf.size();
      vector<int> temp = buf[sz-1];
      int lines = temp[0],maxn = temp[1];
      float* p = (float*)malloc(maxn*sizeof(float));
      bool * A = (bool*)malloc(maxn*maxn*sizeof(bool));
      bool * d = (bool*)malloc(maxn*sizeof(bool));
      float * D = (float*)malloc(maxn*sizeof(float));
      float *ns = (float*)malloc(maxn*sizeof(float));
      float* new_p = (float*)malloc(maxn*sizeof(float));
      float* tE = (float*)malloc(maxn*sizeof(float));
      
      double tot_time=0;
      
      for (int i=0;i<maxn*maxn;i++){
        A[i]=false;
        if (i<maxn){
          new_p[i]=0;
          d[i]=true;
          ns[i]=0;
          p[i]=(1.0/maxn);
          tE[i]=(l_r/maxn);
        }
      }
      for (int i=0;i<buf.size()-1;i++){ //last element not to be considered
        A[buf[i][1]*maxn + buf[i][0]] = true;
        ns[buf[i][0]]++;
        d[buf[i][0]]&=0;
      }

      vector<vector<float> > to_datasource = to_data_source(A,p,ns,maxn);
      int itr= 0;

      while ((itr==0)||((itr<100)&&(abs(curr-prev)>10e-9))){
        calculate_dangling_metric(d,p,maxn,D);
        pagerank::ds = to_datasource;
        pagerank::max_n = maxn;
        pagerank::p = p;
        mapreduce::specification spec;

        spec.map_tasks = 8;
        spec.reduce_tasks = 2;
        
        pagerank::job::datasource_type datasource;
        pagerank::job job(datasource,spec);
        mapreduce::results result;

        job.run<mapreduce::schedule_policy::cpu_parallel<pagerank::job> >(result);
        
        for (auto it = job.begin_results();it!=job.end_results();++it){
          new_p[it->first] = it->second;
        }
        vec_multiply(new_p,maxn,l_f);
        vec_add(new_p,D,maxn);
        vec_add(new_p,tE,maxn);
        p = new_p;
        if (prev != curr){
          prev = curr;
        }
        curr = sum_vec(p,maxn);
        itr++;
      }
      auto t2= high_resolution_clock::now();
      duration<double> time_span = duration_cast<duration<double>>(t2-t1);
      cout<<"Iterations: "<<itr<<endl;
      cout<<"Time taken: "<<time_span.count()<<" seconds"<<endl;
      output_to_file(outfname,p,maxn);

    return 0;
}