#include "mpi-lib.h"



// using namespace std;

float* link_mat_creator(string fname, long long int n){
    // cout<<"n: "<<n<<endl;
    ifstream myfile(fname);
    // float* ans = (float*)malloc(n*n*sizeof(float));
    float* ans  = (float *)malloc(n*sizeof(float));
    for (int i=0;i<n;i++){
        ans[i]=-1;
    }
    string line;
    if (myfile.is_open()){
        while(getline(myfile,line)){
            long long int n1 = stoi(line.substr(0,line.find(" "))),n2 = stoi(line.substr(line.find(" ")+1,line.length()));
            // cout<<n1<<" "<<n2<<endl;
            ans[n1] = n2;
        }
    }
    return ans;
}

//Function to create key value pairs to input in the mapper
tuple<float*,float*> map_pairs(float* link_vec,float* prob,long long int n){
    float *ans,*keys;
    ans  = (float*)malloc((3)*(n)*sizeof(float)); //values: pj, #(j) and pages(number will be 1 or 0)
    keys = (float*)malloc(n*sizeof(float));
    // vector<PAIR> mapped_pairs;
    for (int i=0;i<n;i++){
        keys[i]=i;
        float c_p = link_vec[i];
        ans[i*3] = prob[i];
        if (c_p == -1){
            ans[i*3+1]=0;
        }else{
            ans[i*3+1] = 1;
            ans[i*3+2] = c_p; 
        }
    }
    tuple<float*,float*> pair;
    pair = make_tuple(keys,ans);
    return pair;
}

void set_initial_prob(float* p, long long int n){
    float u = ((1.0)/n) ;
    // cout<<"u: "<<u<<endl;
    for ( int i=0;i<n;i++){
        p[i]= u;
    }
}

tuple<float*,float*> change_format(vector<PAIR> v,long long int n){
    float* keys,*vals;
    keys = (float*)malloc(n*sizeof(float));
    vals = (float*)malloc(n*sizeof(float));
    for (int i=0;i<n;i++){
        keys[i] = get<0>(v[i]);
        vals[i] = get<1>(v[i]);
    }
    return make_tuple(keys,vals);
}

bool* get_dangling_nodes(float* link_vec, long long int n){
    bool* d = (bool*)malloc(n*sizeof(bool));
    for(int i=0;i<n;i++){
        if(link_vec[i]==-1){
            d[i]=true;
        }else{
            d[i]=false;
        }
    }
    return d;
}
float sum_of_vec(float* v, long long int n){
    float ans =0;
    for (int i=0;i<n;i++){
        ans+=v[i];
    }
    return ans;
}

vector<PAIR> dangling_mul(vector<PAIR> dot_prod,long long int n,float multiplicative_factor=1){
    vector<PAIR> ans;
    int s = dot_prod.size();
    if (s==0){
        cout<<"ERROR: Dangling nodes calculation dot product has size 0"<<endl;
    }else if(s>1){
        cout<<"ERROR: Dangling nodes calculation dot product has size greater than 1"<<endl;
    }
    for (int i=0;i<n;i++){
        ans.push_back(make_tuple(i,(get<1>(dot_prod[0]))*(multiplicative_factor/n)));
    }
    return ans;
}

vector<PAIR> vec_multiply(vector<PAIR>v,float mul_fac=1){
    long long int l= v.size();
    vector<PAIR> ans;
    for (int i=0;i<l;i++){
        ans.push_back(make_tuple(i, (get<1>(v[i]))*mul_fac));
    }
    return ans;
}

vector<PAIR> vec_add(vector<PAIR>v1,vector<PAIR>v2){
    int l1=v1.size(),l2=v2.size();
    vector<PAIR>ans;
    if (l1!=l2){
        cout<<"vec_add function has 2 arguments of different sizes. v1: "<<l1<<";v2: "<<l2<<endl;
    }else{
        for(int i=0;i<l1;i++){
            ans.push_back(make_tuple(i,get<1>(v1[i])+get<1>(v2[i]) ));
        }
    }
    return ans;
}

int main(int argc, char const *argv[])
{
    string fname = argv[1];
    string delim1 = "-",delim2=".";
    long long int n = stoi(fname.substr(fname.find(delim1)+1,fname.find(delim2))); 
    float* A;
    float* p;
    bool *d;
    p = (float *)malloc(n*sizeof(float));
    A = link_mat_creator(fname,n);
    d = get_dangling_nodes(A,n);
    // cout<< A[19979]<<endl;
    set_initial_prob(p,n);
    // cout<<"in main() all val of p: "<<p[1]<<endl;

    float l_f=0.85;
    float l_r= 1-l_f;

    tuple<float*,float*> to_mapper = map_pairs(A,p,n);
    // float prev = sum_of_vec(p,n);
    float prev = 0;
    float curr = prev;
    long int itr = 0;
    vector<PAIR>v,v2;
    tuple<float*,float*> tup;
    MapReduce mr;

    vector<PAIR> tE;
    for(int i=0;i<n;i++){
       tE.push_back(make_tuple(i, (l_r/n) ));
    }

    //Main loop
    while (itr == 0 or abs(curr-prev)>=10e-9){
        to_mapper = map_pairs(A,p,n);

        //Handling dangling nodes
        vector<PAIR> d_dot_p = mr.secondary_map_task(d,to_mapper,n);
        // cout<<"Secondary map_task completed."<<endl;
        vector<PAIR> dot_prod = mr.reduce_task(d_dot_p,1); //The length will be 1 with all summed p_js
        // cout<<"Added all the p_js."<<endl;
        vector<PAIR> Dp = dangling_mul(dot_prod,n);
        // cout<<"Dp calculated."<<endl;

        v = mr.primary_map_task(to_mapper,n); 
        // cout<<"Primary map task completed. Size of v: "<<v.size()<<endl;
        //Can reserve size for performance though. 
        v.insert(v.end(),Dp.begin(),Dp.end());
        // cout<<"Extended this vector with the dangling nodes. New size of v: "<<v.size()<<endl;

        v2 = mr.reduce_task(v,n);
        // cout<<"Add the tuples now added in the reduce task. Size of v2: "<<v2.size()<<endl;
        v2 = vec_multiply(v2,l_f);
        // cout<<"Tuples now multiplied with 0.85 Size of v2: "<<v2.size()<<endl;
        v2 = vec_add(v2,tE);
        // cout<<"Tuples added with the tE term. Size of v2: "<<v2.size()<<endl;
        tup = change_format(v2,n);
        float* new_p = get<1>(tup);
        if (itr!=0){
            prev = curr;
        }

        curr = sum_of_vec(new_p,n);
        // cout<<"Current sum of all p_js is: "<<curr<<endl;
        p = new_p;
        itr++;
    } 
    for ( int i=0;i<n;i++){
        cout<<get<0>(tup)[i]<<" "<<get<1>(tup)[i]<<endl;
    }
    cout<<"Itr: "<<itr<<endl;

   
    return 0;
}
