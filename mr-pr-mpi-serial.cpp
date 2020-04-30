#include "mpi-lib.h"



void link_mat_creator(string fname, long long int n,float* ans){
    // cout<<"n: "<<n<<endl;
    ifstream myfile(fname);
    // float* ans = (float*)malloc(n*n*sizeof(float));
    // float* ans  = (float *)malloc(n*sizeof(float));
    for (int i=0;i<n;i++){
        ans[i]=-1;
    }
    string line;
    if (myfile.is_open()){
        while(getline(myfile,line)){
            long long int n1 = stoi(line.substr(0,line.find(" "))),n2 = stoi(line.substr(line.find(" ")+1,line.length()));
            cout<<n1<<" "<<n2<<endl;
            ans[n1] = n2;
        }
    }
    // return ans;
}

//Function to create key value pairs to input in the mapper
// tuple<float*,float*> map_pairs(float* link_vec,float* prob,long long int n){
void map_pairs(float* link_vec,float* prob,long long int n,float*ans){

    // float *ans,*keys;
    // ans  = (float*)malloc((4)*(n)*sizeof(float)); //values: pj, #(j) and pages(number will be 1 or 0)
    // keys = (float*)malloc(n*sizeof(float));
    // vector<PAIR> mapped_pairs;
    for (int i=0;i<n;i++){
        // keys[i]=i;
        ans[i*4]=i;
        float c_p = link_vec[i];
        ans[i*4+1] = prob[i];
        if (c_p == -1){ //Num of links
            ans[i*4+2]=0;
        }else{
            ans[i*4+2] = 1;
            ans[i*4+3] = c_p; 
        }
    }
    // tuple<float*,float*> pair;
    // pair = make_tuple(keys,ans);
    // return pair;
    // return ans;
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

void get_dangling_nodes(float* link_vec, long long int n,bool*d){
    // bool* d = (bool*)malloc(n*sizeof(bool));
    for(int i=0;i<n;i++){
        if(link_vec[i]==-1){
            d[i]=true;
        }else{
            d[i]=false;
        }
    }
    // return d;
}
float sum_of_vec(float* v, long long int n){
    float ans =0;
    for (int i=0;i<n;i++){
        ans+=v[i*2+1];
    }
    return ans;
}

// vector<PAIR> dangling_mul(vector<PAIR> dot_prod,long long int n,float multiplicative_factor=1){
void dangling_mul(float* dot_prod,long long int n,float*ans,float multiplicative_factor=1){

    // vector<PAIR> ans;
    // float* ans = (float*)malloc(2*n*sizeof(float));
    // int s = dot_prod.size();
    // if (s==0){
        // cout<<"ERROR: Dangling nodes calculation dot product has size 0"<<endl;
    // }else if(s>1){
        // cout<<"ERROR: Dangling nodes calculation dot product has size greater than 1"<<endl;
    // }
    for (int i=0;i<n;i++){
        ans[i*2]=i;
        ans[i*2+1] = dot_prod[1]*(multiplicative_factor/n);
        // ans.push_back(make_tuple(i,(get<1>(dot_prod[0]))*(multiplicative_factor/n)));
    }
    // return ans;
}

// vector<PAIR> vec_multiply(vector<PAIR>v,float mul_fac=1){
void vec_multiply(float*v,long long int n,float mul_fac=1){
    // long long int l= v.size();
    // vector<PAIR> ans;
    for (int i=0;i<n;i++){
        // ans.push_back(make_tuple(i, (get<1>(v[i]))*mul_fac));
        v[i*2+1] = v[i*2+1]*mul_fac;
    }
    // return ans;
}

// vector<PAIR> vec_add(vector<PAIR>v1,vector<PAIR>v2){
void vec_add(float*v1,float*v2,long long int n){

    // float* ans;

    // ans = (float*)malloc(2*n*(sizeof(float)));
    // int l1=v1.size(),l2=v2.size();
    // vector<PAIR>ans;
    // if (l1!=l2){
        // cout<<"vec_add function has 2 arguments of different sizes. v1: "<<l1<<";v2: "<<l2<<endl;
    // }else{
        for(int i=0;i<n;i++){
            // ans.push_back(make_tuple(i,get<1>(v1[i])+get<1>(v2[i]) ));
            // v1[2*i]=v1[2*i];
            v1[2*i+1] = v1[2*i+1]+v2[2*i+1];
        }
        
    // }
    // return ans;
}


// void write_to_file(string fname,tuple<float*,float*> tup,long long int n){
void write_to_file(string fname,float* tup,long long int n){

    // float* keys = get<0>(tup);
    // float* vals = get<1>(tup);

    ofstream myfile(fname);
    for(long long int i=0;i<n;i++){
        // myfile<<keys[i]<<" = "<<vals[i]<<"\n";
        myfile<<tup[i*2]<<" = "<<tup[i*2+1]<<"\n";
    }
    myfile<<"s = 1.0"<<"\n";
    myfile.close();
}

void print_vector(float* v,long long int n,int m=2){
    for (int i=0;i<n;i++){
        for(int j=0;j<m;j++){
            cout<<v[i*m+j]<<" ";
        }
        cout<<"\n";
    }
}
int main(int argc, char const *argv[])
{

    //Parses Argument and gets back 'n', the number of nodes 
    string fname = argv[1];
    string outflag = argv[2]; //Essentially just "-o"
    string outfilename = argv[3];
    string delim1 = "-",delim2=".";
    long long int n = stoi(fname.substr(fname.find(delim1)+1,fname.find(delim2))); 

    
    //Instantiates various arrays for subsequent use.
    float* A;
    bool *d;
    float* p;
    p = (float *)malloc(n*sizeof(float));
    A = (float*)malloc(n*sizeof(float));
    d = (bool*)malloc(n*sizeof(bool));

    //Initializes the variables.
    link_mat_creator(fname,n,A); //Working
    get_dangling_nodes(A,n,d); 
    set_initial_prob(p,n); //Working

    // cout<<"A: "<<endl;
    // print_vector(A,n,1);
    // cout<<"p: "<<endl;
    // print_vector(p,n,1);
    // cout<<"in main() all val of p: "<<p[1]<<endl;

    //Acc. to google
    float l_f=0.85;
    float l_r= 1-l_f;

    //Variables to use in the program.
    float* to_mapper;
    float prev = 0;
    float curr = prev;
    long int itr = 0;

    float *v,*v2;
    to_mapper = (float*)malloc(4*n*sizeof(float));
    v2=(float*)malloc(2*n*sizeof(float));
    // tuple<float*,float*> tup;
    
    float* tup;
    MapReduce mr;

    //The random tE(Probability to move to any page)
    // vector<PAIR> tE;
    float* tE;
    tE = (float*)malloc(2*n*sizeof(float));
    float* temp_mat = (float*)malloc(6*n*sizeof(float));

    for(int i=0;i<n;i++){ 
        tE[i*2] = i;
        tE[i*2+1] = (l_r/n);
    }
    

    //Main loop
    while (itr == 0 or abs(curr-prev)>=10e-9){
        map_pairs(A,p,n,to_mapper); //Working
        // print_vector(to_mapper,n,4);

        //Handling dangling nodes
        // vector<PAIR> d_dot_p = mr.secondary_map_task(d,to_mapper,n);
        mr.secondary_map_task(d,to_mapper,n,temp_mat);
        // cout<<"Secondary map_task completed."<<endl;
        // vector<PAIR> dot_prod = mr.reduce_task(d_dot_p,1); //The length will be 1 with all summed p_js
        float* dot_prod = (float*)malloc(2*sizeof(float));
        mr.reduce_task(temp_mat,n,1,dot_prod);
        // cout<<"Added all the p_js."<<endl;
        // vector<PAIR> Dp = dangling_mul(dot_prod,n);
        // float* Dp = dangling_mul(dot_prod,n);
        dangling_mul(dot_prod,n,temp_mat);

        // cout<<"Dp calculated."<<endl;

        mr.primary_map_task(to_mapper,n,temp_mat+2*n);


        mr.reduce_task(temp_mat,3*n,n,v2);

        // cout<<"reduced."<<endl;
        vec_multiply(v2,n,l_f);


        vec_add(v2,tE,n);
        if (itr!=0){
            prev = curr;
        }
        curr = sum_of_vec(v2,n);
        // cout<<"Current sum of all p_js is: "<<curr<<endl;
        for(int i=0;i<n;i++){
            p[i]=v2[2*i+1];
        }
        // if (itr==2){
            // break;
        // }
        // p = new_p;
        itr++;
    } 

    write_to_file(outfilename,v2,n);

    // for ( int i=0;i<n;i++){
    //     cout<<get<0>(tup)[i]<<" "<<get<1>(tup)[i]<<endl;
    // }
    cout<<"Itr: "<<itr<<endl;
    // float* test = (float*)malloc(17*sizeof(float));
    // for(int i=0;i<17;i++){
    //     test[i]=1;
    // }
    // cout<<sizeof(test)<<" "<<sizeof(float)<<endl;
   
    return 0;
}
