#include "mpi-lib.h"



void link_mat_creator(string fname, long long int n,float* ans){
    ifstream myfile(fname);

    for (int i=0;i<n;i++){
        ans[i]=-1;
    }
    string line;
    int ind = 0;
    if (myfile.is_open()){
        while(getline(myfile,line)){
            long long int n1 = stoi(line.substr(0,line.find(" "))),n2 = stoi(line.substr(line.find(" ")+1,line.length()));
            // cout<<n1<<" "<<n2<<endl;
            // ans[n1] = n2;
            ans[ind++] = n1;
            ans[ind++] = n2;
        }
    }
}

//Function to create key value pairs to input in the mapper
// tuple<float*,float*> map_pairs(float* link_vec,float* prob,long long int n){
void map_pairs(float* link_vec,float* prob, int max_num,int lines,float*ans){
    int * freq = (int*)malloc(max_num*sizeof(int));
    for (int i=0;i<max_num;i++){
        freq[i] = 0;
    }
    for (int i=0;i<lines;i++){
        freq[(int)link_vec[2*i]]++;
    }

    for (int i=0;i<lines;i++){
        int n1 = link_vec[2*i];
        int n2 = link_vec[2*i+1];
        ans[i*4]=n1;
        ans[i*4+1] = prob[n1];
        ans[i*4+2] = freq[n1];
        ans[i*4+3] = n2;
    }
    free(freq);
}

void set_initial_prob(float* p, int max_num){
    float u = ((1.0)/max_num) ;
    for ( int i=0;i<max_num;i++){
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

void get_dangling_nodes(float* link_vec, int lines,bool*d, int max_num){

    for (int i=0;i<max_num;i++){
        d[i] = true;
    }
    for(int i=0;i<lines;i++){
        d[(int)link_vec[i*2]]&=0;  //The outgoing link must be made 0
    }
}
float sum_of_vec(float* v, long long int n){
    float ans =0;
    for (int i=0;i<n;i++){
        ans+=v[i*2+1];
    }
    return ans;
}

// vector<PAIR> dangling_mul(vector<PAIR> dot_prod,long long int n,float multiplicative_factor=1){
void dangling_mul(float* dot_prod,long long int max_num,float*ans,float multiplicative_factor=1){

    for (int i=0;i<max_num;i++){
        ans[i*2]=i;
        ans[i*2+1] = dot_prod[1]*(multiplicative_factor/max_num);
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

        for(int i=0;i<n;i++){
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

template<typename T>
void print_vector(T* v,long long int n,int m=2){
    for (int i=0;i<n;i++){
        for(int j=0;j<m;j++){
            cout<<v[i*m+j]<<" ";
        }
        cout<<"\n";
    }
}

vector<int> fileinfo(string fname){
    ifstream myfile(fname);
    string line;
    int n1,n2,lines=0,max_num=-1;
    while(getline(myfile,line)){
        n1 = stoi(line.substr(0,line.find(" ")));
        n2 = stoi(line.substr(line.find(" ")+1,line.length()));
        lines++;
        max_num=max(max_num,max(n1,n2));
    }
    return {lines,max_num};
}

void emit_zero_pairs(int max_num,float * temp_mat){
    for (int i=0;i<max_num;i++){
        temp_mat[i*2] = i;
        temp_mat[i*2+1] = 0;
    }
}

int main(int argc, char const *argv[])
{

    //Parses Argument and gets back 'n', the number of nodes 
    string fname = argv[1];
    string outflag = argv[2]; //Essentially just "-o"
    string outfilename = argv[3];
    // string delim1 = "-",delim2=".";
    // long long int n = stoi(fname.substr(fname.find(delim1)+1,fname.find(delim2))); 
    vector<int> info = fileinfo(fname);
    int lines = info[0],max_num = info[1];
    max_num++;
    int n = max(lines,max_num);
    cout<<"lines: "<<lines<<" max_num: "<<max_num<<endl;
    //Instantiates various arrays for subsequent use.
    float* A;
    bool *d;
    float* p;
 
    // p = (float *)malloc(n*sizeof(float));
    // A = (float*)malloc(n*sizeof(float));
    // d = (bool*)malloc(n*sizeof(bool));

    p = (float *)malloc(max_num*sizeof(float));
    A = (float*)malloc(2*lines*sizeof(float));
    d = (bool*)malloc(max_num*sizeof(bool));

    // cout<<"Allocated."<<endl;
    //Initializes the variables.
    link_mat_creator(fname,2*lines,A); //Working
    get_dangling_nodes(A,lines,d,max_num);  //working
    set_initial_prob(p,max_num); //Working
    // cout<<"probability set."<<endl;
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
    to_mapper = (float*)malloc(4*lines*sizeof(float));
    v2=(float*)malloc(2*max_num*sizeof(float));
    // tuple<float*,float*> tup;
    
    float* tup;
    MapReduce mr;

    //The random tE(Probability to move to any page)
    // vector<PAIR> tE;
    float* tE;
    tE = (float*)malloc(2*max_num*sizeof(float));
    // float* temp_mat = (float*)malloc(6*n*sizeof(float));
    float* temp_mat = (float*)malloc((4*max_num + 2*lines)*sizeof(float));


    for(int i=0;i<max_num;i++){ 
        tE[i*2] = i;
        tE[i*2+1] = (l_r/max_num);
    }
    
    // cout<<"reached before main loop."<<endl;
    //Main loop
    while (itr == 0 || itr<30){//} || abs(curr-prev)>=10e-12){
        map_pairs(A,p,max_num,lines,to_mapper); //Working
        // print_vector<float>(to_mapper,lines,4);
        // break;
        // cout<<"Pairs mapped"<<endl;

        //Handling dangling nodes
        // vector<PAIR> d_dot_p = mr.secondary_map_task(d,to_mapper,n);
        mr.secondary_map_task(d,p,max_num,temp_mat);
        // print_vector<float>(temp_mat,max_num,2);
        // break;
        // cout<<"Secondary map_task completed."<<endl;
        // vector<PAIR> dot_prod = mr.reduce_task(d_dot_p,1); //The length will be 1 with all summed p_js
        float* dot_prod = (float*)malloc(2*sizeof(float));
        mr.reduce_task(temp_mat,max_num,1,dot_prod);
        // cout<<dot_prod[0]<<" "<<dot_prod[1]<<endl;
        // break;
        // cout<<"Added all the p_js."<<endl;
        // vector<PAIR> Dp = dangling_mul(dot_prod,n);
        // float* Dp = dangling_mul(dot_prod,n);
        dangling_mul(dot_prod,max_num,temp_mat);
        // print_vector<float>(temp_mat,max_num,2);
        // break;
        // cout<<"Dp calculated."<<endl;

        mr.primary_map_task(to_mapper,lines,max_num,temp_mat+2*max_num);

        emit_zero_pairs(max_num,temp_mat + 2*max_num + 2*lines);

        // cout<<"Primary map task done."<<endl;
        mr.reduce_task(temp_mat,((2*max_num)+lines),max_num,v2);

        // cout<<"reduced."<<endl;
        vec_multiply(v2,max_num,l_f);


        vec_add(v2,tE,max_num);
        if (itr!=0){
            prev = curr;
        }
        curr = sum_of_vec(v2,max_num);
        // cout<<"Current sum of all p_js is: "<<curr<<endl;
        for(int i=0;i<max_num;i++){
            p[i]=v2[2*i+1];
        }
        // if (itr==2){
            // break;
        // }
        // p = new_p;
        itr++;
    } 

    write_to_file(outfilename,v2,max_num);

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
