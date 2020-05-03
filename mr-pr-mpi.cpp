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
            // cout<<n1<<" "<<n2<<endl;
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
void write_to_file(string fname,float* tup,long long int n,int proc){

    // float* keys = get<0>(tup);
    // float* vals = get<1>(tup);

    ofstream myfile(fname);
    for(long long int i=0;i<n;i++){
        // myfile<<keys[i]<<" = "<<vals[i]<<"\n";
        myfile<<tup[i*2]/proc<<" = "<<tup[i*2+1]<<"\n";
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
int main(int argc, char *argv[])
{

    //Parses Argument and gets back 'n', the number of nodes 
    string fname = argv[1];
    string outflag = argv[2]; //Essentially just "-o"
    string outfilename = argv[3];
    string delim1 = "-",delim2=".";
    long long int n = stoi(fname.substr(fname.find(delim1)+1,fname.find(delim2))); 


    //MPI vars
    int id;
    int proc;
    int ierr = MPI_Init(&argc,&argv);
    ierr = MPI_Comm_size(MPI_COMM_WORLD,&proc);
    ierr = MPI_Comm_rank(MPI_COMM_WORLD,&id);

    double wtime;
    
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

    //Acc. to google
    float l_f=0.85;
    float l_r= 1-l_f;

    //Variables to use in the program.
    float* to_mapper,*mapper_part;
    float prev = 0;
    float curr = prev;
    long int itr = 0;

    float *v,*v2;
    to_mapper = (float*)malloc(4*n*sizeof(float));
    mapper_part = (float*)malloc(4*(n/proc)*sizeof(float));
    v2=(float*)malloc(2*n*sizeof(float));
    v=(float*)malloc(2*n*sizeof(float));

    // tuple<float*,float*> tup;
    
    float* tup;
    MapReduce mr;

    //The random tE(Probability to move to any page)
    // vector<PAIR> tE;
    float* tE;
    tE = (float*)malloc(2*n*sizeof(float));
    float* temp_mat = (float*)malloc(6*n*sizeof(float));

    float* temp_mat_part = (float*)malloc(6*(n/proc)*sizeof(float));
    float* dot_prod = (float*)malloc(2*sizeof(float));

    if (id==0){
        wtime = MPI_Wtime();
    }
    for(int i=0;i<n;i++){ 
        tE[i*2] = i;
        tE[i*2+1] = (l_r/n);
    }
    
    // bool toDo=true;
    // bool while_cond = (id==0 and (itr==0 or abs(curr-prev)>=10e-9)) or (id !=0 and toDo==true);    
    bool while_cond = true;
    //Main loop
    // while (itr == 0 or abs(curr-prev)>=10e-9){
    while(while_cond){
  
        if (id==0){
            map_pairs(A,p,n,to_mapper); //Working
        }
        //Handling dangling nodes
        MPI_Scatter(to_mapper,4*n/proc,MPI_FLOAT,mapper_part,4*n/proc,MPI_FLOAT,0,MPI_COMM_WORLD);
        // MPI_Barrier(MPI_COMM_WORLD);
        mr.secondary_map_task(d+(id*n/proc),mapper_part,n/proc,temp_mat_part);
        MPI_Gather(temp_mat_part,(2*n/proc),MPI_FLOAT,temp_mat,(2*n/proc),MPI_FLOAT,0,MPI_COMM_WORLD);

        if (id==0){
            mr.reduce_task(temp_mat,n,1,dot_prod);
            dangling_mul(dot_prod,n,temp_mat);

        }
        
        mr.primary_map_task(mapper_part,n/proc,temp_mat_part);
        MPI_Gather(temp_mat_part,(4*n/proc),MPI_FLOAT,temp_mat+2*n,(4*n/proc),MPI_FLOAT,0,MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        MPI_Scatter(temp_mat,6*n/proc,MPI_FLOAT,temp_mat_part,6*n/proc,MPI_FLOAT,0,MPI_COMM_WORLD);
        mr.reduce_task(temp_mat_part,3*n/proc,n,v2);
        MPI_Reduce(v2,v,2*n,MPI_FLOAT,MPI_SUM,0,MPI_COMM_WORLD);
        if (id==0){

            vec_multiply(v,n,l_f);


            vec_add(v,tE,n);
            if (itr!=0){
                prev = curr;
            }
            curr = sum_of_vec(v,n);
            for(int i=0;i<n;i++){
                p[i]=v[2*i+1];
            }

        }
            // break;
            itr++;

        if (id==0){
            while_cond = (id==0 and (itr==0 or abs(curr-prev)>=10e-9));// or (id !=0 and toDo==true);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&while_cond,1,MPI_C_BOOL,0,MPI_COMM_WORLD);
        // cout<<"id: "<<id<<" while_cond: "<<while_cond<<endl;
        MPI_Barrier(MPI_COMM_WORLD);

    } 

    
    if (id==0){
        wtime = MPI_Wtime() - wtime;
        cout<<"Time taken: "<<wtime<<endl;

        write_to_file(outfilename,v,n,proc);
    }

    cout<<"Itr: "<<itr<<" proc: "<<id<<endl;
    ierr = MPI_Finalize();
    return 0;
}
