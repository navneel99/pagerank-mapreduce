#include "mpi-lib.h"



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


void write_to_file(string fname,tuple<float*,float*> tup,long long int n){
    float* keys = get<0>(tup);
    float* vals = get<1>(tup);

    ofstream myfile(fname);
    for(long long int i=0;i<n;i++){
        myfile<<keys[i]<<" = "<<vals[i]<<"\n";
    }
    myfile<<"s = 1.0"<<"\n";
    myfile.close();
}

float* unpack(vector<PAIR> v){
    int l = v.size();
    float *ans = (float*)malloc(l*2*sizeof(float));
    // ans[0] = 2*l+1;
    for(int i=0;i<l;i++){
        k = get<0>(v[i]);
        val = get<1>(v[i]);
        ans[i*2] = k;
        ans[i*2]=val;
    }
    return ans;
}

vector<PAIR> pack(float* f,long long int n){
    // int l = (int)f[0];
    vector<PAIR> ans;
    for(int i=0;i<(l)/2;i++){
        float k = f[i*2];
        float v = f[i*2];
        ans.push_back(make_tuple(k,v));
    }
    return ans;
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

    //Instantiates various arrays for subsequent use.
    float* A;
    bool *d;
    float* p;
    p = (float *)malloc(n*sizeof(float));

    //Initializes the variables.
    A = link_mat_creator(fname,n);
    d = get_dangling_nodes(A,n);
    set_initial_prob(p,n);

    // cout<<"in main() all val of p: "<<p[1]<<endl;

    //Acc. to google
    float l_f=0.85;
    float l_r= 1-l_f;

    //Variables to use in the program.
    tuple<float*,float*> to_mapper;// = map_pairs(A,p,n);    
    float prev = 0;
    float curr = prev;
    long int itr = 0;
    vector<PAIR>v,v2;
    tuple<float*,float*> tup;
    MapReduce mr;

    //The random tE(Probability to move to any page)
    vector<PAIR> tE;
    for(int i=0;i<n;i++){
       tE.push_back(make_tuple(i, (l_r/n) ));
    }
    int *scounts,*displs;
    bool toDo=true;
    bool while_cond = (id==0 and (itr==0 or abs(curr-prev)>=10e-9)) or (id !=0 and toDo==true);
    //Main loop
        // while (itr == 0 or abs(curr-prev)>=10e-9){
        while(while_cond){
            to_mapper = map_pairs(A,p,n);
            //Sends message to other process to do this iteration
            // toDo = true;
            MPI_Bcast(toDo,1,MPI_BOOL,0,MPI_COMM_WORLD);
            if (id!=0 and toDo==false){
                break;
            }

            if (id==0){
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
            
                long long int v_sz = v.size();
                float *uv = unpack(v);
                
                //How to get the length from the root process
                scounts = (int*)malloc(sizeof(int)*p);
                displs =  (int*)malloc(sizeof(int)*p);
                for (int i=0;i<p;i++){
                    if (i==p-1){
                        scounts[i]=(v_sz/p)+(v_sz%p);
                    }else{
                        scounts[i]=(v_sz/p);
                    }
                    displs[i] = i*(v_sz/p);
                }
            }
            int rbufcount,rbufdata;
            float* v_dash;
            MPI_Scatter(scounts,1,MPI_INT,rbufcount,1,MPI_INT,0,MPI_COMM_WORLD); //Sending the count to all process;
            MPI_Barrier(MPI_COMM_WORLD);
            MPI_Scatterv(v,scounts,displs,MPI_FLOAT,v_dash,MPI_FLOAT,0,MPI_COMM_WORLD);
            vector<PAIR> v_temp = pack(v_dash,rbufcount);
            v2 = mr.reduce_task(v_temp,n);

            // v2 = mr.reduce_task(v,n);

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

            while_cond = (id==0 and (itr==0 or abs(curr-prev)>=10e-9)) or (id !=0 and toDo==true);
        } 
        if (id == 0){
            toDo=false;
            MPI_Bcast(toDo,1,MPI_BOOL,0,MPI_COMM_WORLD);
        }
        
        
        write_to_file(outfilename,tup,n);

        cout<<"Itr: "<<itr<<endl;

    ierr = MPI_Finalize();
    return 0;
}
