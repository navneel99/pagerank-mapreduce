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
    int ind = 0;

    if (myfile.is_open()){
        while(getline(myfile,line)){
            long long int n1 = stoi(line.substr(0,line.find(" "))),n2 = stoi(line.substr(line.find(" ")+1,line.length()));
            // cout<<n1<<" "<<n2<<endl;
            // ans[n1] = n2;
            ans[ind++] = n1;
            ans[ind++]= n2;
        }
    }
    // return ans;
}

//Function to create key value pairs to input in the mapper
// void map_pairs(float* link_vec,float* prob,long long int n,float*ans){

//     for (int i=0;i<n;i++){
//         ans[i*4]=i;
//         float c_p = link_vec[i];
//         ans[i*4+1] = prob[i];
//         if (c_p == -1){ //Num of links
//             ans[i*4+2]=0;
//         }else{
//             ans[i*4+2] = 1;
//             ans[i*4+3] = c_p; 
//         }
//     }
// }
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

// void set_initial_prob(float* p, long long int n){
//     float u = ((1.0)/n) ;
//     // cout<<"u: "<<u<<endl;
//     for ( int i=0;i<n;i++){
//         p[i]= u;
//     }
// }
void set_initial_prob(float* p, int max_num){
    float u = ((1.0)/max_num) ;
    for ( int i=0;i<max_num;i++){
        p[i]= u;
    }
}


// tuple<float*,float*> change_format(vector<PAIR> v,long long int n){
//     float* keys,*vals;
//     keys = (float*)malloc(n*sizeof(float));
//     vals = (float*)malloc(n*sizeof(float));
//     for (int i=0;i<n;i++){
//         keys[i] = get<0>(v[i]);
//         vals[i] = get<1>(v[i]);
//     }
//     return make_tuple(keys,vals);
// }

// void get_dangling_nodes(float* link_vec, long long int n,bool*d){
//     for(int i=0;i<n;i++){
//         if(link_vec[i]==-1){
//             d[i]=true;
//         }else{
//             d[i]=false;
//         }
//     }
// }
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

// void dangling_mul(float* dot_prod,long long int n,float*ans,float multiplicative_factor=1){

// //     for (int i=0;i<n;i++){
// //         ans[i*2]=i;
// //         ans[i*2+1] = dot_prod[1]*(multiplicative_factor/n);
// //     }
// // }

void dangling_mul(float* dot_prod,long long int max_num,float*ans,float multiplicative_factor=1){

    for (int i=0;i<max_num;i++){
        ans[i*2]=i;
        ans[i*2+1] = dot_prod[1]*(multiplicative_factor/max_num);
        // ans.push_back(make_tuple(i,(get<1>(dot_prod[0]))*(multiplicative_factor/n)));
    }
    // return ans;
}

void vec_multiply(float*v,long long int n,float mul_fac=1){
    for (int i=0;i<n;i++){
        v[i*2+1] = v[i*2+1]*mul_fac;
    }
}

void vec_add(float*v1,float*v2,long long int n){

        for(int i=0;i<n;i++){
            v1[2*i+1] = v1[2*i+1]+v2[2*i+1];
        }
        
}


// void write_to_file(string fname,tuple<float*,float*> tup,long long int n){
void write_to_file(string fname,float* tup,long long int n,int proc){


    ofstream myfile(fname);
    for(long long int i=0;i<n;i++){
        // myfile<<keys[i]<<" = "<<vals[i]<<"\n";
        myfile<<tup[i*2]/proc<<" = "<<tup[i*2+1]<<"\n";
        // myfile<<tup[i*2]<<" = "<<tup[i*2+1]<<"\n";

    }
    myfile<<"s = 1.0"<<"\n";
    myfile.close();
}

// void print_vector(float* v,long long int n,int m=2){
//     for (int i=0;i<n;i++){
//         for(int j=0;j<m;j++){
//             cout<<v[i*m+j]<<" ";
//         }
//         cout<<"\n";
//     }
// }
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

void emit_zero_pairs(int max_num,float *temp_mat){
    for (int i=0;i<max_num;i++){
        temp_mat[i*2] = i;
        temp_mat[i*2+1] = 0;
    }
}



int main(int argc, char *argv[])
{

    //Parses Argument and gets back 'n', the number of nodes 
    string fname = argv[1];
    string outflag = argv[2]; //Essentially just "-o"
    string outfilename = argv[3];

    vector<int> info = fileinfo(fname);
    int lines = info[0],max_num = info[1];
    max_num++;
    int n = max(lines,max_num);
    cout<<"lines: "<<lines<<" max_num: "<<max_num<<endl;


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
    // p = (float *)malloc(n*sizeof(float));
    // A = (float*)malloc(n*sizeof(float));
    // d = (bool*)malloc(n*sizeof(bool));
    p = (float *)malloc(max_num*sizeof(float));
    A = (float*)malloc(2*lines*sizeof(float));
    d = (bool*)malloc(max_num*sizeof(bool));

    //Initializes the variables.
    // link_mat_creator(fname,n,A); //Working
    // get_dangling_nodes(A,n,d); 
    // set_initial_prob(p,n); //Working
    link_mat_creator(fname,2*lines,A); //Working
    get_dangling_nodes(A,lines,d,max_num);  //working
    set_initial_prob(p,max_num); //Working

    //Acc. to google
    float l_f=0.85;
    float l_r= 1-l_f;

    //Variables to use in the program.
    float* to_mapper,*mapper_part;
    float prev = 0;
    float curr = prev;
    long int itr = 0;

    float *v,*v2;
    // to_mapper = (float*)malloc(4*n*sizeof(float));
    // mapper_part = (float*)malloc(4*(n/proc)*sizeof(float));
    // v2=(float*)malloc(2*n*sizeof(float));
    // v=(float*)malloc(2*n*sizeof(float));

    to_mapper = (float*)malloc(4*lines*sizeof(float));
    v2=(float*)malloc(2*max_num*sizeof(float));
    v=(float*)malloc(2*max_num*sizeof(float));
    if (id != proc-1){
        mapper_part = (float*)malloc(4*(lines/proc)*sizeof(float));
    }else{
        mapper_part = (float*)malloc(4*((lines/proc)+(lines%proc))*sizeof(float));
    }

    // tuple<float*,float*> tup;
    
    // float* tup;
    MapReduce mr;

    //The random tE(Probability to move to any page)
    // vector<PAIR> tE;
    float* tE;
    tE = (float*)malloc(2*max_num*sizeof(float));
    // float* temp_mat = (float*)malloc(6*n*sizeof(float));
    float* temp_mat = (float*)malloc((4*max_num + 2*lines)*sizeof(float));

    // float* temp_mat_part = (float*)malloc(6*(n/proc)*sizeof(float));
    float * temp_mat_part;
    if (id == proc-1){
    // temp_mat_part = (float*)malloc (((2*lines)/proc)+(2*(lines%proc))*sizeof(float));
    temp_mat_part = (float*)malloc((2*((2*max_num+lines)/proc) + 2*((2*max_num+lines)%proc))*sizeof(float));
    }else{
    // temp_mat_part = (float*)malloc(((2*lines)/proc)*sizeof(float));
    temp_mat_part = (float*)malloc(((4*max_num+2*lines)/proc)*sizeof(float));
    }

    float* dot_prod = (float*)malloc(2*sizeof(float));

    if (id==0){
        wtime = MPI_Wtime();
    }
    for(int i=0;i<max_num;i++){ 
        tE[i*2] = i;
        tE[i*2+1] = (l_r/max_num);
    }
    
    bool while_cond = true;
    //Main loop
    int scounts[proc], displs[proc],srecvs[proc],rec_displs[proc];
    while(while_cond){
  
        if (id==0){
            // map_pairs(A,p,n,to_mapper); //Working
            map_pairs(A,p,max_num,lines,to_mapper); //Working

        }
        for (int i=0;i<proc;i++){
            displs[i] = 4*(i*(lines/proc));
            rec_displs[i] = 2*(i*(lines/proc));
            if (i!=proc-1){
                scounts[i] = 4*(lines/proc);
                srecvs[i] = 2*(lines/proc);
            }else{
                scounts[i] = 4*((lines/proc)+(lines%proc));
                srecvs[i] = 2*((lines/proc)+(lines%proc));

            }
            // cout<<"displ["<<i<<"]: "<<displs[i]<<" scounts["<<i<<"]: "<<scounts[i]<<endl;
        }
        int recv_size;
        if (id != proc-1){
            recv_size = 4*(lines/proc);
        }else{
            recv_size = 4*(lines/proc + (lines%proc));
        }
        
        //Handling dangling nodes
        // MPI_Scatter(to_mapper,4*n/proc,MPI_FLOAT,mapper_part,4*n/proc,MPI_FLOAT,0,MPI_COMM_WORLD);
        MPI_Scatterv(to_mapper,scounts,displs,MPI_FLOAT,mapper_part,recv_size,MPI_FLOAT,0,MPI_COMM_WORLD);

        MPI_Barrier(MPI_COMM_WORLD);


        if (id==0){
            mr.secondary_map_task(d,p,max_num,temp_mat);

            mr.reduce_task(temp_mat,max_num,1,dot_prod);

            dangling_mul(dot_prod,max_num,temp_mat);
            emit_zero_pairs(max_num,temp_mat+2*lines+2*max_num);

        }
        
        // mr.primary_map_task(mapper_part,n/proc,temp_mat_part);
        mr.primary_map_task(mapper_part,recv_size/4,max_num,temp_mat_part);
        // cout<<"proc id: "<<id<<" ok."<<endl;
        // MPI_Gather(temp_mat_part,(4*n/proc),MPI_FLOAT,temp_mat+2*n,(4*n/proc),MPI_FLOAT,0,MPI_COMM_WORLD);
        MPI_Gatherv(temp_mat_part,(recv_size)/2,MPI_FLOAT,temp_mat+2*max_num,srecvs,rec_displs,MPI_FLOAT,0,MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);
        for (int i=0;i<proc;i++){
            displs[i] = max(0,(2*i*((2*max_num+lines)/proc)));
            // rec_displs[i] = 2*(i*(lines/proc));
            if (i!=proc-1){
                scounts[i] = ((4*max_num+2*lines)/proc)-1;
                // srecvs[i] = 2*(lines/proc);
            }else{
                scounts[i] = (2*((2*max_num+lines)/proc)+2*((2*max_num+lines)%proc));
                // srecvs[i] = 2*((lines/proc)+(lines%proc));
            }
            // cout<<"displ["<<i<<"]: "<<displs[i]<<" scounts["<<i<<"]: "<<scounts[i]<<endl;
        }

        // cout<<"Here."<<endl;
        // MPI_Scatter(temp_mat,6*n/proc,MPI_FLOAT,temp_mat_part,6*n/proc,MPI_FLOAT,0,MPI_COMM_WORLD);
        // MPI_Scatter(temp_mat,6*n/proc,MPI_FLOAT,temp_mat_part,6*n/proc,MPI_FLOAT,0,MPI_COMM_WORLD);
        MPI_Scatterv(temp_mat,scounts,displs,MPI_FLOAT,temp_mat_part,scounts[id],MPI_FLOAT,0,MPI_COMM_WORLD);
        mr.reduce_task(temp_mat_part,scounts[id]/2,max_num,v2);
        MPI_Reduce(v2,v,2*max_num,MPI_FLOAT,MPI_SUM,0,MPI_COMM_WORLD);
        if (id==0){

            // mr.reduce_task(temp_mat,(2*max_num+lines),max_num,v2);

            // vec_multiply(v,n,l_f);
            // vec_multiply(v2,max_num,l_f);
            vec_multiply(v,max_num,l_f);


            // vec_add(v,tE,n);
            // vec_add(v2,tE,max_num);
            vec_add(v,tE,max_num);

            if (itr!=0){
                prev = curr;
            }
            // curr = sum_of_vec(v2,max_num);
            curr = sum_of_vec(v,max_num);

            for(int i=0;i<max_num;i++){
                // p[i]=v2[2*i+1];
                p[i]=v[2*i+1];
            }

        }
            // break;
            itr++;

        if (id==0){
            while_cond = (id==0 and (itr==0 or itr<30));// abs(curr-prev)>=10e-9));// or (id !=0 and toDo==true);
        }
        MPI_Barrier(MPI_COMM_WORLD);
        MPI_Bcast(&while_cond,1,MPI_C_BOOL,0,MPI_COMM_WORLD);
        // cout<<"id: "<<id<<" while_cond: "<<while_cond<<endl;
        MPI_Barrier(MPI_COMM_WORLD);
    } 

    cout<<"Itr: "<<itr<<" proc: "<<id<<endl;    
    if (id==0){
        wtime = MPI_Wtime() - wtime;
        cout<<"Time taken: "<<wtime<<endl;

        // write_to_file(outfilename,v2,max_num,proc);
        write_to_file(outfilename,v,max_num,proc);

    }

    // MPI_Finalize();
    return 0;
}
