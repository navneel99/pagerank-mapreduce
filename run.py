import os
import subprocess

f = open("timings.txt","r")
l = f.readlines()
f.close()


exe1 = "./mr-pr-cpp.o"
exe2 = "mpirun --quiet -np 4 mr-pr-mpi.o"
exec3 = "mpirun -np 4 pagerank"




for filename in l:
    filename = filename.rstrip("\n")
    e1 = [exe1]
    e2 = exe2.split()
    tmp = ["test/"+filename,"-o","outputs/"+filename.split(".")[0]+"-pr1-cpp.txt"]
    e1.extend(tmp)
    subprocess.run(e1)
    print(filename+" done.")
    break
