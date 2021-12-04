import json, os, math

def dfs_put(num_datanodes,path_to_namenodes,block_size):
    size = os.path.getsize('C:\PESU college stuff\Sem 5\Sem 5\Big Data\Project\cleaned_nba_data.csv')
    num_of_files=math.ceil(size/(block_size*1024))
    f_block="file_block"
    dnode="datanode"
    dict = {}
    for i in range(num_datanodes):
        temp=dnode+str(i)
        dict[temp]=[]
    print(size,num_of_files)
    for i in range(num_of_files):
        t=i%num_datanodes
        temp=dnode+str(t)
        dict[temp].append(f_block+str(i))
    f = open(path_to_namenodes+"namenode.json", "w")
    json.dump(dict, f)
    f.close()