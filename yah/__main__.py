import sys, json, os, argparse
# from .classmodule import MyClass
from .funcmodule import dfs_ls, dfs_put,dfs_cat,dfs_mkdir

f = open('config_sample.json')
data = json.load(f)
block_size = data["block_size"]
path_to_datanodes = data["path_to_datanodes"]
path_to_namenodes = data["path_to_namenodes"]
replication_factor = data["replication_factor"]
num_datanodes = data["num_datanodes"]
datanode_size = data["datanode_size"]
sync_period = data["sync_period"]
datanode_log_path = data["datanode_log_path"]
namenode_log_path = data["namenode_log_path"]
namenode_checkpoints = data["namenode_checkpoints"]
fs_path = data["fs_path"]
dfs_setup_config = data["dfs_setup_config"]

parser = argparse.ArgumentParser()
parser.add_argument('-mapper')
parser.add_argument('-reducer')
parser.add_argument('-input')
parser.add_argument('-output')

for i in range(1, num_datanodes+1):
    try:
        os.makedirs(path_to_datanodes+'/datanode'+str(i))
    except Exception as e:
        pass

def main():
    args = sys.argv[1:]
    if args[0] == 'mkdir':
        dfs_mkdir(path_to_datanodes=path_to_datanodes,path=args[1])
    elif args[0] == 'put':
        dfs_put(num_datanodes=num_datanodes,
                path_to_namenodes=path_to_namenodes, block_size=block_size, directory=args[2], file_location=args[1] , path_to_datanodes=path_to_datanodes)
    elif args[0] == 'ls':
        dfs_ls(path_to_datanodes=path_to_datanodes,path=args[1])
    elif args[0] == 'cat':
        dfs_cat(path_to_datanodes=path_to_datanodes,path=args[1])
    else:
        pargs = parser.parse_args()
        mapper_path = pargs.mapper
        reducer_path = pargs.reducer
        input_path = pargs.input
        output_path = pargs.output
        f = open(path_to_namenodes+'/namenode.json', 'r')
        data = json.load(f)
        for i in data[input_path][path_to_datanodes]:

            path = path_to_datanodes +'/'+data[input_path][path_to_datanodes][i]+'/'+i
            os.system(f'cat {path} | Python3 {mapper_path} >> op.txt')
        os.system(f'cat op.txt | sort -k 1,1 | Python3 {reducer_path} > {path_to_datanodes}/path{output_path}/op.txt')
        os.remove('op.txt')
if __name__ == '__main__':
    main()