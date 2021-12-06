import sys, json, os, argparse
# from .classmodule import MyClass
from .funcmodule import dfs_put

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
        os.makedirs(path_to_datanodes+'datanode'+str(i))
    except:
        pass

def main():
    args = sys.argv[1:]
    if args[0] == 'mkdir':
        fw = open(path_to_namenodes+'/namenode.json', 'w')
        if os.stat(path_to_namenodes+'/namenode.json').st_size == 0:
            dictionary_dir = {}
            dictionary_dir[args[1]] = None
            json.dump(dictionary_dir, fw)
        else:
            fr = open(path_to_namenodes+'/namenode.json', 'r')
            namenode = json.load(fr)
            if args[1] in namenode:
                print('directory already exists, create a new one with a different name')

    elif args[0] == 'put':
        dfs_put(num_datanodes=num_datanodes,
                path_to_namenodes=path_to_namenodes, block_size=block_size, directory=args[2], file_location=args[1] , path_to_datanodes=path_to_datanodes)
    else:
        pargs = parser.parse_args()
        mapper_path = pargs.mapper
        reducer_path = pargs.reducer
        input_path = pargs.input
        output_path = pargs.output
        for i in range(1,4):
            os.system(f'cat {input_path}_{i}.txt | Python3 {mapper_path} >> /Users/naren/Downloads/op.txt')
        os.system(f'cat /Users/naren/Downloads/op.txt | sort -k 1,1 | Python3 {reducer_path}')
if __name__ == '__main__':
    main()