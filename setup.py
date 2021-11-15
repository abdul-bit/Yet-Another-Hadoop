import json
import os
# from pickle import TRUE
# import subprocess
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
print(block_size, path_to_datanodes, path_to_namenodes, replication_factor, num_datanodes, datanode_size,
      sync_period, datanode_log_path, namenode_log_path, namenode_checkpoints, fs_path, dfs_setup_config)
os.makedirs(path_to_datanodes)
os.makedirs(path_to_namenodes)
os.makedirs(datanode_log_path)
os.makedirs(namenode_log_path)
os.makedirs(namenode_checkpoints)
os.makedirs(fs_path)
os.makedirs(dfs_setup_config)
f.close()

for i in range(1, num_datanodes):
    filename = 'DataNode'
    txt = '.txt'
    filename1 = path_to_datanodes+filename+str(i)+txt
    with open(filename1, 'w') as f:
        f.seek(1024*64)
        f.write('0')
        # exe = 'hachoir-metadata'
        # process = subprocess.Popen(
        #     [exe, filename1], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=TRUE)
        # for output in process.stdout:
        #     print(output.strip())
