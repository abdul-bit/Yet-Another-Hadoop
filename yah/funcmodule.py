import json
import os
import math
from fsplit.filesplit import Filesplit

fs = Filesplit()


def dfs_put(num_datanodes, path_to_namenodes, block_size, directory, file_location):
    size = os.path.getsize(file_location)
    fs.split(file=file_location,
             split_size=block_size*(2**10), output_dir="/Users/naren/Downloads/Datanode", newline=True)
    f = open(path_to_namenodes+'/namenode.json', 'w+')
    num_of_files = math.ceil(size/(block_size*1024))

    dict1 = {}
    for j in range(1, num_of_files+1):
        temp = os.path.splitext(os.path.basename(file_location))[0]+'_'+str(j)
        dict1[temp] = []
    for j in range(1, num_of_files+1):
        temp = os.path.splitext(os.path.basename(file_location))[0]+'_'+str(j)
        datanode = 'datanode'+str((j-1) % num_datanodes+1)
        dict1[temp].append(datanode)
    dict_file = {}
    dict_file[os.path.basename(file_location)] = dict1
    temp_json = {directory: dict_file}
    json.dump(temp_json, f)
