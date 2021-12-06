import json
import os
import math, shutil
from fsplit.filesplit import Filesplit
fs = Filesplit()

def dfs_put(num_datanodes, path_to_namenodes, block_size, directory, file_location, path_to_datanodes):
    size = os.path.getsize(file_location)
    output_dir = "/Users/naren/Downloads/Datanode"
    fs.split(file=file_location,
             split_size=block_size*(2**10), output_dir=output_dir, newline=True)
    f = open(path_to_namenodes+'/namenode.json', 'w+')
    num_of_files = math.ceil(size/(block_size*1024))

    dict1 = {}
    for j in range(1, num_of_files+1):
        temp = os.path.splitext(os.path.basename(file_location))[0]+'_'+str(j)
        datanode = 'datanode'+str((j-1) % num_datanodes+1)
        dict1[temp] = datanode
    dict_file = {}
    dict_file[output_dir] = dict1
    temp_json = {directory: dict_file}
    json.dump(temp_json, f)
    fileext = os.path.splitext(file_location)[1]

    f = open(path_to_namenodes+'/namenode.json', 'r+')
    data = json.load(f)
    for i in data[directory][output_dir]:
        shutil.move(output_dir+'/'+i+fileext, path_to_datanodes +
                    data[directory][output_dir][i]+'/'+i+fileext)