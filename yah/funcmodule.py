import json
import os
import math, shutil
from fsplit.filesplit import Filesplit
fs = Filesplit()

def dfs_put(num_datanodes, path_to_namenodes, block_size, directory, file_location, path_to_datanodes):
    size = os.path.getsize(file_location)
    output_dir = path_to_datanodes
    fs.split(file=file_location,
             split_size=block_size*(2**10), output_dir=output_dir, newline=True)
    f = open(path_to_namenodes+'/namenode.json', 'r+')
    num_of_files = math.ceil(size/(block_size*1024))

    dict1 = {}
    for j in range(1, num_of_files+1):
        temp = os.path.splitext(os.path.basename(file_location))[0]+'_'+str(j)+os.path.splitext(os.path.basename(file_location))[1]
        datanode = 'datanode'+str((j-1) % num_datanodes+1)
        dict1[temp] = datanode
    dict_file = {}
    dict_file[output_dir] = dict1
    temp_json = {directory: dict_file}
    # json.dump(temp_json, f)
    try:
        js = json.loads(f.read())
        js.update(temp_json)
        f = open(path_to_namenodes+'/namenode.json', 'w+')
        json.dump(js,f)
    except Exception as e:
        print(e)
        json.dump(temp_json,f)

    f = open(path_to_namenodes+'/namenode.json', 'a+')
    data = json.load(f)
    for i in data[directory][output_dir]:
        shutil.move(output_dir+'/'+i, path_to_datanodes +
                    '/'+data[directory][output_dir][i]+'/'+i)

def dfs_ls(path,path_to_namenode):
    f=open(path_to_namenode+"/namenode.json")
    js = json.load