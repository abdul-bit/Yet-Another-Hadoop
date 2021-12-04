import json, os, math

def dfs_put(num_datanodes,path_to_namenodes,block_size):
    size = os.path.getsize('/Users/naren/Downloads/embedding-sample2.json')
    num_of_files=math.ceil(size/(block_size*1024))
    f_block="file_block"
    dnode="datanode"
    input_path = "/Users/naren/Downloads/embedding-sample2.json"
    dict = {}
    for i in range(1, num_datanodes+1):
        dict['datanode'+str(i)] = []

    CHUNK_SIZE = (block_size-1)*1024

    file_number = 1
    with open(input_path, encoding="utf8") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:
            with open('/Users/naren/Downloads/Datanode/filechunk' + str(file_number), 'w', encoding="utf8") as chunk_file:
                chunk_file.write(chunk)
            z = 'datanode'+str(((file_number-1) % num_datanodes)+1)
            dict[z].append('file_chunk'+str(file_number))
            file_number += 1
            chunk = f.read(CHUNK_SIZE)

    f_json = open(path_to_namenodes+"name_node.json", 'w')
    json.dump(dict, f_json)
    f_json.close()
    # dict = {}
    # for i in range(num_datanodes):
    #     temp=dnode+str(i)
    #     dict[temp]=[]
    # print(size,num_of_files)
    # for i in range(num_of_files):
    #     t=i%num_datanodes
    #     temp=dnode+str(t)
    #     dict[temp].append(f_block+str(i))
    # f = open(path_to_namenodes+"namenode.json", "w")
    # print(path_to_namenodes)
    # json.dump(dict, f)
    # f.close()

