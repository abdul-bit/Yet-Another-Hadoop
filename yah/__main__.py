from os import path
import sys, json
# from .classmodule import MyClass
from .funcmodule import dfs_put

def main():
    print('in main')
    args = sys.argv[1:]
    print('count of args :: {}'.format(len(args)))
    for arg in args:
        print('passed argument :: {}'.format(arg))

    # my_function('hello world')

    # my_object = MyClass('Thomas')
    # my_object.say_name()
    f = open('config_sample.json')
    data = json.load(f)
    block_size = data["block_size"]
    path_to_namenodes = data["path_to_namenodes"]
    num_datanodes = data["num_datanodes"]
    dfs_put(num_datanodes=num_datanodes,path_to_namenodes=path_to_namenodes,block_size=block_size)
if __name__ == '__main__':
    main()