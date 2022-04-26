import os

def get_full_path(file_name):
    '''
    :param file_name: 项目根目录下的相对文件路径
    :return:
    '''
    return  os.path.realpath( os.path.join(os.path.split(os.path.realpath(__file__))[0],"..",file_name))
