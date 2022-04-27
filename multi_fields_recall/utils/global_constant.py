import configparser

from utils.path_utils import get_full_path
config = configparser.ConfigParser()
config.read([get_full_path('conf/global_rec.conf')], encoding="utf-8")
config.read([get_full_path('conf/{}_rec.conf'.format(config.get("env","env")))], encoding="utf-8")
