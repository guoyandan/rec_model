# -*- coding: utf-8 -*-
# @Time        : 2019/12/30 16:19
# @Author      : tianyunzqs
# @Description :
import os
import logging
import logging.handlers

from utils.global_constant import config
from utils.path_utils import get_full_path

class Logger(object):
    def __init__(self,
                 filename,
                 level=logging.INFO,
                 ):

        file_dir = os.path.split(filename)[0]
        # 判断文件路径是否存在，如果不存在，则创建，此处是创建多级目录
        if not os.path.isdir(file_dir):
            os.makedirs(file_dir)

        self.logger = logging.getLogger(filename)
        # 设置日志格式
        format_str = logging.Formatter('%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        # 设置日志级别
        self.logger.setLevel(level)
        # 往屏幕上输出
        sh = logging.StreamHandler()
        # 设置屏幕上显示的格式
        sh.setFormatter(format_str)
        # 实例化TimedRotatingFileHandler
        # interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
        # S 秒
        # M 分
        # H 小时
        # D 天
        # W 每星期（interval==0时代表星期一）
        # midnight 每天凌晨
        th = logging.handlers.TimedRotatingFileHandler(filename=filename,
                                                       when='D',
                                                       interval=1,
                                                       backupCount=5,
                                                       encoding='utf-8'
                                                       )
        th.suffix = "%Y-%m-%d.log"
        # 设置文件里写入的格式
        th.setFormatter(format_str)
        # 把对象加到logger里
        self.logger.addHandler(sh)
        self.logger.addHandler(th)

logger = Logger(get_full_path(config.get('logs', 'log_path'))).logger