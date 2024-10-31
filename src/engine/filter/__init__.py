import os
import glob
import sys
import importlib
import sqlite3
import gflags
import math
import json
import pandas as pd
from conf import *

# 获取当前模块所在的路径
current_dir = os.path.dirname(os.path.abspath(__file__))

# 将当前模块所在目录添加到系统路径
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 动态加载 feature 目录下所有 *_feature.py 文件
modules = glob.glob(os.path.join(current_dir, "*_filter.py"))

# 创建一个字典来存储所有的 *_FEATURE 类
feature_classes = {}
# 动态导入所有 *_feature.py 文件
for module_path in modules:
    module_name = os.path.basename(module_path)[:-3]  # 去掉 .py 扩展名
    imported_module = importlib.import_module(f'filter.{module_name}')
    # 查找模块中的所有类，并找到 *_FEATURE 类
    for attribute_name in dir(imported_module):
        attribute = getattr(imported_module, attribute_name)
        if isinstance(attribute, type) and attribute_name.endswith('_FILTER'):  # 匹配 *_FEATURE 类
            feature_classes[attribute_name] = attribute  # 存储到字典中

# 将所有找到的 *_FEATURE 类添加到全局命名空间
globals().update(feature_classes)