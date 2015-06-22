# -*- coding:UTF-8 -*-
#!/usr/bin/python
# FileName:sqoop_export.py
from com.utls.pro_env import PROJECT_CONF_DIR
import xml.etree.ElementTree as ET

class sqoop_export(object):
    
    def __init__(self):
        pass

    @staticmethod
    def resolve_conf():
    
        # 获得配置文件名
        conf_file = PROJECT_CONF_DIR + "Export.xml"
    
        # 解析配置文件
        xml_tree = ET.parse(conf_file)
    
        # 获得task元素
        tasks = xml_tree.findall('./task')
    
        for task in tasks:
            # 获得表名集合
            tables = task.findall('./table')
        
            # 用来保存待执行的Sqoop命令集合
            cmds = []
        
            # 迭代表名集合，解析表配置文件
            for i in range(len(tables)):
                # 表名
                table_name = tables[i].text
                # 表配置文件名
                table_conf_file = PROJECT_CONF_DIR + table_name + ".xml"
            
                # 解析表配置文件
                xmlTree = ET.parse(table_conf_file)
            
                # 获取sqoop-shell节点
                sqoopNodes = xmlTree.findall("./sqoop-shell")
            
                # 获取sqoop命令类型
                sqoop_cmd_type = sqoopNodes[0].attrib["type"]
                # 获取
                praNodes = sqoopNodes[0].findall("./param")
            
                # 用来保存param的信息的字典
                cmap = {}
            
                for i in range(len(praNodes)):
                    # 获得key属性的值
                    key = praNodes[i].attrib["key"]
                    # 获取param标签中间的值
                    value = praNodes[i].text
                    # 保存到字典中
                    cmap[key] = value
                
                # 首先组装成sqoop命令头
                command = "sqoop " + sqoop_cmd_type
            
                # #迭代字典将param的信息拼接成字符串
                for key in cmap.keys():
                    value = cmap[key]
                
                    # 如果不是键值对形式的命令选项
                    if(value == None or value == "" or value == " "):
                        value = ""
                
                    # 拼装成命令
                    if key == "fields-terminated-by":
                        command += " --" + key + " " + value
                    else:
                        command += " --" + key + " " + value + "\\" + "\n"
            
                cmds.append(command)
    
            return cmds
    
