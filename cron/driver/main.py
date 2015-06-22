# -*- coding:UTF-8 -*-
#!/usr/bin/python
# FileName:main.py

from com.cal.sqoop_export import sqoop_export
from com.cal.sqoop_import import sqoop_import
from com.utls.pro_env import PROJECT_LIB_DIR, PROJECT_DIR
from com.utls.sqoop import SqoopUtil
import os
import time

# 一共分为5个步骤：
# 1.将数据库中新产生的数据导入HDFS
# 2.把刚才导入到HDFS中的数据拷贝到相应的位置
# 3.推荐引擎进行新一轮的计算
# 4.把数据库中原来存储的推荐结果表清空
# 5.从HDFS导入新计算出来的结果


def execute():
    
    # 调度模块将昨天的时间传入
    now = time.time()
    n = 1
    before = now - n * 24 * 3600  # 可以改变n 的值计算n天前的
    dt = time.strftime("%Y-%m-%d", time.localtime(before))
    
    #####Step1#####
    
    # 解析配置文件，获得sqoop命令集合
    cmds = sqoop_import.resolve_conf(dt)
    # 迭代集合，执行命令
    for i in range(len(cmds)):
        cmd = cmds[i]
        
        # 执行导入过程
        print cmd
        SqoopUtil.execute_shell(cmd)

    #####Step2#####
    
    shell = "hadoop fs -cp /user/hadoop/" + dt + "/part-m-00000 /user/hadoop/Recommend/InputData/" + dt
    print shell
    SqoopUtil.execute_shell(shell)

    #####Step3#####

    shell = "hadoop jar " + PROJECT_LIB_DIR + "/ItemBasedRecommend.jar recommend.Recommend"
    print shell
    SqoopUtil.execute_shell(shell)
  
    #####Step4#####
    
    # 执行脚本：清空表格
    os.system(PROJECT_DIR + '/lib/export.sh')

    #####Step5#####

    # 迭代集合，执行命令
    cmds = sqoop_export.resolve_conf()
    for i in range(len(cmds)):
        cmd = cmds[i]
        
        # 执行导出过程
        # print cmd
        SqoopUtil.execute_shell(cmd)
    

# Python模块的入口：main函数
if __name__ == '__main__':
    
    while True:
        execute()
        time.sleep(24*60*60)
    
        
        
    
