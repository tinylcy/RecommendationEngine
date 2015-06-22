#!/usr/bin/python
# FileName sqoop.py
# -*- coding:UTF-8 -*-
import os
class SqoopUtil(object):
    '''
    sqoop operation
    '''
    def __init__(self):
        pass
    
    @staticmethod
    def execute_shell(shell):
        
        os.system(shell)
        
