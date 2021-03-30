#! /usr/bin/env python
#encoding=utf-8
'''
Created on 2010-10-28

@author: jiugao
'''
import logging
import os
import re
import time
from ptail import Config

logger = logging.getLogger("tailfile.dirscanner")
basepath = os.path.dirname(__file__)
conf = Config.Config(basepath + '/../../conf/tailfile.conf')

def on_error(oserror):
    logger.error("error during scan: " + str(oserror.filename))
    
class DirScanner(object):
    """directory scanner, width firstly, and filter older folder"""
    def __init__(self, root, tmp, modify_time, regx, q, history_files):
        self.root = root
        self.modify_time = modify_time
        self.path_regx = self.__split_regx(regx)
        logger.debug("path regx: " + str(self.path_regx))
        self.q = q
        self.tmp = tmp
        #trace already done files
        self.toDel = []
        self.to_del_modify_time = 0
        #for samoe mtime file use
        self.his_files = history_files
    
    def __split_regx(self, regx):
        ret = []
        count = regx.count("/")
        i = 0
        last_index = -1
        while i < count:
            index = regx.index("/", last_index + 1)
            ret.append(re.compile(regx[last_index + 1:index]))
            last_index = index
            i += 1
        ret.append(re.compile(regx[last_index + 1:]))
        return ret
    
    def __mtime(self, path):
        mtime = 0;
        try:
            mtime = os.path.getmtime(path)
        except:
            logger.error("file not exist: " + str(path))
        return mtime
             
    def __intern_scan(self):
        res = []
        max_depth = len(self.path_regx)
        depth = -1
        logger.debug("scan dir and last modify time: " + str(self.modify_time))
        if os.path.exists(self.root) == False:
            return
        parent_dir = None
        #recode last modify time
        old_modify_time = self.modify_time
        #recode max modify time during scan
        max_time = [self.modify_time]
#        print "1: "+str(max_time[0])
        #width first scan
        for path, subpaths, files in os.walk(self.root, True, on_error, False):
            inner_parent_dir = os.path.dirname(os.path.normpath(path)) 
            #increment search depth base on weather parent directory changes or not
            if inner_parent_dir != parent_dir:
                depth += 1
                parent_dir = inner_parent_dir
            
            if depth >= max_depth:
                    logger.info("search to the most depth: " + str(max_depth))
                    break
            regx = self.path_regx[depth]
            
            if depth < max_depth - 1:
                to_remove = []
                for subp in subpaths:
                    if regx.match(subp) is None:
                        logger.debug("remove dir " + str(subp))
                        to_remove.append(subp)
                        continue
                    mtime = self.__mtime(os.path.join(path, subp))
                    #change <= to <
                    if depth == max_depth - 2 and mtime < self.modify_time:
                        logger.debug("remove dir" + str(subp) + " due to not newer than " + str(mtime))
                        to_remove.append(subp)
                for i in to_remove:
                    subpaths.remove(i)
            else:
                for file in files:
                    full_path = os.path.join(path, file)
                    #change > to >=
                    mt = self.__mtime(full_path)
                    if regx.match(file) != None and mt >= self.modify_time:
                        logger.debug("file hit: " + full_path)
                        res.append(full_path)
#                        print "2-: "+str(max_time[0])
                        if mt > max_time[0]:
                            max_time[0] = mt
#                            print "2: "+str(max_time[0])
        if len(res) == 0:
            return
        self.modify_time = max_time[0]
#        print "3: "+str(self.modify_time)
        res = sorted(res, key=lambda f:self.__mtime(f))
        for file_res in res:
            try:
                self.q.index(file_res)
                logger.debug("file: " + file_res + " has been exist in q")
            except ValueError:
                try:
                    self.toDel.index(file_res)
                    logger.debug("file: " + file_res + " has been exist in todelete q")
                except ValueError:
                    if len(self.his_files) == 0:    
                        logger.debug("file: " + file_res + " has been added")
                        self.q.append(file_res)
                    else:
                        try:
                            self.his_files.index(file_res)
                            logger.debug("file: " + file_res + " has been exist in history files")
                        except ValueError:
                            logger.debug("file: " + file_res + " has been added")
                            self.q.append(file_res)
        if self.modify_time > old_modify_time:
            self.his_files = []
        return
    
    #not thread-safe
    def addToDel(self, filename):
        mtime = self.__mtime(filename)
        if mtime > self.to_del_modify_time:
            self.toDel = []
            self.to_del_modify_time = mtime    
        self.toDel.append(filename)
        return self.toDel
    
    #not thread-safe 
    def scan(self, force=False):
        if force is False and len(self.q) > 3:
            return
        self.__intern_scan()
        if self.tmp != "null":
            try:
                self.q.index(self.tmp)
            except ValueError:
                self.q.append(self.tmp)
                return
            self.q.remove(self.tmp)
            self.q.append(self.tmp)
        return
        
if __name__ == '__main__':
#    dir_scanner = DirScanner("D:\\workspace\\ptailfile\\test", "null", 0, "d2/\d/t1\\\\.*", ["1", "2", 'D:\\workspace\\ptailfile\\test\\d2\\d2f.txt'], "null")
    _path_regx = conf.get_path_regx()
    _base_path = conf.get_base_path()
    print _path_regx
    print _base_path
    dir_scanner = DirScanner(_base_path, "null", 0, _path_regx, ["1", "2", 'D:\\workspace\\ptailfile\\test\\d2\\d2f.txt'], "null")
#    print os.path.getmtime('D:\\workspace\\ptailfile\\test\\d2\\b.txt')
#    print os.path.getmtime('D:\\workspace\\ptailfile\\test\\d2')
    
    while True:
        dir_scanner.scan(True)
        print dir_scanner.q
        
        time.sleep(10)
    
    
