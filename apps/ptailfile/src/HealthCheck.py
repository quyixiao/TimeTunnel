#! /usr/bin/env python
#encoding=utf-8
'''
Created on 2010-11-26

@author: jiugao
'''
import os
import ConfigParser
import traceback
import sys
from string import atol
from ptail.Meta import MetaInfo
import time
from threading import Thread
from ptail.Config import Config

SEP = "_"

class LogConfig(object):
    def __init__(self, path):
        self.config = ConfigParser.ConfigParser()
        try:
            fd = open(path)
        except:
            print >> sys.stderr, "can not locate log conf path: " + path
            sys.exit(1)
        try:
            self.config.readfp(fd)
        except IOError:
            print >> sys.stderr, 'read log conf error ' + str(traceback.format_exc())
            sys.exit(1)
        finally:
            fd.close()
    def getLogPath(self):
        value = self.config.get('handler_fileHandler', 'args')
        if value is None or value is "":
            print >> sys.stderr, "no log name defined in log conf"
            sys.exit(1)
        return eval(value)[0]

basepath = os.path.dirname(__file__)
log_config = LogConfig(basepath + "/../conf/log.conf")
conf = Config(basepath + '/../conf/tailfile.conf')
       
class HealthCheck(object):
    def __init__(self, log_path, cp_path, cpname):
        self.log_path = log_path
        self.dir = cp_path
        self.name = cpname
        pass
    
    def __check_meta(self):
        print >> sys.stderr, "{COLLECT CHECK INFO BEGIN}"
        f = None
        while True:
            index = self.__max_index()
            if index == -1L:
                return  "Last Processed File: null" + "\nCurrent Processed File: null At Position: 0"
            meta_name = self.__bulidname(index)
            print >> sys.stderr, "meta_name: " + meta_name
            try:
                f = open(meta_name, "r")
                break
            except:
                print >> sys.stderr, "cp file has been switched when open"
                continue
        check_info = self.__load_from_file(f)
        print >> sys.stderr, "{COLLECT CHECK INFO END}"
        return check_info
    
    def __bulidname(self, index):
        return self.dir + os.path.sep + self.name + "_" + str(index)
    
    def __load_from_file(self, f):
        times = 0;
        while True:
            try:
                meta = self.__setMeta(f)
                ret = "Last Processed File: " + meta.last_file + "\nCurrent Processed File: " + meta.cur_file + " At Position: " + str(meta.offset)
                return ret
            except Exception:
                print >> sys.stderr, "check latest file error, maybe due to write not finish, wait 1 sec"
                if times <3:
                    time.sleep(1)
                    times += 1
                    continue
                else:
                    print >> sys.stderr, "get check meta info error, maybe due to wrong format"
                    break
        f.close()
                    
    def __max_index(self):
        highest_index = -1L
        if os.path.exists(self.dir) is False:
            print >> sys.stderr, "cp path not exist: " + self.dir
            return highest_index
        files = os.listdir(self.dir)
        if files is None or len(files) == 0:
            print >> sys.stderr, "no cp file in cp path: " + self.dir
            return highest_index
        global SEP
        
        for f in files:
            if f.startswith(self.name) is False:
                continue
            rp = f.rpartition(SEP)
            if len(rp) == 0:
                print >> sys.stderr, "error filename format: " + f
                continue
            try:
                num = atol(rp[len(rp) - 1])
            except:
                print >> sys.stderr, "error filename format: " + f
                continue
            if num > highest_index:
                highest_index = num
        return highest_index
            
    def __setMeta(self, open_file):       
        line = None
        try:
            open_file.seek(0, os.SEEK_SET)
            line = open_file.readlines()
            print line
            meta = MetaInfo()
            meta.set(line)
            return meta
        except Exception:
            print >> sys.stderr, traceback.format_exc()
            raise Exception("load from file for meta failed")
    
    def check(self):
        check_info = self.__check_meta()
        print "[WORK LOG]"
        print check_info
#        log_info = self.__check_log()
#        print "[LOG INFO RESULT]"
#        if len(log_info) == 0:
#            print "NO FILE SWITCHED"
#        print log_info
    
    def __check_log(self):
        print >> sys.stderr, "{COLLECT LOG INFO BEGIN}"
        print >> sys.stderr, "log file: " + self.log_path
        f = open(self.log_path, "r")
        f.seek(0, 2)
        ret = []
        i = 0
        try:
            while i < 2:
                time.sleep(10)
                lines = f.readlines(10000)
                print "read len from log file: " + str(len(lines))
                for l in lines:
                    if l.startswith("CHECK_" + self.name) is False:
                        continue
                    else:
                        ret.append(l)
                if len(ret) == 0:
                    time.sleep(20)
                    i += 1
                    continue
                else:
                    break
        except Exception:
            print >> sys.stderr, traceback.format_exc()
            raise Exception("read log file failed")
        finally:
            f.close()
        print >> sys.stderr, "{COLLECT LOG INFO END}"
        return "".join(ret)
      
if __name__ == '__main__':
    topics = conf.get_topic_name().split(",")
    i = 0
    path = log_config.getLogPath()
#    print str(topics)
    print "####TT2 TAILFILE HEALTH CHECK LIST####"
    for t in topics:
        print "{TOPIC " + t + "}"
        base_path = conf.get_base_path().split(",")[i]
        print "[SOURCE FILE BASE PATH]"
        print base_path
        path_regx = conf.get_path_regx().split(",")[i]
        print "[PATH REGULAR EXPRESSION]"
        print path_regx
        cp_path = conf.get_cp_path()
        cp_name = conf.get_cp_name().split(",")[i]
        i += 1
        HealthCheck(path, cp_path, cp_name).check()
        print ""
    pass
