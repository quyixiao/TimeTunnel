#!/bin/bash


base_dir=`dirname $0`
PID_FILE=${base_dir}/.dfswriter.pid;

LIB_PATH=${base_dir}/../lib
CONF_PATH=${base_dir}/../conf


CLASSPATH=$LIB_PATH/*:$CONF_PATH

script="java -classpath $CLASSPATH  com.taobao.timetunnel.dfswriter.app.HDFSWriterApp"
echo $script > /tmp/timetunnel.dfswriter.start
nohup $script >> /tmp/timetunnel.dfswriter.start
pid=$!
echo ${pid}>${PID_FILE}
