echo $abs_path

#!/bin/bash

base_dir=`dirname $0`
LIB_PATH=${base_dir}/../lib

if [ $# -ge 1 ];
then
        CONF_PATH=$1;
else
        CONF_PATH=${base_dir}/../conf
fi

CLASSPATH=$LIB_PATH/*:$CONF_PATH

old_dir=`pwd`
abs_path=$(cd "$CONF_PATH"; pwd)
cd ${old_dir}

finger_print=`echo $abs_path | md5sum | awk '{print $1}'`

PID_FILE=${base_dir}/.${finger_print}.pid;

script="java -Xmx2g -Xms2g -DROUTER=localhost:9999 -DRPCTIMEOUT=30000 -classpath $CLASSPATH  com.taobao.timetunnel.savefile.app.SaveFileApp"
echo $script > /tmp/timetunnel.savefile.start
nohup $script >> /tmp/timetunnel.savefile.start &
pid=$!
echo ${pid}>${PID_FILE}
