#!/bin/bash

base_dir=`dirname $0`

if [ $# -ge 1 ];
then
        CONF_PATH=$1;
else
        echo "input conf_path as paramter";
        exit -1;
fi


old_dir=`pwd`
abs_path=$(cd "$CONF_PATH"; pwd)
cd ${old_dir}

finger_print=`echo $abs_path | md5sum | awk '{print $1}'`

PID_FILE=${base_dir}/.${finger_print}.pid;

if [ -f $PID_FILE ];
then
        old_pid=`cat $PID_FILE`;
        pids=`ps aux | grep java | grep savefile | awk '{print $2;}'`;
        for pid in $pids
        do
                if [ $pid -eq $old_pid ];
                then
                        echo "process is running as $pid, now stop it.";
                        ret=`kill $old_pid`
                        exit $ret
                fi
        done
fi


