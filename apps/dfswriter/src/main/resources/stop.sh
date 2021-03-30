#!/bin/bash

base_dir=`dirname $0`
PID_FILE=${base_dir}/.dfswriter.pid;

if [ -f $PID_FILE ];
then
        old_pid=`cat $PID_FILE`;
        pids=`ps aux | grep java | grep dfswriter | awk '{print $2;}'`;
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


