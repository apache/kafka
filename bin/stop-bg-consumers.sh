#!/bin/bash
all="java -Xmx512M -server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.head";
if [ -z $1 ]; then
  echo "kill all consumers"
  pids=$(ps -aux | grep "$all" | awk {'print $2'})
else
  echo "kill $1 consumers"
  pids=$(ps -S | grep "$1" | awk {'print $1'})
fi
terminate() {
  echo $1
  kill -TERM $1 ;
}

for pid in $pids; do
  terminate $pid;
done;
exit 0;