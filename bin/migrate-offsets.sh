#!/bin/bash
# If the topic does not exist on the target cluster: topics will be created automaticaly
# If the topic exist on the target cluster, any prior consumer will be killed
BROKER_A=$1
BROKER_B=$2
CGROUP_A=$3
CGROUP_B=$4
TOPIC=$5
PART_SUM=0;
tmp=tmp.tcc
SEPARATOR="-"
describe_origin() {
  let key=null;
  ./kafka-consumer-groups.sh \
  --bootstrap-server $BROKER_A \
  --group $CGROUP_A \
  --describe | \
  grep "$CGROUP_A $TOPIC" | \
  awk {' print $3"-"$4'} > $tmp;
  PART_SUM=$(wc $tmp | awk {' print $1 '})
}

copy_offset(){
  offset=${1/-/" "};
  offset_array=($offset);
  ./kafka-consumer-groups.sh \
  --bootstrap-server $BROKER_B \
  --reset-offsets \
  --group $CGROUP_B \
  --topic $TOPIC:${offset_array[0]} \
  --to-offset ${offset_array[1]} \
  --execute
}

stop_consumers() {
  consumers=$(./kafka-consumer-groups.sh \
  --bootstrap-server $BROKER_B \
  --group $CGROUP_B \
  --describe \
  --members | \
  awk {' print $2 '});
  consumers=${consumers/"CONSUMER-ID"/""}
  echo "consumers to kill : \n"$consumers;
  nbligne=$(echo $consumers | wc -l)
  if [ $nbligne -gt 0]; then
    ./stop-bg-consumers.sh $CGROUP_B
  fi
}

start_process() {
  stop_consumers;
  if [ $PART_SUM > 0 ]; then
    c="0";
    echo "processing $PART_SUM partitions";
      cat $tmp | while read value; do
      ((++c));
      copy_offset $value
      done;
  else
    echo "source topic has $PART_SUM partitions";
    exit
  fi
}

describe_origin;
start_process;
exit 0;