#!/bin/bash

num_messages=400000
message_size=400

base_dir=$(dirname $0)/..

rm -rf /tmp/zookeeper_source
rm -rf /tmp/zookeeper_target
rm -rf /tmp/kafka-source1-logs
mkdir /tmp/kafka-source1-logs
mkdir /tmp/kafka-source1-logs/test01-0
touch /tmp/kafka-source1-logs/test01-0/00000000000000000000.kafka
rm -rf /tmp/kafka-source2-logs
mkdir /tmp/kafka-source2-logs
mkdir /tmp/kafka-source2-logs/test01-0
touch /tmp/kafka-source2-logs/test01-0/00000000000000000000.kafka
rm -rf /tmp/kafka-source3-logs
mkdir /tmp/kafka-source3-logs
mkdir /tmp/kafka-source3-logs/test01-0
touch /tmp/kafka-source3-logs/test01-0/00000000000000000000.kafka
rm -rf /tmp/kafka-target1-logs
rm -rf /tmp/kafka-target2-logs

echo "start the servers ..."
$base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_source.properties 2>&1 > $base_dir/zookeeper_source.log &
$base_dir/../../bin/zookeeper-server-start.sh $base_dir/config/zookeeper_target.properties 2>&1 > $base_dir/zookeeper_target.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source1.properties 2>&1 > $base_dir/kafka_source1.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source2.properties 2>&1 > $base_dir/kafka_source2.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_source3.properties 2>&1 > $base_dir/kafka_source3.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target1.properties $base_dir/config/consumer.properties 2>&1 > $base_dir/kafka_target1.log &
$base_dir/../../bin/kafka-run-class.sh kafka.Kafka $base_dir/config/server_target2.properties $base_dir/config/consumer.properties 2>&1 > $base_dir/kafka_target2.log &

sleep 4
echo "start producing messages ..."
$base_dir/../../bin/kafka-run-class.sh kafka.tools.ProducerPerformance --brokerinfo zk.connect=localhost:2181 --topic test01 --messages $num_messages --message-size $message_size --batch-size 200 --vary-message-size --threads 1 --reporting-interval 400000 num_messages --async --delay-btw-batch-ms 10 &

echo "wait for consumer to finish consuming ..."
cur1_offset="-1"
cur2_offset="-1"
quit1=0
quit2=0
while [ $quit1 -eq 0 ] && [ $quit2 -eq 0 ]
do
  sleep 2
  target1_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
  if [ $target1_size -eq $cur1_offset ]
  then
    quit1=1
  fi
  cur1_offset=$target1_size
  target2_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
  if [ $target2_size -eq $cur2_offset ]
  then
    quit2=1
  fi
  cur2_offset=$target2_size
done

sleep 2
source_part0_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9092 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
source_part1_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9091 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
source_part2_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9090 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
target_part0_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9093 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`
target_part1_size=`$base_dir/../../bin/kafka-run-class.sh kafka.tools.GetOffsetShell --server kafka://localhost:9094 --topic test01 --partition 0 --time -1 --offsets 1 | tail -1`

expected_size=`expr $source_part0_size + $source_part1_size + $source_part2_size`
actual_size=`expr $target_part0_size + $target_part1_size`
if [ $expected_size != $actual_size ]
then
   echo "source size: $expected_size target size: $actual_size test failed!!! look at it!!!"
else
   echo "test passed"
fi

echo "stopping the servers"
ps ax | grep -i 'kafka.kafka' | grep -v grep | awk '{print $1}' | xargs kill -15 2>&1 > /dev/null
sleep 2
ps ax | grep -i 'QuorumPeerMain' | grep -v grep | awk '{print $1}' | xargs kill -15 2>&1 > /dev/null
