#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Clean-up script to reset a Kafka Streams (v0.10.0.0) application for reprocessing from the very beginning
#
# resets topic offsets to zero
# deletes content of intermediate and result topics (user topics); via kafka-streams-cleanup.sh
# deletes internal topics completely
# clears local state store directory
#

# setup instructions:
#
# install librdkafka (required for Kafka Python clients):
#
# git clone https://github.com/edenhill/librdkafka
# cd librdkafka
# ./configure
# make
# sudo make install
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
#
# install Kafka Python clients:
#
# sudo pip install confluent-kafka

from confluent_kafka import Consumer
import sys
import subprocess
import shutil

def print_usage (appname):
    sys.stderr.write(('Usage: %s <bootstrap-brokers> <zookeeper>'
                     + ' <application-id> <source-topics>'
                     + ' [--sinks <intermediate-and-sink-topics>] [--operators <names>] [--windows <names>]'
                     + ' [<state-dir>]\n') % appname)
    sys.exit(1)

if __name__ == '__main__':
    if len(sys.argv) < 5:
        sys.stderr.write('Incomplete argument list.\n')
        print_usage(sys.argv[0])

    idx = 1;
    broker = sys.argv[idx]
    idx += 1
    zookeeper = sys.argv[idx]
    idx += 1
    group  = sys.argv[idx] # in Kafka-Stream: application-id == group
    idx += 1
    source_topics = sys.argv[idx].split(',')

    state_dir = '/tmp/kafka-streams'

    int_sink_topics = []
    operators = []
    windows = []

    idx += 1
    while idx < len(sys.argv) and idx < 12:
        if sys.argv[idx] == '--sinks':
            idx += 1
            int_sink_topics = sys.argv[idx].split(',')
        elif sys.argv[idx] == '--operators':
            idx += 1
            operators = sys.argv[idx].split(',')
        elif sys.argv[idx] == '--windows':
            idx += 1
            windows = sys.argv[idx].split(',')
        elif idx == len(sys.argv)-1:
            state_dir = sys.argv[idx]
        else:
            sys.stderr.write('Unknown argument: %s\n' % sys.argv[idx])
            print_usage(sys.argv[0])
        idx += 1

    changelog_topics = []
    for o in operators:
        changelog_topics.append(group + '-' + o + '-changelog')
    for w in windows:
        changelog_topics.append(group + '-' + o + '-changelog')

    all_topics = source_topics + int_sink_topics + changelog_topics

    state_dir = state_dir + '/' + group



    print '>>> Resetting offsets to zero for topics:', all_topics

    # Consumer configuration
    conf = {'bootstrap.servers': broker, 'group.id': group, 'enable.auto.commit': False}

    c = Consumer(**conf)

    topic_partitions = []
    def get_assignment (consumer, partitions):
        global topic_partitions
        topic_partitions = partitions

    # subscribe to topics
    c.subscribe(all_topics, on_assign=get_assignment)

    # poll to get assignment
    c.poll(timeout=1.0)

    # set all offsets to zero
    for partition in topic_partitions:
        partition.offset = 0

    # commit all offsets
    c.commit(offsets=topic_partitions)

    # close consumer to commit offsets
    c.close()
    print '>>> Done.'



    print '>>> Clearing intermediate and sink topics:', int_sink_topics
    cmd = './kafka-streams-cleanup.sh ' + zookeeper
    for topic in int_sink_topics:
        cmd = cmd + ' ' + topic
    print(cmd)
    subprocess.call(cmd, shell=True)
    print '>>> Done.'



    print '>>> Deleting internal changlelog topics:', changelog_topics
    for topic in changelog_topics:
        cmd = './bin/kafka-topics.sh --zookeeper ' + zookeeper + ' --delete --topic ' + topic
        print(cmd)
        subprocess.call(cmd, shell=True)
    print '>>> Done.'



    print '>>> Removing local state stores from', state_dir
    shutil.rmtree(state_dir)
    print '>>> Done.'



    print 'Clean-up finished.'

