#!/bin/bash
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

function getKafkaPIDs {
	PIDS=`ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}'`
}

function checkKilled {
	echo "sent signal to $PIDS" 1>&2
	sleep 1
	getKafkaPIDs
}

getKafkaPIDs

if [ -n "$PIDS" ]; then
	echo $PIDS | xargs kill -SIGINT
	checkKilled
fi
if [ -n "$PIDS" ]; then
	echo "INTERRUPT SIGNAL FAILED; TRYING SIGTERM" 1>&2
	echo $PIDS | xargs kill -SIGTERM
	checkKilled
fi
if [ -n "$PIDS" ]; then
	echo "TERM SIGNAL FAILED; TRYING SIGKILL" 1>&2
	echo $PIDS | xargs kill -SIGKILL
	checkKilled
fi
if [ -n "$PIDS" ]; then
	echo "Failed to kill these processes: $PIDS" 1>&2
	exit 1
else
	echo "processes killed" 1>&2
fi
