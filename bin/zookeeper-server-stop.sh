#!/bin/sh
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

# Which jps to use
if [ -z "$JAVA_HOME" ]; then
  JPS="$(which jps)"
else
  JPS="$JAVA_HOME/bin/jps"
fi

if [ -x "$JPS" ]; then
  PIDS=$(${JPS} -vl | grep -i QuorumPeerMain | awk '{print $1}')
else
  PIDS=$(ps ax | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}')
fi

if [ -z "$PIDS" ]; then
  echo "No zookeeper server to stop"
  exit 1
else
  kill -s TERM $PIDS
fi

