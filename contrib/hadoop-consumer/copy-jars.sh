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

if [ $# -lt 1 ];
then
  echo "USAGE: $0 dir"
  exit 1
fi

base_dir=$(dirname $0)/../..

hadoop=${HADOOP_HOME}/bin/hadoop

echo "$hadoop fs -rmr $1"
$hadoop fs -rmr $1

echo "$hadoop fs -mkdir $1"
$hadoop fs -mkdir $1

local_dir=$(dirname $0)

# include hadoop-consumer jars
for file in $local_dir/lib/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

