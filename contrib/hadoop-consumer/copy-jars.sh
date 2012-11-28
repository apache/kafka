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

# include kafka jars
for file in $base_dir/contrib/hadoop-consumer/target/scala_2.8.0/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

# include kafka jars
echo "$hadoop fs -put $base_dir/core/target/scala_2.8.0/kafka-*.jar; $1/"
$hadoop fs -put $base_dir/core/target/scala_2.8.0/kafka-*.jar $1/ 

# include core lib jars
for file in $base_dir/core/lib/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

for file in $base_dir/core/lib_managed/scala_2.8.0/compile/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

# include scala library jar
echo "$hadoop fs -put $base_dir/project/boot/scala-2.8.0/lib/scala-library.jar; $1/"
$hadoop fs -put $base_dir/project/boot/scala-2.8.0/lib/scala-library.jar $1/

local_dir=$(dirname $0)

# include hadoop-consumer jars
for file in $local_dir/lib/*.jar;
do
   echo "$hadoop fs -put $file $1/"
   $hadoop fs -put $file $1/ 
done

