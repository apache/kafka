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

base_dir=$(dirname $0)/..
rat_excludes_file=$base_dir/.rat-excludes

if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

rat_command="$JAVA -jar $base_dir/lib_managed/scala_2.8.0/compile/apache-rat-0.8.jar --dir $base_dir "

for f in $(cat $rat_excludes_file);
do
  rat_command="${rat_command} -e $f"  
done

echo "Running " $rat_command
$rat_command > $base_dir/rat.out

