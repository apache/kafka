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

# We need libtool for ./configure && make && make install stage
command -v libtool
if  [ $? -ne 0 ]; then
    echo "autoconf.sh: error: unable to locate libtool"
    exit 1
fi

# We need autoreconf to build the ./configure script
command -v autoreconf
if  [ $? -ne 0 ]; then
    echo "autoconf.sh: error: unable to locate autoreconf"
    exit 1
fi

mkdir -p ./build-aux/m4
autoreconf --verbose --force --install

