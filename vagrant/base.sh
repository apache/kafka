#!/usr/bin/env bash
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

set -ex

# The version of Kibosh to use for testing.
# If you update this, also update tests/docker/Dockerfile
export KIBOSH_VERSION=8841dd392e6fbf02986e2fb1f1ebf04df344b65a

path_to_jdk_cache() {
  jdk_version=$1
  echo "/tmp/jdk-${jdk_version}.tar.gz"
}

fetch_jdk_tgz() {
  jdk_version=$1

  path=$(path_to_jdk_cache $jdk_version)

  if [ ! -e $path ]; then
    mkdir -p $(dirname $path)
    curl --retry 5 -s -L "https://s3-us-west-2.amazonaws.com/kafka-packages/jdk-${jdk_version}.tar.gz" -o $path
  fi
}

JDK_MAJOR="${JDK_MAJOR:-8}"
JDK_FULL="${JDK_FULL:-8u202-linux-x64}"

if [ -z `which javac` ]; then
    apt-get -y update
    apt-get install -y software-properties-common python-software-properties binutils java-common

    echo "===> Installing JDK..." 

    mkdir -p /opt/jdk
    cd /opt/jdk
    rm -rf $JDK_MAJOR
    mkdir -p $JDK_MAJOR
    cd $JDK_MAJOR
    fetch_jdk_tgz $JDK_FULL
    tar x --strip-components=1 -zf $(path_to_jdk_cache $JDK_FULL)
    for bin in /opt/jdk/$JDK_MAJOR/bin/* ; do 
      name=$(basename $bin)
      update-alternatives --install /usr/bin/$name $name $bin 1081 && update-alternatives --set $name $bin
    done
    echo -e "export JAVA_HOME=/opt/jdk/$JDK_MAJOR\nexport PATH=\$PATH:\$JAVA_HOME/bin" > /etc/profile.d/jdk.sh
    echo "JDK installed: $(javac -version 2>&1)"

fi

chmod a+rw /opt
if [ -h /opt/kafka-dev ]; then
    # reset symlink
    rm /opt/kafka-dev
fi
ln -s /vagrant /opt/kafka-dev


get_kafka() {
    version=$1
    scala_version=$2

    kafka_dir=/opt/kafka-$version
    url=https://s3-us-west-2.amazonaws.com/kafka-packages/kafka_$scala_version-$version.tgz
    # the .tgz above does not include the streams test jar hence we need to get it separately
    url_streams_test=https://s3-us-west-2.amazonaws.com/kafka-packages/kafka-streams-$version-test.jar
    if [ ! -d /opt/kafka-$version ]; then
        pushd /tmp
        curl --retry 5 -O $url
        curl --retry 5 -O $url_streams_test || true
        file_tgz=`basename $url`
        file_streams_jar=`basename $url_streams_test` || true
        tar -xzf $file_tgz
        rm -rf $file_tgz

        file=`basename $file_tgz .tgz`
        mv $file $kafka_dir
        mv $file_streams_jar $kafka_dir/libs || true
        popd
    fi
}

# Install Kibosh
apt-get update -y && apt-get install -y git cmake pkg-config libfuse-dev
pushd /opt
rm -rf /opt/kibosh
git clone -q  https://github.com/confluentinc/kibosh.git
pushd "/opt/kibosh"
git reset --hard $KIBOSH_VERSION
mkdir "/opt/kibosh/build"
pushd "/opt/kibosh/build"
../configure && make -j 2
popd
popd
popd

# Install iperf
apt-get install -y iperf traceroute

# Test multiple Kafka versions
# We want to use the latest Scala version per Kafka version
# Previously we could not pull in Scala 2.12 builds, because Scala 2.12 requires Java 8 and we were running the system
# tests with Java 7. We have since switched to Java 8, so 2.0.0 and later use Scala 2.12.
get_kafka 0.8.2.2 2.11
chmod a+rw /opt/kafka-0.8.2.2
get_kafka 0.9.0.1 2.11
chmod a+rw /opt/kafka-0.9.0.1
get_kafka 0.10.0.1 2.11
chmod a+rw /opt/kafka-0.10.0.1
get_kafka 0.10.1.1 2.11
chmod a+rw /opt/kafka-0.10.1.1
get_kafka 0.10.2.2 2.11
chmod a+rw /opt/kafka-0.10.2.2
get_kafka 0.11.0.3 2.11
chmod a+rw /opt/kafka-0.11.0.3
get_kafka 1.0.2 2.11
chmod a+rw /opt/kafka-1.0.2
get_kafka 1.1.1 2.11
chmod a+rw /opt/kafka-1.1.1
get_kafka 2.0.1 2.12
chmod a+rw /opt/kafka-2.0.1
get_kafka 2.1.1 2.12
chmod a+rw /opt/kafka-2.1.1
get_kafka 2.2.2 2.12
chmod a+rw /opt/kafka-2.2.2
get_kafka 2.3.1 2.12
chmod a+rw /opt/kafka-2.3.1
get_kafka 2.4.1 2.12
chmod a+rw /opt/kafka-2.4.1
get_kafka 2.5.1 2.12
chmod a+rw /opt/kafka-2.5.1
get_kafka 2.6.2 2.12
chmod a+rw /opt/kafka-2.6.2
get_kafka 2.7.1 2.12
chmod a+rw /opt/kafka-2.7.1
get_kafka 2.8.1 2.12
chmod a+rw /opt/kafka-2.8.1
get_kafka 3.0.1 2.12
chmod a+rw /opt/kafka-3.0.1
get_kafka 3.1.0 2.12
chmod a+rw /opt/kafka-3.1.0

# For EC2 nodes, we want to use /mnt, which should have the local disk. On local
# VMs, we can just create it if it doesn't exist and use it like we'd use
# /tmp. Eventually, we'd like to also support more directories, e.g. when EC2
# instances have multiple local disks.
if [ ! -e /mnt ]; then
    mkdir /mnt
fi
chmod a+rwx /mnt

# Run ntpdate once to sync to ntp servers
# use -u option to avoid port collision in case ntp daemon is already running
ntpdate -u pool.ntp.org
# Install ntp daemon - it will automatically start on boot
apt-get -y install ntp

# Increase the ulimit
mkdir -p /etc/security/limits.d
echo "* soft nofile 128000" >> /etc/security/limits.d/nofile.conf
echo "* hard nofile 128000" >> /etc/security/limits.d/nofile.conf

ulimit -Hn 128000
ulimit -Sn 128000
