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

# This script can be used to set up a driver machine on aws from which you will run tests
# or bring up your mini Kafka cluster.

# Install dependencies
sudo apt-get install -y \
  maven \
  openjdk-8-jdk-headless \
  build-essential \
  ruby-dev \
  zlib1g-dev \
  realpath \
  python-setuptools

base_dir=`dirname $0`/../..

if [ -z `which vagrant` ]; then
    echo "Installing vagrant..."
    wget https://releases.hashicorp.com/vagrant/1.9.3/vagrant_1.9.3_x86_64.deb
    sudo dpkg -i vagrant_1.9.3_x86_64.deb
    rm -f vagrant_1.9.3_x86_64.deb
fi

# Install necessary vagrant plugins
# Note: Do NOT install vagrant-cachier since it doesn't work on AWS and only
# adds log noise
vagrant_plugins="vagrant-aws vagrant-hostmanager"
existing=`vagrant plugin list`
for plugin in $vagrant_plugins; do
    echo $existing | grep $plugin > /dev/null
    if [ $? != 0 ]; then
        vagrant plugin install $plugin
    fi
done

# Create Vagrantfile.local as a convenience
if [ ! -e "$base_dir/Vagrantfile.local" ]; then
    cp $base_dir/vagrant/aws/aws-example-Vagrantfile.local $base_dir/Vagrantfile.local
fi

gradle="gradle-2.2.1"
if [ -z `which gradle` ] && [ ! -d $base_dir/$gradle ]; then
    if [ ! -e $gradle-bin.zip ]; then
        wget https://services.gradle.org/distributions/$gradle-bin.zip
    fi
    unzip $gradle-bin.zip
    rm -rf $gradle-bin.zip
    mv $gradle $base_dir/$gradle
fi

# Ensure aws access keys are in the environment when we use a EC2 driver machine
LOCAL_HOSTNAME=$(hostname -d)
if [[ ${LOCAL_HOSTNAME} =~ .*\.compute\.internal ]]; then
  grep "AWS ACCESS KEYS" ~/.bashrc > /dev/null
  if [ $? != 0 ]; then
    echo "# --- AWS ACCESS KEYS ---" >> ~/.bashrc
    echo ". `realpath $base_dir/aws/aws-access-keys-commands`" >> ~/.bashrc
    echo "# -----------------------" >> ~/.bashrc
    source ~/.bashrc
  fi
fi
