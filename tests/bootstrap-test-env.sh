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

# This script automates the process of setting up a local machine for running Kafka system tests
export GREP_OPTIONS='--color=never'

# Helper function which prints version numbers so they can be compared lexically or numerically
function version { echo "$@" | awk -F. '{ printf("%03d%03d%03d%03d\n", $1,$2,$3,$4); }'; }

base_dir=`dirname $0`/..
cd $base_dir

echo "Checking Virtual Box installation..."
bad_vb=false
if [ -z `vboxmanage --version` ]; then
    echo "It appears that Virtual Box is not installed. Please install and try again (see https://www.virtualbox.org/ for details)"
    bad_vb=true   
else
    echo "Virtual Box looks good."
fi

echo "Checking Vagrant installation..."
vagrant_version=`vagrant --version | egrep -o "[0-9]+\.[0-9]+\.[0-9]+"`
bad_vagrant=false
if [ "$(version $vagrant_version)" -lt "$(version 1.6.4)" ]; then
    echo "Found Vagrant version $vagrant_version. Please upgrade to 1.6.4 or higher (see https://www.vagrantup.com for details)"
    bad_vagrant=true
else
    echo "Vagrant installation looks good."
fi

if [ "x$bad_vagrant" == "xtrue" -o "x$bad_vb" == "xtrue" ]; then
    exit 1
fi

echo "Checking for necessary Vagrant plugins..."
hostmanager_version=`vagrant plugin list | grep vagrant-hostmanager | egrep -o "[0-9]+\.[0-9]+\.[0-9]+"`
if [ -z "$hostmanager_version"  ]; then
    vagrant plugin install vagrant-hostmanager
fi

echo "Creating and packaging a reusable base box for Vagrant..."
vagrant/package-base-box.sh

# Set up Vagrantfile.local if necessary
if [ ! -e Vagrantfile.local ]; then
    echo "Creating Vagrantfile.local..."
    cp vagrant/system-test-Vagrantfile.local Vagrantfile.local
else
    echo "Found an existing Vagrantfile.local. Keeping without overwriting..."
fi

# Sanity check contents of Vagrantfile.local
echo "Checking Vagrantfile.local..."
vagrantfile_ok=true
num_brokers=`egrep -o "num_brokers\s*=\s*[0-9]+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
num_zookeepers=`egrep -o "num_zookeepers\s*=\s*[0-9]+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
num_workers=`egrep -o "num_workers\s*=\s*[0-9]+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
if [ "x$num_brokers" == "x" -o "$num_brokers" != 0 ]; then
    echo "Vagrantfile.local: bad num_brokers. Update to: num_brokers = 0"
    vagrantfile_ok=false
fi
if [ "x$num_zookeepers" == "x" -o "$num_zookeepers" != 0 ]; then
    echo "Vagrantfile.local: bad num_zookeepers. Update to: num_zookeepers = 0"
    vagrantfile_ok=false
fi
if [ "x$num_workers" == "x" -o "$num_workers" == 0 ]; then
    echo "Vagrantfile.local: bad num_workers (size of test cluster). Set num_workers high enough to run your tests."
    vagrantfile_ok=false
fi

if [ "$vagrantfile_ok" == "true" ]; then
    echo "Vagrantfile.local looks good."
fi
