#!/bin/bash

# This script should be run once on your aws test driver machine before
# attempting to run any ducktape tests

# Install dependencies
sudo apt-get install -y maven openjdk-6-jdk build-essential \
            ruby-dev zlib1g-dev realpath python-setuptools

base_dir=`dirname $0`/..

if [ -z `which vagrant` ]; then
    echo "Installing vagrant..."
    wget https://dl.bintray.com/mitchellh/vagrant/vagrant_1.7.2_x86_64.deb
    sudo dpkg -i vagrant_1.7.2_x86_64.deb
    rm -f vagrant_1.7.2_x86_64.deb
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
    cp $base_dir/aws/aws-example-Vagrantfile.local $base_dir/Vagrantfile.local
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

