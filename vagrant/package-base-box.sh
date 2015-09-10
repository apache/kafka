#!/usr/bin/env bash

# This script automates the process of creating and packaging 
# a new vagrant base_box. For use locally (not aws).

base_dir=`dirname $0`/..
cd $base_dir

backup_vagrantfile=backup_Vagrantfile.local
local_vagrantfile=Vagrantfile.local

# Restore original Vagrantfile.local, if it exists
function revert_vagrantfile {
    rm -f $local_vagrantfile
    if [ -e $backup_vagrantfile ]; then
        mv $backup_vagrantfile $local_vagrantfile
    fi
}

function clean_up {
    echo "Cleaning up..."
    vagrant destroy -f
    rm -f package.box
    revert_vagrantfile
}

# Name of the new base box
base_box="kafkatest-worker"

# vagrant VM name
worker_name="worker1"

echo "Destroying vagrant machines..."
vagrant destroy -f

echo "Removing $base_box from vagrant..."
vagrant box remove $base_box

echo "Bringing up a single vagrant machine from scratch..."
if [ -e $local_vagrantfile ]; then
    mv $local_vagrantfile $backup_vagrantfile
fi
echo "num_workers = 1" > $local_vagrantfile
echo "num_brokers = 0" >> $local_vagrantfile
echo "num_zookeepers = 0" >> $local_vagrantfile
vagrant up
up_status=$?
if [ $up_status != 0 ]; then
    echo "Failed to bring up a template vm, please try running again."
    clean_up
    exit $up_status
fi

echo "Packaging $worker_name..."
vagrant package $worker_name

echo "Adding new base box $base_box to vagrant..."
vagrant box add $base_box package.box

clean_up

