#!/usr/bin/env bash

# This script automates the process of creating and packaging 
# a new vagrant base_box. For use locally (not aws).

# Name of the new base box
base_box="kafkatest-worker"

# vagrant VM name
worker_name="worker1"

base_dir=`dirname $0`/..
cd $base_dir

echo "Destroying vagrant machines..."
vagrant destroy -f

echo "Removing $base_box from vagrant..."
vagrant box remove $base_box

echo "Bringing up a single vagrant machine from scratch..."
backup_vagrantfile=backup_Vagrantfile.local
local_vagrantfile=Vagrantfile.local
if [ -e $local_vagrantfile ]; then
    mv $local_vagrantfile $backup_vagrantfile
fi
echo "num_workers = 1" > $local_vagrantfile
echo "num_brokers = 0" >> $local_vagrantfile
echo "num_zookeepers = 0" >> $local_vagrantfile
vagrant up
up_success=$?
if [ $up_sucess != 0 ]; then
    echo "Failed to bring up a template vm, please try running again."
    exit $up_success
fi

echo "Packaging $worker_name..."
vagrant package $worker_name

echo "Adding new base box $base_box to vagrant..."
vagrant box add $base_box package.box

echo "Cleaning up..."
vagrant destroy -f
rm -f package.box
# restore the original vagrantfile.local
rm -f $local_vagrantfile
if [ -e $backup_vagrantfile ]; then
    mv $backup_vagrantfile $local_vagrantfile
fi
