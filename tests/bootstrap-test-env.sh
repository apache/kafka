#!/usr/bin/env bash

# This script automates the process of setting up a local machine for running Kafka system tests

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
vagrant_version=`vagrant --version | egrep -o "\d+\.\d+\.\d+"`
bad_vagrant=false
if [ "$(version $vagrant_version)" -lt "$(version 1.6.4)" ]; then
    echo "Found Vagrant version $vagrant_version. Please upgrade to 1.6.4 or higher (see http://www.vagrantup.com for details)"
    bad_vagrant=false
else
    echo "Vagrant installation looks good."
fi

if [ "x$bad_vagrant" == "xtrue" -o "x$bad_vb" == "xtrue" ]; then
    exit 1
fi

echo "Checking for necessary Vagrant plugins..."
install_hostmanager=false
hostmanager_version=`vagrant plugin list | grep vagrant-hostmanager | egrep -o "\d+\.\d+\.\d+"`
if [ -z "$hostmanager_version"  ]; then
    install_hostmanager=true
elif [ "$hostmanager_version" != "1.5.0" ]; then
    echo "You have the wrong version of vagrant plugin vagrant-hostmanager. Uninstalling..."
    vagrant plugin uninstall vagrant-hostmanager
    install_hostmanager=true
fi
if [ "x$install_hostmanager" == "xtrue" ]; then
    vagrant plugin install vagrant-hostmanager --plugin-version 1.5.0
fi

echo "Creating and packaging a reusable base box for Vagrant..."
vagrant/package-base-box.sh

if [ ! -e Vagrantfile.local ]; then
    echo "Creating Vagrantfile.local..."
    cp vagrant/system-test-Vagrantfile.local Vagrantfile.local
else
    echo "Found an existing Vagrantfile.local. Keeping without overwriting..."
fi

# Sanity check contents of Vagrantfile.local
echo "Checking Vagrantfile.local..."
vagrantfile_ok=true
num_brokers=`egrep -o "num_brokers\s*=\s*\d+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
num_zookeepers=`egrep -o "num_zookeepers\s*=\s*\d+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
num_workers=`egrep -o "num_workers\s*=\s*\d+" Vagrantfile.local | cut -d '=' -f 2 | xargs`
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
