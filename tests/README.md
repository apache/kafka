System Integration & Performance Testing
========================================

This directory contains Kafka system integration and performance tests.
[ducktape](https://github.com/confluentinc/ducktape) is used to run the tests.  
(ducktape is a distributed testing framework which provides test runner,
result reporter and utilities to pull up and tear down services.)

Running tests using docker
--------------------------
Docker containers can be used for running kafka system tests locally.
* Requirements
  - Docker 1.12.3 is installed and running on the machine.
  - Test require a single kafka_*SNAPSHOT.tgz to be present in core/build/distributions.
   This can be done by running ./gradlew clean releaseTarGz  
* Run all tests
```
bash tests/docker/run_tests.sh
```
* Run all tests with debug on (warning will produce log of logs)
```
_DUCKTAPE_OPTIONS="--debug" bash tests/docker/run_tests.sh | tee debug_logs.txt
```
* Run a subset of tests
```
TC_PATHS="tests/kafkatest/tests/streams tests/kafkatest/tests/tools" bash tests/docker/run_tests.sh
```
* Notes
  - The scripts to run tests creates and destroys docker network named *knw*.
   This network can't be used for any other purpose.
  - The docker containers are named knode01, knode02 etc.
   These nodes can't be used for any other purpose.

Local Quickstart
----------------
This quickstart will help you run the Kafka system tests on your local machine. Note this requires bringing up a cluster of virtual machines on your local computer, which is memory intensive; it currently requires around 10G RAM.
For a tutorial on how to setup and run the Kafka system tests, see
https://cwiki.apache.org/confluence/display/KAFKA/tutorial+-+set+up+and+run+Kafka+system+tests+with+ducktape

* Install Virtual Box from [https://www.virtualbox.org/](https://www.virtualbox.org/) (run `$ vboxmanage --version` to check if it's installed).
* Install Vagrant >= 1.6.4 from [http://www.vagrantup.com/](http://www.vagrantup.com/) (run `vagrant --version` to check if it's installed).
* Install system test dependencies, including ducktape, a command-line tool and library for testing distributed systems. We recommend to use virtual env for system test development

        $ cd kafka/tests
        $ virtualenv venv
        $ . ./venv/bin/activate
        $ python setup.py develop
        $ cd ..  # back to base kafka directory

* Run the bootstrap script to set up Vagrant for testing

        $ tests/bootstrap-test-env.sh

* Bring up the test cluster

        $ vagrant/vagrant-up.sh
        $ # When using Virtualbox, it also works to run: vagrant up

* Build the desired branch of Kafka

        $ git checkout $BRANCH
        $ gradle  # (only if necessary)
        $ ./gradlew systemTestLibs

* Run the system tests using ducktape:

        $ ducktape tests/kafkatest/tests

EC2 Quickstart
--------------
This quickstart will help you run the Kafka system tests on EC2. In this setup, all logic is run
on EC2 and none on your local machine.

There are a lot of steps here, but the basic goals are to create one distinguished EC2 instance that
will be our "test driver", and to set up the security groups and iam role so that the test driver
can create, destroy, and run ssh commands on any number of "workers".

As a convention, we'll use "kafkatest" in most names, but you can use whatever name you want.

Preparation
-----------
In these steps, we will create an IAM role which has permission to create and destroy EC2 instances,
set up a keypair used for ssh access to the test driver and worker machines, and create a security group to allow the test driver and workers to all communicate via TCP.

* [Create an IAM role](http://docs.aws.amazon.com/IAM/latest/UserGuide/Using_SettingUpUser.html#Using_CreateUser_console). We'll give this role the ability to launch or kill additional EC2 machines.
 - Create role "kafkatest-master"
 - Role type: Amazon EC2
 - Attach policy: AmazonEC2FullAccess (this will allow our test-driver to create and destroy EC2 instances)

* If you haven't already, [set up a keypair to use for SSH access](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). For the purpose
of this quickstart, let's say the keypair name is kafkatest, and you've saved the private key in kafktest.pem

* Next, create a security group called "kafkatest".
 - After creating the group, inbound rules: allow SSH on port 22 from anywhere; also, allow access on all ports (0-65535) from other machines in the kafkatest group.

Create the Test Driver
----------------------
* Launch a new test driver machine
 - OS: Ubuntu server is recommended
 - Instance type: t2.medium is easily enough since this machine is just a driver
 - Instance details: Most defaults are fine.
 - IAM role -> kafkatest-master
 - Tagging the instance with a useful name is recommended.
 - Security group -> 'kafkatest'


* Once the machine is started, upload the SSH key to your test driver:

        $ scp -i /path/to/kafkatest.pem \
            /path/to/kafkatest.pem ubuntu@public.hostname.amazonaws.com:kafkatest.pem

* Grab the public hostname/IP (available for example by navigating to your EC2 dashboard and viewing running instances) of your test driver and SSH into it:

        $ ssh -i /path/to/kafkatest.pem ubuntu@public.hostname.amazonaws.com

Set Up the Test Driver
----------------------
The following steps assume you have ssh'd into
the test driver machine.

* Start by making sure you're up to date, and install git and ducktape:

        $ sudo apt-get update && sudo apt-get -y upgrade && sudo apt-get install -y git
        $ pip install ducktape

* Get Kafka:

        $ git clone https://git-wip-us.apache.org/repos/asf/kafka.git kafka

* Install some dependencies:

        $ cd kafka
        $ ./vagrant/aws/aws-init.sh
        $ . ~/.bashrc

* An example Vagrantfile.local has been created by aws-init.sh which looks something like:

        # Vagrantfile.local
        ec2_instance_type = "..." # Pick something appropriate for your
                                  # test. Note that the default m3.medium has
                                  # a small disk.
        enable_hostmanager = false
        num_zookeepers = 0
        num_kafka = 0
        num_workers = 9
        ec2_keypair_name = 'kafkatest'
        ec2_keypair_file = '/home/ubuntu/kafkatest.pem'
        ec2_security_groups = ['kafkatest']
        ec2_region = 'us-west-2'
        ec2_ami = "ami-29ebb519"

* Start up the instances:

        # This will brink up worker machines in small parallel batches
        $ vagrant/vagrant-up.sh --aws

* Now you should be able to run tests:

        $ cd kafka/tests
        $ ducktape kafkatest/tests

* To halt your workers without destroying persistent state, run `vagrant halt`. Run `vagrant destroy -f` to destroy all traces of your workers.

Unit Tests
----------
The system tests have unit tests! The various services in the python `kafkatest` module are reasonably complex, and intended to be reusable. Hence we have unit tests
for the system service classes.

Where are the unit tests?
* The kafkatest unit tests are located under kafka/tests/unit

How do I run the unit tests?
* cd kafka/tests # The base system test directory
* python setup.py test

How can I add a unit test?
* Follow the naming conventions - module name starts with "check", class name begins with "Check", test method name begins with "check"
* These naming conventions are defined in "setup.cfg". We use "check" to distinguish unit tests from system tests, which use "test" in the various names.

