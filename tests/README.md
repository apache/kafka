System Integration & Performance Testing
========================================

This directory contains Kafka system integration and performance tests. 
[ducktape](https://github.com/confluentinc/ducktape) is used to run the tests.  
(ducktape is a distributed testing framework which provides test runner, 
result reporter and utilities to pull up and tear down services.) 

Local Quickstart
----------------
This quickstart will help you run the Kafka system tests on your local machine.

* Install Virtual Box from [https://www.virtualbox.org/](https://www.virtualbox.org/) (run `$ vboxmanage --version` to check if it's installed).
* Install Vagrant >= 1.6.4 from [http://www.vagrantup.com/](http://www.vagrantup.com/) (run `vagrant --version` to check if it's installed).
* Install Vagrant Plugins:

        # Required
        $ vagrant plugin install vagrant-hostmanager

* Build a specific branch of Kafka
       
        $ cd kafka
        $ git checkout $BRANCH
        $ gradle
        $ ./gradlew jar
      
* Setup a testing cluster with Vagrant. Configure your Vagrant setup by creating the file 
   `Vagrantfile.local` in the directory of your Kafka checkout. At a minimum, you *MUST* 
   set `mode = "test"` and the value of `num_workers` high enough for the test(s) you're trying to run.
    An example resides in kafka/vagrant/system-test-Vagrantfile.local

        # Example Vagrantfile.local for use on local machine
        # Vagrantfile.local should reside in the base Kafka directory
        mode = "test"
        num_workers = 9

* Bring up the cluster (note that the initial provisioning process can be slow since it involves
installing dependencies and updates on every vm.):

        $ vagrant up

* Install ducktape:
       
        $ pip install ducktape

* Run the system tests using ducktape:

        $ cd tests
        $ ducktape kafkatest/tests

* If you make changes to your Kafka checkout, you'll need to rebuild and resync to your Vagrant cluster:

        $ cd kafka
        $ ./gradlew jar
        $ vagrant rsync # Re-syncs build output to cluster
        
EC2 Quickstart
--------------
This quickstart will help you run the Kafka system tests using Amazon EC2. As a convention, we'll use "kafkatest" 
in most names, but you can use whatever you want.

* [Create an IAM role](http://docs.aws.amazon.com/IAM/latest/UserGuide/Using_SettingUpUser.html#Using_CreateUser_console). We'll give this role the ability to launch or kill additional EC2 machines.
 - Create role "kafkatest-master"
 - Role type: Amazon EC2
 - Attach policy: AmazonEC2FullAccess
 
* If you haven't already, [set up a keypair to use for SSH access](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). For the purpose
of this quickstart, let's say the keypair name is kafkatest, and you've saved the private key in kafktest.pem

* Next, create a security group called "kafkatest-insecure". 
 - After creating the group, inbound rules: allow SSH on port 22 from anywhere; also, allow access on all ports (0-65535) from other machines in the kafkatest-insecure group.

* Launch a new test driver machine 
 - OS: Ubuntu server is recommended
 - Instance type: t2.medium is easily enough since this machine is just a driver
 - Instance details: Most defaults are fine.
 - IAM role -> kafkatest-master
 - Tagging the instance with a useful name is recommended. 
 - Security group -> 'kafkatest-insecure'
  
* Once the machine is started, upload the SSH key:

        $ scp -i /path/to/kafkatest.pem \
            /path/to/kafkatest.pem ubuntu@public.hostname.amazonaws.com:kafkatest.pem

* Grab the public hostname/IP and SSH into the host:

        $ ssh -i /path/to/kafkatest.pem ubuntu@public.hostname.amazonaws.com
        
* The following steps assume you are logged into
the test driver machine.

* Start by making sure you're up to date, and install git and ducktape:

        $ sudo apt-get update && sudo apt-get -y upgrade && sudo apt-get install -y git
        $ pip install ducktape

* Get Kafka:

        $ git clone https://git-wip-us.apache.org/repos/asf/kafka.git kafka
        
* Install some dependencies:

        $ cd kafka
        $ kafka/vagrant/aws/aws-init.sh
        $ . ~/.bashrc

* An example Vagrantfile.local has been created by aws-init.sh which looks something like:

        # Vagrantfile.local
        ec2_instance_type = "..." # Pick something appropriate for your
                                  # test. Note that the default m3.medium has
                                  # a small disk.
        mode = "test"
        num_workers = 9
        ec2_keypair_name = 'kafkatest'
        ec2_keypair_file = '/home/ubuntu/kafkatest.pem'
        ec2_security_groups = ['kafkatest-insecure']
        ec2_region = 'us-west-2'
        ec2_ami = "ami-29ebb519"

* Start up the instances:

        $ vagrant up --provider=aws --no-provision --no-parallel && vagrant provision

* Now you should be able to run tests:

        $ cd kafka/tests
        $ ducktape kafkatest/tests


