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
  - Docker 1.12.3 (or higher) is installed and running on the machine.
  - Test requires that Kafka, including system test libs, is built. This can be done by running
```
./gradlew clean systemTestLibs
```
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
* Run a specific tests file
```
TC_PATHS="tests/kafkatest/tests/client/pluggable_test.py" bash tests/docker/run_tests.sh
```
* Run multiple test files
```
TC_PATHS="tests/kafkatest/tests/client/pluggable_test.py tests/kafkatest/services/console_consumer.py" bash tests/docker/run_tests.sh
```
* Run a specific test class
```
TC_PATHS="tests/kafkatest/tests/client/pluggable_test.py::PluggableConsumerTest" bash tests/docker/run_tests.sh
```
* Run a specific test method
```
TC_PATHS="tests/kafkatest/tests/client/pluggable_test.py::PluggableConsumerTest.test_start_stop" bash tests/docker/run_tests.sh
```
* Run a specific test method with specific parameters
```
TC_PATHS="tests/kafkatest/tests/streams/streams_upgrade_test.py::StreamsUpgradeTest.test_metadata_upgrade" _DUCKTAPE_OPTIONS='--parameters '\''{"from_version":"0.10.1.1","to_version":"2.6.0-SNAPSHOT"}'\' bash tests/docker/run_tests.sh
```
* Run tests with a specific image name
```
image_name="ducker-ak-openjdk:17-buster" bash tests/docker/run_tests.sh
```
* Run tests with a different JVM
```
bash tests/docker/ducker-ak up -j 'openjdk:17-buster'; tests/docker/run_tests.sh
```
* Remove ducker-ak containers
```
bash tests/docker/ducker-ak down -f
```
* Rebuild first and then run tests
```
REBUILD="t" bash tests/docker/run_tests.sh
```
* Run tests with Kafka in `native` mode
  - To run tests with Kafka in `native` mode, pass `{"kafka_mode": "native"}` to the ducktape globals. This will bring up ducker nodes with the native Kafka binary inside them and use it to start Kafka while running the tests.
    ```
    _DUCKTAPE_OPTIONS="--globals '{\"kafka_mode\":\"native\"}'" TC_PATHS="tests/kafkatest/tests/"  bash tests/docker/run_tests.sh
    ```
  - To only bring up ducker nodes with kafka native binary inside it.
    ```
    bash tests/docker/ducker-ak up -m native
    ```
  - To run tests with Kafka in `native` mode using `ducker-ak test`(Make sure that the ducker nodes are already up with kafka native binary inside it).
    ```
    tests/docker/ducker-ak test tests/kafkatest/tests/client/compression_test.py -- --globals '{\"kafka_mode\":\"native\"}'
    ```
* Debug tests in VS Code:
  - Run test with `--debug` flag (can be before or after file name):
    ```
    tests/docker/ducker-ak up; tests/docker/ducker-ak test tests/kafkatest/tests/core/security_test.py --debug
    ```
  - Test will run in debug mode and wait for a debugger to attach. 
  - Launch VS Code debugger with `"attach"` request - here's an example:
    ```json
    {
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Attach to Ducker",
            "type": "python",
            "request": "attach",
            "connect": {
                "host": "localhost",
                "port": 5678
            },
            "justMyCode": false,
            "pathMappings": [
                {
                    "localRoot": "${workspaceFolder}",
                    "remoteRoot": "."
                }
            ]
        }
      ]
    }
    ```
  - To pass `--debug` flag to ducktape itself, use `--`:
    ```
    tests/docker/ducker-ak test tests/kafkatest/tests/core/security_test.py --debug -- --debug
    ```

* Notes
  - The scripts to run tests creates and destroys docker network named *knw*.
   This network can't be used for any other purpose.
  - The docker containers are named knode01, knode02 etc.
   These nodes can't be used for any other purpose.

* Exposing ports using --expose-ports option of `ducker-ak up` command

    If `--expose-ports` is specified then we will expose those ports to random ephemeral ports
    on the host. The argument can be a single port (like 5005), a port range like (5005-5009)
    or a combination of port/port-range separated by comma (like 2181,9092 or 2181,5005-5008).
    By default no port is exposed.
    
    The exposed port mapping can be seen by executing `docker ps` command. The PORT column
    of the output shows the mapping like this (maps port 33891 on host to port 2182 in container):

    0.0.0.0:33891->2182/tcp

    Behind the scene Docker is setting up a DNAT rule for the mapping and it is visible in
    the DOCKER section of iptables command (`sudo iptables -t nat -L -n`), something like:

    <pre>DNAT       tcp  --  0.0.0.0/0      0.0.0.0/0      tcp       dpt:33882       to:172.22.0.2:9092</pre>

    The exposed port(s) are useful to attach a remote debugger to the process running
    in the docker image. For example if port 5005 was exposed and is mapped to an ephemeral
    port (say 33891), then a debugger attaching to port 33891 on host will be connecting to
    a debug session started at port 5005 in the docker image. As an example, for above port
    numbers, run following commands in the docker image (say by ssh using `./docker/ducker-ak ssh ducker02`):

    > $ export KAFKA_OPTS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005"
    
    > $ /opt/kafka-dev/bin/kafka-topics.sh --bootstrap-server ducker03:9095 --topic __consumer_offsets --describe

    This will run the TopicCommand to describe the __consumer-offset topic. The java process
    will stop and wait for debugger to attach as `suspend=y` option was specified. Now starting
    a debugger on host with host `localhost` and following parameter as JVM setting:

    `-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=33891`

    will attach it to the TopicCommand process running in the docker image.

Local Quickstart
----------------
This quickstart will help you run the Kafka system tests on your local machine. Note this requires bringing up a cluster of virtual machines on your local computer, which is memory intensive; it currently requires around 10G RAM.
For a tutorial on how to setup and run the Kafka system tests, see
https://cwiki.apache.org/confluence/display/KAFKA/tutorial+-+set+up+and+run+Kafka+system+tests+with+ducktape

* Install Virtual Box from [https://www.virtualbox.org/](https://www.virtualbox.org/) (run `$ vboxmanage --version` to check if it's installed).
* Install Vagrant >= 1.6.4 from [https://www.vagrantup.com/](https://www.vagrantup.com/) (run `vagrant --version` to check if it's installed).
* Install system test dependencies, including ducktape, a command-line tool and library for testing distributed systems. We recommend to use virtual env for system test development

        $ cd kafka/tests
        $ virtualenv -p python3 venv
        $ . ./venv/bin/activate
        $ python3 -m pip install --editable .
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

* [Create an IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user.html). We'll give this role the ability to launch or kill additional EC2 machines.
 - Create role "kafkatest-master"
 - Role type: Amazon EC2
 - Attach policy: AmazonEC2FullAccess (this will allow our test-driver to create and destroy EC2 instances)

* If you haven't already, [set up a keypair to use for SSH access](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-key-pairs.html). For the purpose
of this quickstart, let's say the keypair name is kafkatest, and you've saved the private key in kafktest.pem

* Next, create a EC2 security group called "kafkatest".
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

        $ sudo apt-get update && sudo apt-get -y upgrade && sudo apt-get install -y python3-pip git
        $ pip install ducktape

* Get Kafka:

        $ git clone https://git-wip-us.apache.org/repos/asf/kafka.git kafka

* Update your AWS credentials:

        export AWS_IAM_ROLE=$(curl -s http://169.254.169.254/latest/meta-data/iam/info | grep InstanceProfileArn | cut -d '"' -f 4 | cut -d '/' -f 2)
        export AWS_ACCESS_KEY=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM_ROLE | grep AccessKeyId | awk -F\" '{ print $4 }')
        export AWS_SECRET_KEY=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM_ROLE | grep SecretAccessKey | awk -F\" '{ print $4 }')
        export AWS_SESSION_TOKEN=$(curl -s http://169.254.169.254/latest/meta-data/iam/security-credentials/$AWS_IAM_ROLE | grep Token | awk -F\" '{ print $4 }')

* Install some dependencies:

        $ cd kafka
        $ ./vagrant/aws/aws-init.sh
        $ . ~/.bashrc

* An example Vagrantfile.local has been created by aws-init.sh which looks something like:

        # Vagrantfile.local
        ec2_instance_type = "..." # Pick something appropriate for your
                                  # test. Note that the default m3.medium has
                                  # a small disk.
        ec2_spot_max_price = "0.123"  # On-demand price for instance type
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

* Update Worker VM

If you change code in a branch on your driver VM, you need to update your worker VM to pick up this change:

        $ ./gradlew systemTestLibs
        $ vagrant rsync

* To halt your workers without destroying persistent state, run `vagrant halt`. Run `vagrant destroy -f` to destroy all traces of your workers.

Unit Tests
----------
The system tests have unit tests! The various services in the python `kafkatest` module are reasonably complex, and intended to be reusable. Hence we have unit tests
for the system service classes.

Where are the unit tests?
* The kafkatest unit tests are located under kafka/tests/unit

How do I run the unit tests?
```bash
$ cd kafka/tests # The base system test directory
$ python3 setup.py test
```

How can I add a unit test?
* Follow the naming conventions - module name starts with "check", class name begins with "Check", test method name begins with "check"
* These naming conventions are defined in "setup.cfg". We use "check" to distinguish unit tests from system tests, which use "test" in the various names.

