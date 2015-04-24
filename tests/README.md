System Integration & Performance Testing
========================================

This directory contains Kafka system integration and performance tests. 
[Ducktape](https://github.com/confluentinc/ducktape) is used to run the tests.  

Ducktape is a distributed testing framework which provides test runner, 
result reporter and utilities to pull up and tear down services. It automatically
discovers tests from a directory and generate an HTML report for each run.

To run the tests: 

1. Build a specific branch of Kafka
       
        $ cd kafka
        $ git checkout $BRANCH
        $ gradle
        $ ./gradlew jar
      
2. Setup a testing cluster. You can use Vagrant to create a cluster of local 
   VMs or on EC2. Configure your Vagrant setup by creating the file 
   `Vagrantfile.local` in the directory of your Kafka checkout. At a minimum
   , you *MUST* set `mode = "test"` and the value of `num_workers` high enough for 
   the test you're trying to run. If you run on AWS, you also need to set 
   enable_dns = true.
        
3. Bring up the cluster, making sure you have enough workers. For Vagrant, 
   use `vagrant up`. If you want to run on AWS, use `vagrant up
   --provider=aws --no-parallel`.
4. Install ducktape:
       
        $ git clone https://github.com/confluentinc/ducktape
        $ cd ducktape
        $ pip install ducktape
5. Run the system tests using ducktape, you can view results in the `results`
   directory.
        
        $ cd tests
        $ ducktape tests
6. To iterate/run again if you made any changes:

        $ cd kafka
        $ ./gradlew jar
        $ vagrant rsync # Re-syncs build output to cluster
