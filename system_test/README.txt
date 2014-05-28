# ==========================
# Known Issues:
# ==========================
1. This test framework currently doesn't support MacOS due to different "ps" argument options from Linux. The correct ps execution is required to terminate the background running processes properly.

# ==========================
# Overview
# ==========================

"system_test" is now transformed to a system regression test framework intended for the automation of system / integration testing of data platform software such as Kafka. The test framework is implemented in Python which is a popular scripting language with well supported features.

The framework has the following levels:

1. The first level is generic and does not depend on any product specific details.
   location: system_test
   a. system_test_runner.py - It implements the main class RegTest as an entry point.
   b. system_test_env.py    - It implements the class RegTestEnv which defines the testing environment of a test session such as the base directory and environment variables specific to the local machine.

2. The second level defines a suite of testing such as Kafka's replication (including basic testing, failure testing, ... etc)
   location: system_test/<suite directory name>*.

   * Please note the test framework will look for a specific suffix of the directories under system_test to determine what test suites are available. The suffix of <suite directory name> can be defined in SystemTestEnv class (system_test_env.py)

   a. replica_basic_test.py - This is a test module file. It implements the test logic for basic replication testing as follows:

       i.    start zookeepers
       ii.   start brokers
       iii.  create kafka topics
       iv.   lookup the brokerid as a leader
       v.    terminate the leader (if defined in the testcase config json file)
       vi.   start producer to send n messages
       vii.  start consumer to receive messages
       viii. validate if there is data loss

   b. config/ - This config directory provides templates for all properties files needed for zookeeper, brokers, producer and consumer (any changes in the files under this directory would be reflected or overwritten by the settings under testcase_<n>/testcase_<n>_properties.json)

   d. testcase_<n>** - The testcase directory contains the testcase argument definition file: testcase_1_properties.json. This file defines the specific configurations for the testcase such as the followings (eg. producer related):
      i.   no. of producer threads
      ii.  no. of messages to produce
      iii. zkconnect string
      
      When this test case is being run, the test framework will copy and update the template properties files to testcase_<n>/config. The logs of various components will be saved in testcase_<n>/logs

   ** Please note the test framework will look for a specific prefix of the directories under system_test/<test suite dir>/ to determine what test cases are available. The prefix of <testcase directory name> can be defined in SystemTestEnv class (system_test_env.py)

# ==========================
# Quick Start
# ==========================

* Please note that the following commands should be executed after downloading the kafka source code to build all the required binaries:
  1. <kafka install dir>/ $ ./gradlew jar

  Now you are ready to follow the steps below.
  1. Update system_test/cluster_config.json for "kafka_home" & "java_home" specific to your environment
  2. Edit system_test/replication_testsuite/testcase_1/testcase_1_properties.json and update "broker-list" to the proper settings of your environment. (If this test is to be run in a single localhost, no change is required for this.)
  3. To run the test, go to <kafka_home>/system_test and run the following command:
     $ python -u -B system_test_runner.py 2>&1 | tee system_test_output.log
  4. To turn on debugging, update system_test/logging.conf by changing the level in handlers session from INFO to DEBUG.

# ==========================
# Adding Test Case
# ==========================

To create a new test suite called "broker_testsuite", please do the followings:

  1. Copy and paste system_test/replication_testsuite => system_test/broker_testsuite
  2. Rename system_test/broker_testsuite/replica_basic_test.py => system_test/broker_testsuite/broker_basic_test.py
  3. Edit system_test/broker_testsuite/broker_basic_test.py and update all ReplicaBasicTest related class name to BrokerBasicTest (as an example)
  4. Follow the flow of system_test/broker_testsuite/broker_basic_test.py and modify the necessary test logic accordingly.


To create a new test case under "replication_testsuite", please do the followings:

  1. Copy and paste system_test/replication_testsuite/testcase_1 => system_test/replication_testsuite/testcase_2
  2. Rename system_test/replication_testsuite/testcase_2/testcase_1_properties.json => system_test/replication_testsuite/testcase_2/testcase_2_properties.json
  3. Update system_test/replication_testsuite/testcase_2/testcase_2_properties.json with the corresponding settings for testcase 2.

Note:
The following testcases are for the old producer and the old mirror maker. We can remove them once we phase out the old producer client.
  replication_testsuite: testcase_{10101 - 10110} testcase_{10131 - 10134}
  mirror_maker_testsuite: testcase_{15001 - 15006}
