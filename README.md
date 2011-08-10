# Kafka is a distributed publish/subscribe messaging system #

It is designed to support the following

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware Kafka can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over Kafka servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Support for parallel data load into Hadoop.

Kafka is aimed at providing a publish-subscribe solution that can handle all activity stream data and processing on a consumer-scale web site. This kind of activity (page views, searches, and other user actions) are a key ingredient in many of the social feature on the modern web. This data is typically handled by "logging" and ad hoc log aggregation solutions due to the throughput requirements. This kind of ad hoc solution is a viable solution to providing logging data to an offline analysis system like Hadoop, but is very limiting for building real-time processing. Kafka aims to unify offline and online processing by providing a mechanism for parallel load into Hadoop as well as the ability to partition real-time consumption over a cluster of machines.

See our [web site](http://incubator.apache.org/kafka/) for more details on the project.

## Contribution ##

Kafka is a new project, and we are interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://incubator.apache.org/kafka/contact.html).

The Kafka code is available from svn or a read only git mirror:
 * svn co http://svn.apache.org/repos/asf/incubator/kafka/trunk kafka
 * git clone git://git.apache.org/kafka.git

To build: 

1. ./sbt
2. update - This downloads all the dependencies for all sub projects
3. package - This will compile all sub projects and creates all the jars

Here are some useful sbt commands, to be executed at the sbt command prompt (./sbt) -

actions : Lists all the sbt commands and their descriptions

clean : Deletes all generated files (the target directory).

clean-cache : Deletes the cache of artifacts downloaded for automatically managed dependencies.

clean-lib : Deletes the managed library directory.

compile : Compile all the sub projects, but not create the jars

test : Run all unit tests in all sub projects

release-zip : Create all the jars, run unit tests and create a deployable release zip

package-all: Creates jars for src, test, docs etc

projects : List all the sub projects 

project sub_project_name : Switch to a particular sub-project. For example, to switch to the core kafka code, use "project core-kafka"

Following commands can be run only on a particular sub project -

test-only package.test.TestName : Runs only the specified test in the current sub project

run : Provides options to run any of the classes that have a main method. For example, you can switch to project java-examples, and run the examples there by executing "project java-examples" followed by "run" 


