# Kafka is a distributed publish/subscribe messaging system #

It is designed to support the following

* Persistent messaging with O(1) disk structures that provide constant time performance even with many TB of stored messages.
* High-throughput: even with very modest hardware Kafka can support hundreds of thousands of messages per second.
* Explicit support for partitioning messages over Kafka servers and distributing consumption over a cluster of consumer machines while maintaining per-partition ordering semantics.
* Support for parallel data load into Hadoop.

Kafka is aimed at providing a publish-subscribe solution that can handle all activity stream data and processing on a consumer-scale web site. This kind of activity (page views, searches, and other user actions) are a key ingredient in many of the social feature on the modern web. This data is typically handled by "logging" and ad hoc log aggregation solutions due to the throughput requirements. This kind of ad hoc solution is a viable solution to providing logging data to an offline analysis system like Hadoop, but is very limiting for building real-time processing. Kafka aims to unify offline and online processing by providing a mechanism for parallel load into Hadoop as well as the ability to partition real-time consumption over a cluster of machines.

See our [web site](http://kafka.apache.org/) for more details on the project.

## Contribution ##

Kafka is a new project, and we are interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

The Kafka code is available from:
 * git clone http://git-wip-us.apache.org/repos/asf/kafka.git kafka

To contribute you can follow:
 * https://cwiki.apache.org/confluence/display/KAFKA/Git+Workflow

To build for all supported versions of Scala: 

1. ./sbt +package

To build for a particular version of Scala (either 2.8.0, 2.8.2, 2.9.1 or 2.9.2): 

1. ./sbt "++2.8.0 package" *or* ./sbt "++2.8.2 package" *or* ./sbt "++2.9.1 package" *or* ./sbt "++2.9.2 package"

Here are some useful sbt commands, to be executed at the sbt command prompt (./sbt). Prefixing with "++<version> " runs the
command for a specific Scala version, prefixing with "+" will perform the action for all versions of Scala, and no prefix
runs the command for the default (2.8.0) version of Scala. -

tasks : Lists all the sbt commands and their descriptions

clean : Deletes all generated files (the target directory).

compile : Compile all the sub projects, but not create the jars

test : Run all unit tests in all sub projects

release-zip : Create all the jars, run unit tests and create a deployable release zip

package: Creates jars for src, test, docs etc

projects : List all the sub projects 

project sub_project_name : Switch to a particular sub-project. For example, to switch to the core kafka code, use "project core-kafka"

Following commands can be run only on a particular sub project -

test-only package.test.TestName : Runs only the specified test in the current sub project

run : Provides options to run any of the classes that have a main method. For example, you can switch to project java-examples, and run the examples there by executing "project java-examples" followed by "run" 

For more details please see the [SBT documentation](https://github.com/harrah/xsbt/wiki)

