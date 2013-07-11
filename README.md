# Apache Kafka #

See our [web site](http://kafka.apache.org) details on the project.

## Building It ##

To build for all supported versions of Scala: 
1. ./sbt +package

To build for a particular version of Scala (either 2.8.0, 2.8.2, 2.9.1 or 2.9.2): 

1. ./sbt "++2.8.0 package" *or* ./sbt "++2.8.2 package" *or* ./sbt "++2.9.1 package" *or* ./sbt "++2.9.2 package"

## Running It ##

To run follow the instructions here:
1. http://kafka.apache.org/08/quickstart.html

## Other Build Tips ##

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

The following commands can be run only on a particular sub project -
test-only package.test.TestName : Runs only the specified test in the current sub project
run : Provides options to run any of the classes that have a main method. For example, you can switch to project java-examples, and run the examples there by executing "project java-examples" followed by "run" 

For more details please see the [SBT documentation](https://github.com/harrah/xsbt/wiki)

## Contribution ##

Kafka is a new project, and we are interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * http://kafka.apache.org/contributing.html

We also welcome patches for the website and documentation which can be found here:
 * https://svn.apache.org/repos/asf/kafka/site




