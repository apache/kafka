Apache Kafka
=================
See our [web site](http://kafka.apache.org) for details on the project.

### Building a jar and running it ###
    ./gradlew jar  

Follow instuctions in http://kafka.apache.org/documentation.html#quickstart

### Running unit tests ###
    ./gradlew test

### Forcing re-running unit tests w/o code change ###
    ./gradlew cleanTest test

### Running a particular unit test ###
    ./gradlew -Dtest.single=RequestResponseSerializationTest core:test

### Building a binary release gzipped tar ball ###
    ./gradlew clean
    ./gradlew releaseTarGz  

The release file can be found inside ./core/build/distributions/.

### Cleaning the build ###
    ./gradlew clean

### Running a task on a particular version of Scala ####
either 2.8.0, 2.8.2, 2.9.1, 2.9.2 or 2.10.1) (If building a jar with a version other than 2.8.0, the scala version variable in bin/kafka-run-class.sh needs to be changed to run quick start.)
    ./gradlew -PscalaVersion=2.9.1 jar
    ./gradlew -PscalaVersion=2.9.1 test
    ./gradlew -PscalaVersion=2.9.1 releaseTarGz

### Running a task for a specific project ###
This is for 'core', 'perf', 'contrib:hadoop-consumer', 'contrib:hadoop-producer', 'examples' and 'clients'
    ./gradlew core:jar
    ./gradlew core:test

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
    ./gradlew eclipse
    ./gradlew idea

### Building the jar for all scala versions and for all projects ###
    ./gradlew jarAll

### Running unit tests for all scala versions and for all projects ###
    ./gradlew testAll

### Building a binary release gzipped tar ball for all scala versions ###
    ./gradlew releaseTarGzAll

### Publishing the jar for all version of Scala and for all projects to maven ###
    ./gradlew uploadArchivesAll

Please note for this to work you should create/update `~/.gradle/gradle.properties` and assign the following variables

    mavenUrl=
    mavenUsername=
    mavenPassword=
    signing.keyId=
    signing.password=
    signing.secretKeyRingFile=

### Building the test jar ###
    ./gradlew testJar

### Determining how transitive dependencies are added ###
    ./gradlew core:dependencies --configuration runtime

### Contribution ###

Apache Kafka interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * http://kafka.apache.org/contributing.html

We also welcome patches for the website and documentation which can be found here:
 * https://svn.apache.org/repos/asf/kafka/site
