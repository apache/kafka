Apache Kafka
=================
See our [web site](http://kafka.apache.org) for details on the project.

You need to have [Gradle](http://www.gradle.org/installation) and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

Kafka requires Gradle 4.5 or higher.

Java 8 should be used for building in order to support both Java 8 and Java 10 at runtime.

### First bootstrap and download the wrapper ###
    cd kafka_source_dir
    gradle

Now everything else will work.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in http://kafka.apache.org/documentation.html#quickstart

### Build source jar ###
    ./gradlew srcJar

### Build aggregated javadoc ###
    ./gradlew aggregatedJavadoc

### Build javadoc and scaladoc ###
    ./gradlew javadoc
    ./gradlew javadocJar # builds a javadoc jar for each module
    ./gradlew scaladoc
    ./gradlew scaladocJar # builds a scaladoc jar for each module
    ./gradlew docsJar # builds both (if applicable) javadoc and scaladoc jars for each module

### Run unit/integration tests ###
    ./gradlew test # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    
### Force re-running tests without code change ###
    ./gradlew cleanTest test
    ./gradlew cleanTest unitTest
    ./gradlew cleanTest integrationTest

### Running a particular unit/integration test ###
    ./gradlew -Dtest.single=RequestResponseSerializationTest core:test

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testMetadataUpdateWaitTime

### Running a particular unit/integration test with log4j output ###
Change the log4j setting in either `clients/src/test/resources/log4j.properties` or `core/src/test/resources/log4j.properties`

    ./gradlew -i -Dtest.single=RequestResponseSerializationTest core:test

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean
    ./gradlew releaseTarGz

The above command will fail if you haven't set up the signing key. To bypass signing the artifact, you can run:

    ./gradlew releaseTarGz -x signArchives

The release file can be found inside `./core/build/distributions/`.

### Cleaning the build ###
    ./gradlew clean

### Running a task on a particular version of Scala (either 2.11.x or 2.12.x) ###
*Note that if building the jars with a version other than 2.11.12, you need to set the `SCALA_VERSION` variable or change it in `bin/kafka-run-class.sh` to run the quick start.*

You can pass either the major version (eg 2.11) or the full version (eg 2.11.12):

    ./gradlew -PscalaVersion=2.11 jar
    ./gradlew -PscalaVersion=2.11 test
    ./gradlew -PscalaVersion=2.11 releaseTarGz

### Running a task for a specific project ###
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
*Note that this is not strictly necessary (IntelliJ IDEA has good built-in support for Gradle projects, for example).*

    ./gradlew eclipse
    ./gradlew idea

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

### Building the jar for all scala versions and for all projects ###
    ./gradlew jarAll

### Running unit/integration tests for all scala versions and for all projects ###
    ./gradlew testAll

### Building a binary release gzipped tar ball for all scala versions ###
    ./gradlew releaseTarGzAll

### Publishing the jar for all version of Scala and for all projects to maven ###
    ./gradlew uploadArchivesAll

Please note for this to work you should create/update `${GRADLE_USER_HOME}/gradle.properties` (typically, `~/.gradle/gradle.properties`) and assign the following variables

    mavenUrl=
    mavenUsername=
    mavenPassword=
    signing.keyId=
    signing.password=
    signing.secretKeyRingFile=

### Publishing the streams quickstart archetype artifact to maven ###
For the Streams archetype project, one cannot use gradle to upload to maven; instead the `mvn deploy` command needs to be called at the quickstart folder:

    cd streams/quickstart
    mvn deploy

Please note for this to work you should create/update user maven settings (typically, `${USER_HOME}/.m2/settings.xml`) to assign the following variables

    <settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0
                           https://maven.apache.org/xsd/settings-1.0.0.xsd">
    ...                           
    <servers>
       ...
       <server>
          <id>apache.snapshots.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
       </server>
       <server>
          <id>apache.releases.https</id>
          <username>${maven_username}</username>
          <password>${maven_password}</password>
        </server>
        ...
     </servers>
     ...


### Installing the jars to the local Maven repository ###
    ./gradlew installAll

### Building the test jar ###
    ./gradlew testJar

### Determining how transitive dependencies are added ###
    ./gradlew core:dependencies --configuration runtime

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, findbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also are printed to the console. The build will fail if Checkstyle fails.

#### Findbugs ####
Findbugs uses static analysis to look for bugs in the code.
You can run findbugs using:

    ./gradlew findbugsMain findbugsTest -x test

The findbugs warnings will be found in `reports/findbugs/main.html` and `reports/findbugs/test.html` files in the subproject build
directories.  Use -PxmlFindBugsReport=true to generate an XML report instead of an HTML one.

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: limits the maximum number of processes for each task.
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlFindBugsReport`: enable XML reports for findBugs. This also disables HTML reports as only one can be enabled at a time.

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * http://kafka.apache.org/contributing.html
