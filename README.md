Apache Kafka
=================
See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test Apache Kafka with Java 8, 11 and 14. We set the `release` parameter in javac and scalac
to `8` to ensure the generated binaries are compatible with Java 8 or higher (independently of the Java version
used for compilation).

Scala 2.13 is used by default, see below for how to use a different Scala version or all of the supported Scala versions.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in https://kafka.apache.org/documentation.html#quickstart

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
    ./gradlew clients:test --tests RequestResponseTest

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testMetadataUpdateWaitTime

### Running a particular unit/integration test with log4j output ###
Change the log4j setting in either `clients/src/test/resources/log4j.properties` or `core/src/test/resources/log4j.properties`

    ./gradlew clients:test --tests RequestResponseTest

### Specifying test retries ###
By default, each failed test is retried once up to a maximum of five retries per test run. Tests are retried at the end of the test task. Adjust these parameters in the following way:

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=5
    
See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) for more details.

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage

### Generating an API compatibility report between the latest release and your local branch ###

    ./gradlew apiCompatibilityReport

By default it just lists the incompatible changes. If you want a more complete report, you can set the `onlyIncompatible`
flag to false:

    ./gradlew apiCompatibilityReport -PonlyIncompatible=false

There are some other options too. If you need to control the build result on binary or source incompatible changes, 
then you can do it so with the following flags. The below example would result in failing only in case of source
incompatibility.

    ./gradlew apiCompatibilityReport -PfailOnBinaryIncompatibility=false -PfailOnSourceIncompatibility=true

It is allowed to do incompatible changes in major releases but compatibility should be kept in minor and patch
(maintenance) releases. This is called [semantic versioning](https://semver.org/spec/v2.0.0.html).
This can be controlled the following way:

    ./gradlew apiCompatibilityReport -PfailOnSemanticIncompatibility=true

### Generating an API compatibility report between releases  ###

     ./gradlew apiCompatibilityReport -Psource=1.1.0 -Ptarget=1.1.1

### Generating an API compatibility report between a release and a local branch ###

     ./gradlew apiCompatibilityReport -Psource=2.0.0 -Ptarget=my-patch-branch
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean releaseTarGz

The above command will fail if you haven't set up the signing key. To bypass signing the artifact, you can run:

    ./gradlew clean releaseTarGz -x signArchives

The release file can be found inside `./core/build/distributions/`.

### Building auto generated messages ###
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:
 
    ./gradlew processMessages processTestMessages

### Cleaning the build ###
    ./gradlew clean

### Running a task with one of the Scala versions available (2.12.x or 2.13.x) ###
*Note that if building the jars with a version other than 2.12.x, you need to set the `SCALA_VERSION` variable or change it in `bin/kafka-run-class.sh` to run the quick start.*

You can pass either the major version (eg 2.12) or the full version (eg 2.12.7):

    ./gradlew -PscalaVersion=2.12 jar
    ./gradlew -PscalaVersion=2.12 test
    ./gradlew -PscalaVersion=2.12 releaseTarGz

### Running a task with all the scala versions enabled by default ###

Invoke the `gradlewAll` script followed by the task(s):

    ./gradlewAll test
    ./gradlewAll jar
    ./gradlewAll releaseTarGz

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

### Publishing the jar for all version of Scala and for all projects to maven ###
    ./gradlewAll uploadArchives

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
    ./gradlewAll install

### Building the test jar ###
    ./gradlew testJar

### Determining how transitive dependencies are added ###
    ./gradlew core:dependencies --configuration runtime

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.

#### Spotbugs ####
Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:

    ./gradlew spotbugsMain spotbugsTest -x test

The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: limits the maximum number of processes for each task.
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: the maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.

### Dependency Analysis ###

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtime --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

### Running system tests ###

See [tests/README.md](tests/README.md).

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html
