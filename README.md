Apache Kafka
=================

[![CI](https://github.com/apache/kafka/actions/workflows/ci.yml/badge.svg?branch=trunk&event=push)](https://github.com/apache/kafka/actions/workflows/ci.yml?query=event%3Apush+branch%3Atrunk)
[![Flaky Test Report](https://github.com/apache/kafka/actions/workflows/generate-reports.yml/badge.svg?branch=trunk&event=schedule)](https://github.com/apache/kafka/actions/workflows/generate-reports.yml?query=event%3Aschedule+branch%3Atrunk)

See our [web site](https://kafka.apache.org) for details on the project.

You need to have [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) installed.

We build and test Apache Kafka with 17 and 23. The `release` parameter in javac is set to `11` for the clients 
and streams modules, and `17` for the rest, ensuring compatibility with their respective
minimum Java versions. Similarly, the `release` parameter in scalac is set to `11` for the streams modules and `17`
for the rest.

Scala 2.13 is the only supported version in Apache Kafka.

### Build a jar and run it ###
    ./gradlew jar

Follow instructions in https://kafka.apache.org/quickstart

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
    ./gradlew test  # runs both unit and integration tests
    ./gradlew unitTest
    ./gradlew integrationTest
    ./gradlew quarantinedTest  # runs the quarantined tests

    
### Force re-running tests without code change ###
    ./gradlew test --rerun-tasks
    ./gradlew unitTest --rerun-tasks
    ./gradlew integrationTest --rerun-tasks

### Running a particular unit/integration test ###
    ./gradlew clients:test --tests RequestResponseTest

### Repeatedly running a particular unit/integration test with specific times by setting N ###
    N=500; I=0; while [ $I -lt $N ] && ./gradlew clients:test --tests RequestResponseTest --rerun --fail-fast; do (( I=$I+1 )); echo "Completed run: $I"; sleep 1; done

### Running a particular test method within a unit/integration test ###
    ./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
    ./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate

### Running a particular unit/integration test with log4j output ###
By default, there will be only small number of logs output while testing. You can adjust it by changing the `log4j2.yml` file in the module's `src/test/resources` directory.

For example, if you want to see more logs for clients project tests, you can modify [the line](https://github.com/apache/kafka/blob/trunk/clients/src/test/resources/log4j2.yml#L35) in `clients/src/test/resources/log4j2.yml` 
to `level: INFO` and then run:
    
    ./gradlew cleanTest clients:test --tests NetworkClientTest   

And you should see `INFO` level logs in the file under the `clients/build/test-results/test` directory.

### Specifying test retries ###
Retries are disabled by default, but you can set maxTestRetryFailures and maxTestRetries to enable retries.

The following example declares -PmaxTestRetries=1 and -PmaxTestRetryFailures=3 to enable a failed test to be retried once, with a total retry limit of 3.

    ./gradlew test -PmaxTestRetries=1 -PmaxTestRetryFailures=3

The quarantinedTest task also has no retries by default, but you can set maxQuarantineTestRetries and maxQuarantineTestRetryFailures to enable retries, similar to the test task.

    ./gradlew quarantinedTest -PmaxQuarantineTestRetries=3 -PmaxQuarantineTestRetryFailures=20

See [Test Retry Gradle Plugin](https://github.com/gradle/test-retry-gradle-plugin) for and [build.yml](.github/workflows/build.yml) more details.

### Generating test coverage reports ###
Generate coverage reports for the whole project:

    ./gradlew reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false

Generate coverage for a single module, i.e.: 

    ./gradlew clients:reportCoverage -PenableTestCoverage=true -Dorg.gradle.parallel=false
    
### Building a binary release gzipped tar ball ###
    ./gradlew clean releaseTarGz

The release file can be found inside `./core/build/distributions/`.

### Building auto generated messages ###
Sometimes it is only necessary to rebuild the RPC auto-generated message data when switching between branches, as they could
fail due to code changes. You can just run:
 
    ./gradlew processMessages processTestMessages

### Running a Kafka broker

Using compiled files:

    KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
    ./bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/kraft/reconfig-server.properties
    ./bin/kafka-server-start.sh config/kraft/reconfig-server.properties

Using docker image:

    docker run -p 9092:9092 apache/kafka:latest

### Cleaning the build ###
    ./gradlew clean

### Running a task for a specific project ###
This is for `core`, `examples` and `clients`

    ./gradlew core:jar
    ./gradlew core:test

Streams has multiple sub-projects, but you can run all the tests:

    ./gradlew :streams:testAll

### Listing all gradle tasks ###
    ./gradlew tasks

### Building IDE project ####
*Note Please ensure that JDK17 is used when developing Kafka.*

IntelliJ supports Gradle natively and it will automatically check Java syntax and compatibility for each module, even if
the Java version shown in the `Structure > Project Settings > Modules` may not be the correct one.

When it comes to Eclipse, run:

    ./gradlew eclipse

The `eclipse` task has been configured to use `${project_dir}/build_eclipse` as Eclipse's build directory. Eclipse's default
build directory (`${project_dir}/bin`) clashes with Kafka's scripts directory and we don't use Gradle's build directory
to avoid known issues with this configuration.

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

### Installing all projects to the local Maven repository ###

    ./gradlew -PskipSigning=true publishToMavenLocal

### Installing specific projects to the local Maven repository ###

    ./gradlew -PskipSigning=true :streams:publishToMavenLocal
    
### Building the test jar ###
    ./gradlew testJar

### Running code quality checks ###
There are two code quality analysis tools that we regularly run, spotbugs and checkstyle.

#### Checkstyle ####
Checkstyle enforces a consistent coding style in Kafka.
You can run checkstyle using:

    ./gradlew checkstyleMain checkstyleTest spotlessCheck

The checkstyle warnings will be found in `reports/checkstyle/reports/main.html` and `reports/checkstyle/reports/test.html` files in the
subproject build directories. They are also printed to the console. The build will fail if Checkstyle fails.
For experiments (or regression testing purposes) add `-PcheckstyleVersion=X.y.z` switch (to override project-defined checkstyle version).

#### Spotless ####
The import order is a part of static check. please call `spotlessApply` to optimize the imports of Java codes before filing pull request.

    ./gradlew spotlessApply

#### Spotbugs ####
Spotbugs uses static analysis to look for bugs in the code.
You can run spotbugs using:

    ./gradlew spotbugsMain spotbugsTest -x test

The spotbugs warnings will be found in `reports/spotbugs/main.html` and `reports/spotbugs/test.html` files in the subproject build
directories.  Use -PxmlSpotBugsReport=true to generate an XML report instead of an HTML one.

### JMH microbenchmarks ###
We use [JMH](https://openjdk.java.net/projects/code-tools/jmh/) to write microbenchmarks that produce reliable results in the JVM.
    
See [jmh-benchmarks/README.md](https://github.com/apache/kafka/blob/trunk/jmh-benchmarks/README.md) for details on how to run the microbenchmarks.

### Dependency Analysis ###

The gradle [dependency debugging documentation](https://docs.gradle.org/current/userguide/viewing_debugging_dependencies.html) mentions using the `dependencies` or `dependencyInsight` tasks to debug dependencies for the root project or individual subprojects.

Alternatively, use the `allDeps` or `allDepInsight` tasks for recursively iterating through all subprojects:

    ./gradlew allDeps

    ./gradlew allDepInsight --configuration runtimeClasspath --dependency com.fasterxml.jackson.core:jackson-databind

These take the same arguments as the builtin variants.

### Determining if any dependencies could be updated ###
    ./gradlew dependencyUpdates

### Common build options ###

The following options should be set with a `-P` switch, for example `./gradlew -PmaxParallelForks=1 test`.

* `commitId`: sets the build commit ID as .git/HEAD might not be correct if there are local commits added for build purposes.
* `mavenUrl`: sets the URL of the maven deployment repository (`file://path/to/repo` can be used to point to a local repository).
* `maxParallelForks`: maximum number of test processes to start in parallel. Defaults to the number of processors available to the JVM.
* `maxScalacThreads`: maximum number of worker threads for the scalac backend. Defaults to the lowest of `8` and the number of processors
available to the JVM. The value must be between 1 and 16 (inclusive). 
* `ignoreFailures`: ignore test failures from junit
* `showStandardStreams`: shows standard out and standard error of the test JVM(s) on the console.
* `skipSigning`: skips signing of artifacts.
* `testLoggingEvents`: unit test events to be logged, separated by comma. For example `./gradlew -PtestLoggingEvents=started,passed,skipped,failed test`.
* `xmlSpotBugsReport`: enable XML reports for spotBugs. This also disables HTML reports as only one can be enabled at a time.
* `maxTestRetries`: maximum number of retries for a failing test case.
* `maxTestRetryFailures`: maximum number of test failures before retrying is disabled for subsequent tests.
* `enableTestCoverage`: enables test coverage plugins and tasks, including bytecode enhancement of classes required to track said
coverage. Note that this introduces some overhead when running tests and hence why it's disabled by default (the overhead
varies, but 15-20% is a reasonable estimate).
* `keepAliveMode`: configures the keep alive mode for the Gradle compilation daemon - reuse improves start-up time. The values should 
be one of `daemon` or `session` (the default is `daemon`). `daemon` keeps the daemon alive until it's explicitly stopped while
`session` keeps it alive until the end of the build session. This currently only affects the Scala compiler, see
https://github.com/gradle/gradle/pull/21034 for a PR that attempts to do the same for the Java compiler.
* `scalaOptimizerMode`: configures the optimizing behavior of the scala compiler, the value should be one of `none`, `method`, `inline-kafka` or
`inline-scala` (the default is `inline-kafka`). `none` is the scala compiler default, which only eliminates unreachable code. `method` also
includes method-local optimizations. `inline-kafka` adds inlining of methods within the kafka packages. Finally, `inline-scala` also
includes inlining of methods within the scala library (which avoids lambda allocations for methods like `Option.exists`). `inline-scala` is
only safe if the Scala library version is the same at compile time and runtime. Since we cannot guarantee this for all cases (for example, users
may depend on the kafka jar for integration tests where they may include a scala library with a different version), we don't enable it by
default. See https://www.lightbend.com/blog/scala-inliner-optimizer for more details.

### Running system tests ###

See [tests/README.md](tests/README.md).

### Running in Vagrant ###

See [vagrant/README.md](vagrant/README.md).

### Contribution ###

Apache Kafka is interested in building the community; we would welcome any thoughts or [patches](https://issues.apache.org/jira/browse/KAFKA). You can reach us [on the Apache mailing lists](http://kafka.apache.org/contact.html).

To contribute follow the instructions here:
 * https://kafka.apache.org/contributing.html 
