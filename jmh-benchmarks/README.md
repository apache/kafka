### JMH-Benchmark module

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.
Writing correct micro-benchmarks in Java (or another JVM language) is difficult and there are many non-obvious pitfalls (many
due to compiler optimizations). JMH is a framework for running and analyzing benchmarks (micro or macro) written in Java (or
another JVM language).

For help in writing correct JMH tests, the best place to start is the [sample code](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/) provided
by the JMH project.

Typically, JMH is expected to run as a separate project in Maven. The jmh-benchmarks module uses
the [gradle shadow jar](https://github.com/johnrengelman/shadow) plugin to emulate this behavior, by creating the required
uber-jar file containing the benchmarking code and required JMH classes.  

JMH is highly configurable and users are encouraged to look through the samples for suggestions
on what options are available. A good tutorial for using JMH can be found [here](http://tutorials.jenkov.com/java-performance/jmh.html#return-value-from-benchmark-method)

### Gradle Tasks / Running benchmarks in gradle

If no benchmark mode is specified, the default is used which is throughput. It is assumed that users run
the gradle tasks with './gradlew' from the root of the Kafka project.

*  jmh-benchmarks:shadowJar - creates the uber jar required to run the benchmarks.

*  jmh-benchmarks:jmh - runs the `clean` and `shadowJar` tasks followed by all the benchmarks.
 
### Using the jmh script
If you want to set specific JMH flags or only run a certain test(s) passing arguments via
gradle tasks is cumbersome.  Instead you can use the `jhm.sh` script.  NOTE: It is assumed users run
the jmh.sh script from the jmh-benchmarks module.

* Run a specific test setting fork-mode (number iterations) to 2 :`./jmh.sh -f 2 LRUCacheBenchmark`

* By default all JMH output goes to stdout.  To run a benchmark and capture the results in a file:
`./jmh.sh -f 2 -o benchmarkResults.txt LRUCacheBenchmark`
NOTE: For now this script needs to be run from the jmh-benchmarks directory.
 
### Running JMH outside of gradle
The JMH benchmarks can be run outside of gradle as you would with any executable jar file:
`java -jar <kafka-repo-dir>/jmh-benchmarks/build/libs/kafka-jmh-benchmarks-all.jar -f2 LRUCacheBenchmark`

### JMH Options
Some common JMH options are:
```text
 
   -e <regexp+>                Benchmarks to exclude from the run. 
 
   -f <int>                    How many times to fork a single benchmark. Use 0 to 
                               disable forking altogether. Warning: disabling 
                               forking may have detrimental impact on benchmark 
                               and infrastructure reliability, you might want 
                               to use different warmup mode instead. 
 
   -o <filename>               Redirect human-readable output to a given file. 
 
  
 
   -v <mode>                   Verbosity mode. Available modes are: [SILENT, NORMAL, 
                               EXTRA] 
```
To view all options run jmh with the -h flag. 
