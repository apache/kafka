### JMH-Benchmarks module

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.
Writing correct micro-benchmarks in Java (or another JVM language) is difficult and there are many non-obvious pitfalls (many
due to compiler optimizations). JMH is a framework for running and analyzing benchmarks (micro or macro) written in Java (or
another JVM language).

### Running benchmarks

If you want to set specific JMH flags or only run certain benchmarks, passing arguments via
gradle tasks is cumbersome. These are simplified by the provided `jmh.sh` script.

You can run all the benchmarks using:

    ./jmh-benchmarks/jmh.sh
    
Pass a pattern or name after the command to select the benchmarks, for example:

    ./jmh-benchmarks/jmh.sh LRUCacheBenchmark

Run a specific test setting fork-mode (number iterations) to 2:

    ./jmh-benchmarks/jmh.sh -f 2 LRUCacheBenchmark

All JMH output goes to stdout by default, to run a benchmark and capture the results in a file:

    ./jmh-benchmarks/jmh.sh -f 2 -o benchmarkResults.txt LRUCacheBenchmark

### Using JMH with async-profiler

It's good practice to check profiler output for microbenchmarks in order to verify that they are valid.
JMH includes [async-profiler](https://github.com/jvm-profiling-tools/async-profiler) integration that makes this easy,
although the specifics vary slightly depending on the platform. A simple Linux example:

    LD_LIBRARY_PATH=/path/to/async-profiler ./jmh-benchmarks/jmh.sh -prof async

`DYLD_LIBRARY_PATH` should be used on macOS instead:

    DYLD_LIBRARY_PATH=/path/to/async-profiler ./jmh-benchmarks/jmh.sh -prof async 

A number of arguments can be passed to async-profiler, run the following for a description:

    ./jmh-benchmarks/jmh.sh -prof async:help
 
A number of arguments can be passed to async-profiler, run the following for a description: 

    ./jmh-benchmarks/jmh.sh -prof async:help

### Using JMH GC profiler

It's good practice to run your benchmark with `-prof gc` to measure its allocation rate:

      ./jmh-benchmarks/jmh.sh -prof gc

Of particular importance is the `norm` alloc rates, which measure the allocations per operation rather than allocations
per second which can increase when you have make your code faster.

### Running JMH outside of gradle

The JMH benchmarks can be run outside of gradle as you would with any executable jar file:

    java -jar <kafka-repo-dir>/jmh-benchmarks/build/libs/kafka-jmh-benchmarks-all.jar -f2 LRUCacheBenchmark

### Writing benchmarks

For help in writing correct JMH tests, the best place to start is the [sample code](https://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/) provided
by the JMH project.

Typically, JMH is expected to run as a separate project in Maven. The jmh-benchmarks module uses
the [gradle shadow jar](https://github.com/johnrengelman/shadow) plugin to emulate this behavior, by creating the required
uber-jar file containing the benchmarking code and required JMH classes.  

JMH is highly configurable and users are encouraged to look through the samples for suggestions
on what options are available. A good tutorial for using JMH can be found [here](http://tutorials.jenkov.com/java-performance/jmh.html#return-value-from-benchmark-method)

### Gradle Tasks

If no benchmark mode is specified, the default is used which is throughput. It is assumed that users run
the gradle tasks with `./gradlew` from the root of the Kafka project.

* `jmh-benchmarks:shadowJar` - creates the uber jar required to run the benchmarks.

* `jmh-benchmarks:jmh` - runs the `clean` and `shadowJar` tasks followed by all the benchmarks.

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

   -prof <profiler>            Use profilers to collect additional benchmark data. 
                               Some profilers are not available on all JVMs and/or 
                               all OSes. Please see the list of available profilers 
                               with -lprof. 
```

To view all options run jmh with the -h flag. 
