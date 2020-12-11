### JMH-Benchmarks module

This module contains benchmarks written using [JMH](https://openjdk.java.net/projects/code-tools/jmh/) from OpenJDK.
Writing correct micro-benchmarks in Java (or another JVM language) is difficult and there are many non-obvious pitfalls (many
due to compiler optimizations). JMH is a framework for running and analyzing benchmarks (micro or macro) written in Java (or
another JVM language).

### Running benchmarks

If you want to set specific JMH flags or only run certain benchmarks, passing arguments via
gradle tasks is cumbersome. These are simplified by the provided `jmh.sh` script.

The default behavior is to run all benchmarks:

    ./jmh-benchmarks/jmh.sh
    
Pass a pattern or name after the command to select the benchmarks:

    ./jmh-benchmarks/jmh.sh LRUCacheBenchmark

Check which benchmarks that match the provided pattern:

    ./jmh-benchmarks/jmh.sh -l LRUCacheBenchmark

Run a specific test and override the number of forks, iterations and warm-up iteration to `2`:

    ./jmh-benchmarks/jmh.sh -f 2 -i 2 -wi 2 LRUCacheBenchmark

Run a specific test with async and GC profilers on Linux and flame graph output:

    ./jmh-benchmarks/jmh.sh -prof gc -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph LRUCacheBenchmark

The following sections cover async profiler and GC profilers in more detail.

### Using JMH with async profiler

It's good practice to check profiler output for microbenchmarks in order to verify that they represent the expected
application behavior and measure what you expect to measure. Some example pitfalls include the use of expensive mocks
or accidental inclusion of test setup code in the benchmarked code. JMH includes
[async-profiler](https://github.com/jvm-profiling-tools/async-profiler) integration that makes this easy:

    ./jmh-benchmarks/jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so

With flame graph output (the semicolon is escaped to ensure it is not treated as a command separator):

    ./jmh-benchmarks/jmh.sh -prof async:libPath=/path/to/libasyncProfiler.so\;output=flamegraph

A number of arguments can be passed to configure async profiler, run the following for a description:

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

   -i <int>                    Number of measurement iterations to do. Measurement
                               iterations are counted towards the benchmark score.
                               (default: 1 for SingleShotTime, and 5 for all other
                               modes)

   -l                          List the benchmarks that match a filter, and exit.

   -lprof                      List profilers, and exit.

   -o <filename>               Redirect human-readable output to a given file. 

   -prof <profiler>            Use profilers to collect additional benchmark data. 
                               Some profilers are not available on all JVMs and/or 
                               all OSes. Please see the list of available profilers 
                               with -lprof.

   -v <mode>                   Verbosity mode. Available modes are: [SILENT, NORMAL,
                               EXTRA]

   -wi <int>                   Number of warmup iterations to do. Warmup iterations
                               are not counted towards the benchmark score. (default:
                               0 for SingleShotTime, and 5 for all other modes)
```

To view all options run jmh with the -h flag. 
