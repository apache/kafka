# State stores lose state when tasks are reassigned under EOS with standby replicas and default acceptable lag.

## Goal 
The attached stream is an extension to the known deduplication example often use in Kafka.
The goal is to assign a unique identifier to each key in the input topic and pass the key and identifier to the output topic.
If a key have previously been observed then reuse the ID given last time.


### GIVEN:
The stream uses exactly once semantics.
There are 2 partitions and partition assigment is done using : key MODULO 2 + 1

ID assignment is done using a partition specific counter and the following logic:
IF
Store contains the Key
THEN
Reuse the associated ID.
ELSE
Assign ID = counter * partitionCount + context.partition()
Increment counter
Store the key and ID assigment.

### WHEN:<br/>
**Input topic:**<br/>
The stream takes as input a topic containing KeyVale<Integer,Integer> where the key and value are equal.<br/>
[{0, 0} ,{1, 1} ,{2, 2} ,{1, 1},...]

### THEN:

The Streams will observe the following:
#### Stream 0 :<br/>
**INPUT   :** [{1, 1}, {1, 1}]<br/>
**OUTPUT  :** [{1, 0}, {1, 0}]

##### Stream 1 :<br/>
**INPUT   :** [{0, 0}, {2, 2}]<br/>
**OUTPUT  :** [{0, 1}, {2, 3}]

#### Output topic:<br/>
The output of the stream is a topic in which each entry from the input topic is assigned a stable ID.<br/>
**OUTPUT :** [{0, 1} ,{1, 0} ,{2, 3} ,{1, 0},...]

#### Counter changelog topic (before compaction):<br/>
**partition 0 :** [{0 ,0}]<br/>
**partition 1 :** [{0 ,0}, {0 ,1}]

#### Store changelog topic (before compaction):<br/>
**partition 0 :** [{1, 0}]<br/>
**partition 1 :** [{0, 1}, {2, 3}]


## The bug and test case
The input and transform are both deterministic and so should the output due to the exactly-once semantics.
The test demonstrates that a re-balancing event can break the exactly-once semantics and this results in IDs being reused.
The test case with default acceptable lag and caching enabled produces duplicate IDs.
The result of an execution of the test is presented below and the logs of the failing run is attacthed:
```
Test shouldHonorEOSWhenUsingCachingAndStandbyReplicas[DEFAULT_CONFIG] FAILED (1m 53s)

java.lang.AssertionError: Each output should correspond to one distinct value
Expected: is <63000L>
     but: was <62888L>
    at com.chainalysis.enumerator.StandbyTaskEOSIntegrationTest.shouldHonorEOSWhenUsingCachingAndStandbyReplicas(StandbyTaskEOSIntegrationTest.java:246)

Test shouldHonorEOSWhenUsingCachingAndStandbyReplicas[CACHING_DISABLED] PASSED (2m 2s)
Test shouldHonorEOSWhenUsingCachingAndStandbyReplicas[NO_LAG_ACCEPTABLE] PASSED (1m 36s)
```

The issue can be reliably reproduced on an i7-8750H CPU @ 2.20GHz × 12 with 32 GiB Memory when using caching and the default acceptable lag for standby replicas.

When caching is disabled OR the acceptable lag is set to 0 then the test no longer breaks.
The underlying cause is unknown and so is unknown whether the above settings fix or hides the problem.

A similar issue have been reported here:
https://stackoverflow.com/questions/69038181/kafka-streams-aggregation-data-loss-between-instance-restarts-and-rebalances

Flow of the test case :
1) Produce 3000 messages.
2) Start a stream.
3) Wait for the 3000 messages to be processed.
4) Start a new stream and wait for it to start syncing
5) Produce 60.000 messages
6) Wait for 5 second.
7) Start a new thread which should introduce a re-balancing event.
8) Wait until the entire log is processed by the stream.
9) Check the uniqueness of the assigned IDs.
