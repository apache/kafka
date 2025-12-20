---
title: Hardware and OS
description: Hardware and OS
weight: 7
tags: ['kafka', 'docs']
aliases: 
keywords: 
type: docs
---

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->


We are using dual quad-core Intel Xeon machines with 24GB of memory. 

You need sufficient memory to buffer active readers and writers. You can do a back-of-the-envelope estimate of memory needs by assuming you want to be able to buffer for 30 seconds and compute your memory need as write_throughput*30. 

The disk throughput is important. We have 8x7200 rpm SATA drives. In general disk throughput is the performance bottleneck, and more disks is better. Depending on how you configure flush behavior you may or may not benefit from more expensive disks (if you force flush often then higher RPM SAS drives may be better). 

## OS

Kafka should run well on any unix system and has been tested on Linux and Solaris. 

We have seen a few issues running on Windows and Windows is not currently a well supported platform though we would be happy to change that. 

It is unlikely to require much OS-level tuning, but there are three potentially important OS-level configurations: 

  * File descriptor limits: Kafka uses file descriptors for log segments and open connections. If a broker hosts many partitions, consider that the broker needs at least (number_of_partitions)*(partition_size/segment_size) to track all log segments in addition to the number of connections the broker makes. We recommend at least 100000 allowed file descriptors for the broker processes as a starting point. Note: The mmap() function adds an extra reference to the file associated with the file descriptor fildes which is not removed by a subsequent close() on that file descriptor. This reference is removed when there are no more mappings to the file. 
  * Max socket buffer size: can be increased to enable high-performance data transfer between data centers as [described here](https://www.psc.edu/index.php/networking/641-tcp-tune). 
  * Maximum number of memory map areas a process may have (aka vm.max_map_count). [See the Linux kernel documentation](https://kernel.org/doc/Documentation/sysctl/vm.txt). You should keep an eye at this OS-level property when considering the maximum number of partitions a broker may have. By default, on a number of Linux systems, the value of vm.max_map_count is somewhere around 65535. Each log segment, allocated per partition, requires a pair of index/timeindex files, and each of these files consumes 1 map area. In other words, each log segment uses 2 map areas. Thus, each partition requires minimum 2 map areas, as long as it hosts a single log segment. That is to say, creating 50000 partitions on a broker will result allocation of 100000 map areas and likely cause broker crash with OutOfMemoryError (Map failed) on a system with default vm.max_map_count. Keep in mind that the number of log segments per partition varies depending on the segment size, load intensity, retention policy and, generally, tends to be more than one. 


## Disks and Filesystem

We recommend using multiple drives to get good throughput and not sharing the same drives used for Kafka data with application logs or other OS filesystem activity to ensure good latency. You can either RAID these drives together into a single volume or format and mount each drive as its own directory. Since Kafka has replication the redundancy provided by RAID can also be provided at the application level. This choice has several tradeoffs. 

If you configure multiple data directories partitions will be assigned round-robin to data directories. Each partition will be entirely in one of the data directories. If data is not well balanced among partitions this can lead to load imbalance between disks. 

RAID can potentially do better at balancing load between disks (although it doesn't always seem to) because it balances load at a lower level. The primary downside of RAID is that it is usually a big performance hit for write throughput and reduces the available disk space. 

Another potential benefit of RAID is the ability to tolerate disk failures. However our experience has been that rebuilding the RAID array is so I/O intensive that it effectively disables the server, so this does not provide much real availability improvement. 

## Application vs. OS Flush Management

Kafka always immediately writes all data to the filesystem and supports the ability to configure the flush policy that controls when data is forced out of the OS cache and onto disk using the flush. This flush policy can be controlled to force data to disk after a period of time or after a certain number of messages has been written. There are several choices in this configuration. 

Kafka must eventually call fsync to know that data was flushed. When recovering from a crash for any log segment not known to be fsync'd Kafka will check the integrity of each message by checking its CRC and also rebuild the accompanying offset index file as part of the recovery process executed on startup. 

Note that durability in Kafka does not require syncing data to disk, as a failed node will always recover from its replicas. 

We recommend using the default flush settings which disable application fsync entirely. This means relying on the background flush done by the OS and Kafka's own background flush. This provides the best of all worlds for most uses: no knobs to tune, great throughput and latency, and full recovery guarantees. We generally feel that the guarantees provided by replication are stronger than sync to local disk, however the paranoid still may prefer having both and application level fsync policies are still supported. 

The drawback of using application level flush settings is that it is less efficient in its disk usage pattern (it gives the OS less leeway to re-order writes) and it can introduce latency as fsync in most Linux filesystems blocks writes to the file whereas the background flushing does much more granular page-level locking. 

In general you don't need to do any low-level tuning of the filesystem, but in the next few sections we will go over some of this in case it is useful. 

## Understanding Linux OS Flush Behavior

In Linux, data written to the filesystem is maintained in [pagecache](https://en.wikipedia.org/wiki/Page_cache) until it must be written out to disk (due to an application-level fsync or the OS's own flush policy). The flushing of data is done by a set of background threads called pdflush (or in post 2.6.32 kernels "flusher threads"). 

Pdflush has a configurable policy that controls how much dirty data can be maintained in cache and for how long before it must be written back to disk. This policy is described [here](https://web.archive.org/web/20160518040713/http://www.westnet.com/~gsmith/content/linux-pdflush.htm). When Pdflush cannot keep up with the rate of data being written it will eventually cause the writing process to block incurring latency in the writes to slow down the accumulation of data. 

You can see the current state of OS memory usage by doing 
    
    
    $ cat /proc/meminfo

The meaning of these values are described in the link above. 

Using pagecache has several advantages over an in-process cache for storing data that will be written out to disk: 

  * The I/O scheduler will batch together consecutive small writes into bigger physical writes which improves throughput. 
  * The I/O scheduler will attempt to re-sequence writes to minimize movement of the disk head which improves throughput. 
  * It automatically uses all the free memory on the machine 


## Filesystem Selection

Kafka uses regular files on disk, and as such it has no hard dependency on a specific filesystem. The two filesystems which have the most usage, however, are EXT4 and XFS. Historically, EXT4 has had more usage, but recent improvements to the XFS filesystem have shown it to have better performance characteristics for Kafka's workload with no compromise in stability.

Comparison testing was performed on a cluster with significant message loads, using a variety of filesystem creation and mount options. The primary metric in Kafka that was monitored was the "Request Local Time", indicating the amount of time append operations were taking. XFS resulted in much better local times (160ms vs. 250ms+ for the best EXT4 configuration), as well as lower average wait times. The XFS performance also showed less variability in disk performance.

### General Filesystem Notes

For any filesystem used for data directories, on Linux systems, the following options are recommended to be used at mount time: 

  * noatime: This option disables updating of a file's atime (last access time) attribute when the file is read. This can eliminate a significant number of filesystem writes, especially in the case of bootstrapping consumers. Kafka does not rely on the atime attributes at all, so it is safe to disable this.



### XFS Notes

The XFS filesystem has a significant amount of auto-tuning in place, so it does not require any change in the default settings, either at filesystem creation time or at mount. The only tuning parameters worth considering are: 

  * largeio: This affects the preferred I/O size reported by the stat call. While this can allow for higher performance on larger disk writes, in practice it had minimal or no effect on performance.
  * nobarrier: For underlying devices that have battery-backed cache, this option can provide a little more performance by disabling periodic write flushes. However, if the underlying device is well-behaved, it will report to the filesystem that it does not require flushes, and this option will have no effect.



### EXT4 Notes

EXT4 is a serviceable choice of filesystem for the Kafka data directories, however getting the most performance out of it will require adjusting several mount options. In addition, these options are generally unsafe in a failure scenario, and will result in much more data loss and corruption. For a single broker failure, this is not much of a concern as the disk can be wiped and the replicas rebuilt from the cluster. In a multiple-failure scenario, such as a power outage, this can mean underlying filesystem (and therefore data) corruption that is not easily recoverable. The following options can be adjusted: 

  * data=writeback: Ext4 defaults to data=ordered which puts a strong order on some writes. Kafka does not require this ordering as it does very paranoid data recovery on all unflushed log. This setting removes the ordering constraint and seems to significantly reduce latency. 
  * Disabling journaling: Journaling is a tradeoff: it makes reboots faster after server crashes but it introduces a great deal of additional locking which adds variance to write performance. Those who don't care about reboot time and want to reduce a major source of write latency spikes can turn off journaling entirely. 
  * commit=num_secs: This tunes the frequency with which ext4 commits to its metadata journal. Setting this to a lower value reduces the loss of unflushed data during a crash. Setting this to a higher value will improve throughput. 
  * nobh: This setting controls additional ordering guarantees when using data=writeback mode. This should be safe with Kafka as we do not depend on write ordering and improves throughput and latency. 
  * delalloc: Delayed allocation means that the filesystem avoid allocating any blocks until the physical write occurs. This allows ext4 to allocate a large extent instead of smaller pages and helps ensure the data is written sequentially. This feature is great for throughput. It does seem to involve some locking in the filesystem which adds a bit of latency variance. 


## Replace KRaft Controller Disk

When Kafka is configured to use KRaft, the controllers store the cluster metadata in the directory specified in `metadata.log.dir` \-- or the first log directory, if `metadata.log.dir` is not configured. See the documentation for `metadata.log.dir` for details.

If the data in the cluster metadata directory is lost either because of hardware failure or the hardware needs to be replaced, care should be taken when provisioning the new controller node. The new controller node should not be formatted and started until the majority of the controllers have all of the committed data. To determine if the majority of the controllers have the committed data, run the `kafka-metadata-quorum.sh` tool to describe the replication status:
    
    
    $ bin/kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
    NodeId  LogEndOffset    Lag     LastFetchTimestamp      LastCaughtUpTimestamp   Status
    1       25806           0       1662500992757           1662500992757           Leader
    ...     ...             ...     ...                     ...                     ...

Check and wait until the `Lag` is small for a majority of the controllers. If the leader's end offset is not increasing, you can wait until the lag is 0 for a majority; otherwise, you can pick the latest leader end offset and wait until all replicas have reached it. Check and wait until the `LastFetchTimestamp` and `LastCaughtUpTimestamp` are close to each other for the majority of the controllers. At this point it is safer to format the controller's metadata log directory. This can be done by running the `kafka-storage.sh` command.
    
    
    $ bin/kafka-storage.sh format --cluster-id uuid --config server_properties

It is possible for the `bin/kafka-storage.sh format` command above to fail with a message like `Log directory ... is already formatted`. This can happen when combined mode is used and only the metadata log directory was lost but not the others. In that case and only in that case, can you run the `kafka-storage.sh format` command with the `--ignore-formatted` option.

Start the KRaft controller after formatting the log directories.
    
    
    $ bin/kafka-server-start.sh server_properties
