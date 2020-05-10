/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 此类充当其累积记录到一个队列MemoryRecords实例被发送到服务器。
 * 累加器使用的内存和附加呼叫量有限时内存耗尽，除非这种行为是明确禁用会阻止。
 * This class acts as a queue that accumulates records into {@link org.apache.kafka.common.record.MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);
    //是否已经关闭
    private volatile boolean closed;
    //刷新进行中
    private final AtomicInteger flushesInProgress;
    //累加进行中
    private final AtomicInteger appendsInProgress;
    //每个recordBatch底层ByteBuffer的大小
    private final int batchSize;
    //压缩类型
    private final CompressionType compression;
    //超过lingerMs时发送给sender线程
    private final long lingerMs;
    //每次重试需要等到的时间
    private final long retryBackoffMs;
    //可用的bytebuffer缓冲池
    private final BufferPool free;
    private final Time time;
    //TopicPartition和RecordBatch集合的映射，类型是CopyOnWriteMap，线程安全的，ArrayDeque是线程不安全的
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    //未发送完成的RecordBatch集合
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    // 以下变量仅由发送方线程访问，因此我们不需要保护它们。发送中批次的分区
    private final Set<TopicPartition> muted;
    //“使用drain方法批量导出RecordBatch时，为了防止饥饿，使用drainIndex记录上次发送停止时的位置，下次继续从此位置开始发送。”
    private int drainIndex;

    /**
     * Create a new record accumulator
     *
     * @param batchSize      The size to use when allocating {@link org.apache.kafka.common.record.MemoryRecords} instances
     * @param totalSize      The maximum memory the record accumulator can use.
     * @param compression    The compression codec for the records
     * @param lingerMs       An artificial delay time to add before declaring a records instance that isn't full ready for
     *                       sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *                       latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *                       exhausting all retries in a short period of time.
     * @param metrics        The metrics
     * @param time           The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        //指标名称
        String metricGrpName = "producer-metrics";
        //甚于内存大小
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        //不完整批记录
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        //注册指标
        registerMetrics(metrics, metricGrpName);
    }

    /**
     * 注册指标
     *
     * @param metrics
     * @param metricGrpName
     */
    private void registerMetrics(Metrics metrics, String metricGrpName) {
        //等待线程指标
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        //得到free内任务个数
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        //添加进指标
        metrics.addMetric(metricName, waitingThreads);

        //缓存总大小
        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        //得到BufferPool中的总大小
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        //可用内存
        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        //内存交换记录
        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * 添加一个结果到记录收集器里
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp             The topic/partition to which this record is being sent
     * @param timestamp      The timestamp of the record
     * @param key            The key for the record
     * @param value          The value for the record
     * @param callback       The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        //正在累加的计数器+1
        appendsInProgress.incrementAndGet();
        try {
            //根据TopicPartition去拿到对应的RecordBatch，如果不存在则新建
            // check if we have an in-progress batch
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            //对dq枷锁
            /**
             * 这里为什么使用两个synchronized代码块？
             * 主要是因为在向BufferPool申请新ByteBuffer的时候，可能会导致阻塞。我们假设在一个synchronized块中完成上面所有追加操作，
             * 有下面的场景：线程1发送的消息比较大，需要向BufferPool申请新空间，而此时BufferPool空间不足，线程1在BufferPool上等待，此时它依然持有对应Deque的锁；
             * 线程2发送的消息较小，Deque最后一个RecordBatch剩余空间够用，但是由于线程1未释放Deque的锁，所以也需要一起等待。
             * 若线程2这样的线程较多，就会造成很多不必要的线程阻塞，降低了吞吐量。这里体现了“减少锁的持有时间”这一优化手段
             */
            synchronized (dq) {
                //如果producer已经关闭
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                //调用tryAppend方法
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                //如果不为nulkl返回累加结果
                if (appendResult != null)
                    return appendResult;
            }
            //如果dq没有RecordBatch

            //得到最大的bytebuffer大小
            // we don't have an in-progress record batch try to allocate a new batch
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            //从bytebuffer池中申请复用新的ByteBuffer
            /**
             * 防止内存碎片，线程1发现最后一个RecordBatch空间不够用，申请空间并创建一个新RecordBatch对象添加到Deque的尾部；
             * 线程2与线程1并发执行，也将新创建一个RecordBatch添加到Deque尾部。从上面的逻辑中我们可以得知，之后的追加操作只会在Deque尾部进行，这样就会出现下图的场景，RecordBatch4不再被使用，这就出现了内部碎片。
             *
             */
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                //二次校验
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                //如果可以申请到RecordBatch，则释放ByteBuffer
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    free.deallocate(buffer);
                    return appendResult;
                }
                //如果仍然没有申请到RecordBatch，则去创建一个buffer大小的MemoryRecords
                MemoryRecords records = MemoryRecords.emptyRecords(buffer, compression, this.batchSize);
                //新建RecordBatch
                RecordBatch batch = new RecordBatch(tp, records, time.milliseconds());
                //累加到batch其中
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));
                //添加到dq中
                dq.addLast(batch);
                //并且将batch放入未发送完成的RecordBatch集合
                incomplete.add(batch);
                //创建新的RecordAppendResult
                return new RecordAppendResult(future, dq.size() > 1 || batch.records.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     * 如果`RecordBatch.tryAppend`失败（即记录批次已满），靠近它的内存记录，以释放临时资源（如压缩流缓冲）。
     */
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        //从尾部拿到RecordBatch
        RecordBatch last = deque.peekLast();
        if (last != null) {
            //
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            //lat RecordBatch已经写满
            if (future == null)
                //关闭MemoryRecords
                last.records.close();
            else
                //返回一个新的记录
                return new RecordAppendResult(future, deque.size() > 1 || last.records.isFull(), false);
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.records.isFull();
                        // check if the batch is expired
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            expiredBatches.add(batch);
                            count++;
                            batchIterator.remove();
                            deallocate(batch);
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", count);

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        batch.attempts++;
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            deque.addFirst(batch);
        }
    }

    /**
     * 获得所有分区准备发送的节点集合，以及所有不能发送的分区准备的最早时间。
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     * {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     * is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     * <li>The record set is full</li>
     * <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     * <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     * are immediately considered ready).</li>
     * <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        //就绪节点
        Set<Node> readyNodes = new HashSet<>();
        //下一次准备校验延迟
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        //是否未知leader副本存在
        boolean unknownLeadersExist = false;
        //是否有阻塞的扩容线程
        boolean exhausted = this.free.queued() > 0;
        //便利全部批量数据
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();
            //得到leader节点
            Node leader = cluster.leaderFor(part);
            //如果为null，表示不知道leader副本的存在
            if (leader == null) {
                unknownLeadersExist = true;
                //准备就绪的节点不包含leader节点并且muted不包含当前part
            } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                synchronized (deque) {
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        //重试大于0并且最后重试时间+配置的重试间隔大于当前时间则还可以重试
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        //计算等待时间
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        //如果还可以重试则timeToWaitMs为重试间隔否则为最大batch发送时间，如果一直未达到batch。size大小则发送
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        //计算重试时间或lingerMs是否大于等待时间
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        //如果RecordBatch大于1或者当前批次的记录以及满
                        boolean full = deque.size() > 1 || batch.records.isFull();
                        //是否否起，retryBackoffMs或lingerMs大于等于等待时间
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        //是否发送，如果满、过期、有线程阻塞等待bufferPool内存，生产者关闭，有需要flush的数据
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        //如果需要发送并且不需要重试，则将leader节点添加到就绪节点，否则计算下次校验就绪的时间
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }
        //返回就绪节点检查结果
        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeadersExist);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }


    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     *
     * @param cluster The current cluster metadata 当前记录元数据
     * @param nodes   The list of node to drain 需要调用的节点集合
     * @param maxSize The maximum number of bytes to drain 最大请求大小
     * @param now     The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();
        //转换后的数据，key nodeid value 发送的数据
        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        //遍历就绪节点
        for (Node node : nodes) {
            int size = 0;
            //根据node id得到分区相关信息集合
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            //就绪发送数据
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            //使饥饿的可能性降低，该循环不会从0开始
            //drainIndex 是batches的下标，记录上次发送停止时的位置，下次继续从此位置开始发送
            int start = drainIndex = drainIndex % parts.size();
            do {
                PartitionInfo part = parts.get(drainIndex);
                //创建TopicPartition
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches. 只处理这个分区没有在发送中批次
                if (!muted.contains(tp)) {
                    //获得发往该分区的RecordBatch
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            //得到第一个RecordBatch
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                //是否还需要重试
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                //如果不需要重试发送
                                if (!backoff) {
                                    //size+bytebuffer的大小如果大于最大大小并且就绪不为空
                                    if (size + first.records.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        //数据量已满，结束循环，一般是一个请求的大小
                                        break;
                                    } else {
                                        RecordBatch batch = deque.pollFirst();
                                        //“关闭Compressor及底层输出流，并将MemoryRecords设置为只读”
                                        batch.records.close();
                                        //计算size大小
                                        size += batch.records.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                //更新drainIndex
                this.drainIndex = (this.drainIndex + 1) % parts.size();
                //如果start！=drainIndex
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        //根据分区得到对应批记录队列
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        //如果不存在则添加
        d = new ArrayDeque<>();
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        //这里防止锁竞争问题
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        incomplete.remove(batch);
        free.deallocate(batch.records.buffer(), batch.records.initialCapacity());
    }

    /**
     * Are there any threads currently waiting on a flush?
     * <p>
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }

    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.records.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final boolean unknownLeadersExist;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, boolean unknownLeadersExist) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeadersExist = unknownLeadersExist;
        }
    }

    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }

        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }

        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }

        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
