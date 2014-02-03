package kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import kafka.clients.producer.internals.FutureRecordMetadata;
import kafka.clients.producer.internals.Metadata;
import kafka.clients.producer.internals.Partitioner;
import kafka.clients.producer.internals.RecordAccumulator;
import kafka.clients.producer.internals.Sender;
import kafka.common.Cluster;
import kafka.common.KafkaException;
import kafka.common.Metric;
import kafka.common.PartitionInfo;
import kafka.common.TopicPartition;
import kafka.common.config.ConfigException;
import kafka.common.errors.RecordTooLargeException;
import kafka.common.metrics.JmxReporter;
import kafka.common.metrics.MetricConfig;
import kafka.common.metrics.Metrics;
import kafka.common.metrics.MetricsReporter;
import kafka.common.network.Selector;
import kafka.common.record.CompressionType;
import kafka.common.record.Record;
import kafka.common.record.Records;
import kafka.common.utils.KafkaThread;
import kafka.common.utils.SystemTime;

/**
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and should generally be shared among all threads for best performance.
 * <p>
 * The producer manages a single background thread that does I/O as well as a TCP connection to each of the brokers it
 * needs to communicate with. Failure to close the producer after use will leak these resources.
 */
public class KafkaProducer implements Producer {

    private final Partitioner partitioner;
    private final int maxRequestSize;
    private final long metadataFetchTimeoutMs;
    private final long totalMemorySize;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Metrics metrics;
    private final Thread ioThread;

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs));
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties));
    }

    private KafkaProducer(ProducerConfig config) {
        this.metrics = new Metrics(new MetricConfig(),
                                   Collections.singletonList((MetricsReporter) new JmxReporter("kafka.producer.")),
                                   new SystemTime());
        this.partitioner = new Partitioner();
        this.metadataFetchTimeoutMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
        this.metadata = new Metadata();
        this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        this.totalMemorySize = config.getLong(ProducerConfig.TOTAL_BUFFER_MEMORY_CONFIG);
        this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.MAX_PARTITION_SIZE_CONFIG),
                                                 this.totalMemorySize,
                                                 config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                                                 config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL),
                                                 metrics,
                                                 new SystemTime());
        List<InetSocketAddress> addresses = parseAndValidateAddresses(config.getList(ProducerConfig.BROKER_LIST_CONFIG));
        this.metadata.update(Cluster.bootstrap(addresses), System.currentTimeMillis());
        this.sender = new Sender(new Selector(),
                                 this.metadata,
                                 this.accumulator,
                                 config.getString(ProducerConfig.CLIENT_ID_CONFIG),
                                 config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                                 config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                                 (short) config.getInt(ProducerConfig.REQUIRED_ACKS_CONFIG),
                                 config.getInt(ProducerConfig.REQUEST_TIMEOUT_CONFIG),
                                 new SystemTime());
        this.ioThread = new KafkaThread("kafka-network-thread", this.sender, true);
        this.ioThread.start();
    }

    private static List<InetSocketAddress> parseAndValidateAddresses(List<String> urls) {
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        for (String url : urls) {
            if (url != null && url.length() > 0) {
                String[] pieces = url.split(":");
                if (pieces.length != 2)
                    throw new ConfigException("Invalid url in metadata.broker.list: " + url);
                try {
                    InetSocketAddress address = new InetSocketAddress(pieces[0], Integer.parseInt(pieces[1]));
                    if (address.isUnresolved())
                        throw new ConfigException("DNS resolution failed for metadata bootstrap url: " + url);
                    addresses.add(address);
                } catch (NumberFormatException e) {
                    throw new ConfigException("Invalid port in metadata.broker.list: " + url);
                }
            }
        }
        if (addresses.size() < 1)
            throw new ConfigException("No bootstrap urls given in metadata.broker.list.");
        return addresses;
    }

    /**
     * Asynchronously send a record to a topic. Equivalent to {@link #send(ProducerRecord, Callback) send(record, null)}
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to and the offset
     * it was assigned.
     * <p>
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will result in the metadata for the record or throw any exception that occurred while
     * sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can do the following:
     * 
     * <pre>
     *   producer.send(new ProducerRecord("the-topic", "key, "value")).get();
     * </pre>
     * <p>
     * Those desiring fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     * 
     * <pre>
     *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
     *   producer.send(myRecord,
     *                 new Callback() {
     *                     public void onCompletion(RecordMetadata metadata, Exception e) {
     *                         if(e != null)
     *                             e.printStackTrace();
     *                         System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                     }
     *                 });
     * </pre>
     * 
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     * 
     * <pre>
     * producer.send(new ProducerRecord(topic, partition, key, value), callback1);
     * producer.send(new ProducerRecord(topic, partition, key2, value2), callback2);
     * </pre>
     * <p>
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     * <p>
     * The producer manages a buffer of records waiting to be sent. This buffer has a hard limit on it's size, which is
     * controlled by the configuration <code>total.memory.bytes</code>. If <code>send()</code> is called faster than the
     * I/O thread can transfer data to the brokers the buffer will eventually run out of space. The default behavior in
     * this case is to block the send call until the I/O thread catches up and more buffer space is available. However
     * in cases where non-blocking usage is desired the setting <code>block.on.buffer.full=false</code> will cause the
     * producer to instead throw an exception when buffer memory is exhausted.
     * 
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback) {
        try {
            Cluster cluster = metadata.fetch(record.topic(), this.metadataFetchTimeoutMs);
            int partition = partitioner.partition(record, cluster);
            ensureValidSize(record.key(), record.value());
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            FutureRecordMetadata future = accumulator.append(tp, record.key(), record.value(), CompressionType.NONE, callback);
            this.sender.wakeup();
            return future;
        } catch (Exception e) {
            if (callback != null)
                callback.onCompletion(null, e);
            return new FutureFailure(e);
        }
    }

    /**
     * Check that this key-value pair will have a serialized size small enough
     */
    private void ensureValidSize(byte[] key, byte[] value) {
        int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
        if (serializedSize > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + serializedSize
                                              + " bytes when serialized which is larger than the maximum request size you have configured with the "
                                              + ProducerConfig.MAX_REQUEST_SIZE_CONFIG
                                              + " configuration.");
        if (serializedSize > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + serializedSize
                                              + " bytes when serialized which is larger than the total memory buffer you have configured with the "
                                              + ProducerConfig.TOTAL_BUFFER_MEMORY_CONFIG
                                              + " configuration.");
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return this.metadata.fetch(topic, this.metadataFetchTimeoutMs).partitionsFor(topic);
    }

    @Override
    public Map<String, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all in-flight requests complete.
     */
    @Override
    public void close() {
        this.sender.initiateClose();
        try {
            this.ioThread.join();
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
        this.metrics.close();
    }

    private static class FutureFailure implements Future<RecordMetadata> {

        private final ExecutionException exception;

        public FutureFailure(Exception exception) {
            this.exception = new ExecutionException(exception);
        }

        @Override
        public boolean cancel(boolean interrupt) {
            return false;
        }

        @Override
        public RecordMetadata get() throws ExecutionException {
            throw this.exception;
        }

        @Override
        public RecordMetadata get(long timeout, TimeUnit unit) throws ExecutionException {
            throw this.exception;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

    }

}
