package kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.clients.producer.internals.Metadata;
import kafka.clients.producer.internals.RecordAccumulator;
import kafka.clients.producer.internals.Sender;
import kafka.common.Cluster;
import kafka.common.KafkaException;
import kafka.common.Metric;
import kafka.common.Serializer;
import kafka.common.TopicPartition;
import kafka.common.config.ConfigException;
import kafka.common.errors.MessageTooLargeException;
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
 * A Kafka producer that can be used to send data to the Kafka cluster.
 * <P>
 * The producer is <i>thread safe</i> and should generally be shared among all threads for best performance.
 * <p>
 * The producer manages a single background thread that does I/O as well as a TCP connection to each of the brokers it
 * needs to communicate with. Failure to close the producer after use will leak these.
 */
public class KafkaProducer implements Producer {

    private final int maxRequestSize;
    private final long metadataFetchTimeoutMs;
    private final long totalMemorySize;
    private final Partitioner partitioner;
    private final Metadata metadata;
    private final RecordAccumulator accumulator;
    private final Sender sender;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
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
        this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serializer.class);
        this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
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
    public RecordSend send(ProducerRecord record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * The send is asynchronous and this method will return immediately once the record has been serialized and stored
     * in the buffer of messages waiting to be sent. This allows sending many records in parallel without necessitating
     * blocking to wait for the response after each one.
     * <p>
     * The {@link RecordSend} returned by this call will hold the future response data including the offset assigned to
     * the message and the error (if any) when the request has completed (or returned an error), and this object can be
     * used to block awaiting the response. If you want the equivalent of a simple blocking send you can easily achieve
     * that using the {@link kafka.clients.producer.RecordSend#await() await()} method on the {@link RecordSend} this
     * call returns:
     * 
     * <pre>
     *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
     *   producer.send(myRecord, null).await();
     * </pre>
     * 
     * Note that the send method will not throw an exception if the request fails while communicating with the cluster,
     * rather that exception will be thrown when accessing the {@link RecordSend} that is returned.
     * <p>
     * Those desiring fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete. Note that the callback will execute in the I/O thread of the
     * producer and so should be reasonably fast. An example usage of an inline callback would be the following:
     * 
     * <pre>
     *   ProducerRecord record = new ProducerRecord("the-topic", "key, "value");
     *   producer.send(myRecord,
     *                 new Callback() {
     *                     public void onCompletion(RecordSend send) {
     *                         try {
     *                             System.out.println("The offset of the message we just sent is: " + send.offset());
     *                         } catch(KafkaException e) {
     *                             e.printStackTrace();
     *                         }
     *                     }
     *                 });
     * </pre>
     * <p>
     * This call enqueues the message in the buffer of outgoing messages to be sent. This buffer has a hard limit on
     * it's size controlled by the configuration <code>total.memory.bytes</code>. If <code>send()</code> is called
     * faster than the I/O thread can send data to the brokers we will eventually run out of buffer space. The default
     * behavior in this case is to block the send call until the I/O thread catches up and more buffer space is
     * available. However if non-blocking usage is desired the setting <code>block.on.buffer.full=false</code> will
     * cause the producer to instead throw an exception when this occurs.
     * 
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     * @throws BufferExhausedException This exception is thrown if the buffer is full and blocking has been disabled.
     * @throws MessageTooLargeException This exception is thrown if the serialized size of the message is larger than
     *         the maximum buffer memory or maximum request size that has been configured (whichever is smaller).
     */
    @Override
    public RecordSend send(ProducerRecord record, Callback callback) {
        Cluster cluster = metadata.fetch(record.topic(), this.metadataFetchTimeoutMs);
        byte[] key = keySerializer.toBytes(record.key());
        byte[] value = valueSerializer.toBytes(record.value());
        byte[] partitionKey = keySerializer.toBytes(record.partitionKey());
        int partition = partitioner.partition(record, key, partitionKey, value, cluster, cluster.partitionsFor(record.topic()).size());
        ensureValidSize(key, value);
        try {
            TopicPartition tp = new TopicPartition(record.topic(), partition);
            RecordSend send = accumulator.append(tp, key, value, CompressionType.NONE, callback);
            this.sender.wakeup();
            return send;
        } catch (InterruptedException e) {
            throw new KafkaException(e);
        }
    }

    /**
     * Check that this key-value pair will have a serialized size small enough
     */
    private void ensureValidSize(byte[] key, byte[] value) {
        int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(key, value);
        if (serializedSize > this.maxRequestSize)
            throw new MessageTooLargeException("The message is " + serializedSize
                                               + " bytes when serialized which is larger than the maximum request size you have configured with the "
                                               + ProducerConfig.MAX_REQUEST_SIZE_CONFIG
                                               + " configuration.");
        if (serializedSize > this.totalMemorySize)
            throw new MessageTooLargeException("The message is " + serializedSize
                                               + " bytes when serialized which is larger than the total memory buffer you have configured with the "
                                               + ProducerConfig.TOTAL_BUFFER_MEMORY_CONFIG
                                               + " configuration.");
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

}
