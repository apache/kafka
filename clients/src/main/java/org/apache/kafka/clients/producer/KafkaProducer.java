/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.clients.producer.internals.RecordAccumulator;
import org.apache.kafka.clients.producer.internals.Sender;
import org.apache.kafka.clients.producer.internals.ProducerInterceptors;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.network.Selector;
import org.apache.kafka.common.network.ChannelBuilder;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 一个kafka客户端，推送记录到kafka集群
 * A Kafka client that publishes records to the Kafka cluster.
 * <P>
 *     生产者是线程安全的 ，跨线程共享一个生产实例通常会比有多个实例更快。
 * The producer is <i>thread safe</i> and sharing a single producer instance across threads will generally be faster than
 * having multiple instances.
 * <p>
 * Here is a simple example of using the producer to send records with strings containing sequential numbers as the key/value
 * pairs.
 * <pre>
 * {@code
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("acks", "all"); 知道全部同步副本都返回ack才会认为是"commit"状态
 * props.put("retries", 0);
 * props.put("batch.size", 16384); 发往同一个分区的批次超过该大小发送
 * props.put("linger.ms", 1); 如果超过1ms没有达到batch.size也会发送这一批次消息
 * props.put("buffer.memory", 33554432); 缓冲区大小，生产者用它来缓冲发送服务器的消息
 * props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 * props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
 *
 * Producer<String, String> producer = new KafkaProducer<>(props);
 * for(int i = 0; i < 100; i++)
 *     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));
 *
 * producer.close();
 * }</pre>
 * <p>
 *
 * 生产者包括缓冲空间池认为，尚未传输到服务器，以及是负责将这些记录到请求，并将它们发送到集群后台I / O线程记录。 如果无法关闭生产者使用后会泄露这些资源。
 * 所述send()方法是异步的。 当把它称为添加记录的未决的记录发送，并立即返回一个缓冲。 这使得生产商批量在一起的单记录效率。
 * 该acks配置控制下，其请求被认为是完整的标准。 “全部”我们已指定将导致阻塞的全部设置提交该记录时，最慢，但最耐用的设置。
 * 如果请求失败，生产者可以自动重试，但因为我们已经指定retries为0，它不会。 启用重也开辟了重复的可能性（见文档的消息传递语义 的详细信息）。
 * 生产者会维护每个分区未发送的记录缓冲区。 这些缓冲区是由指定大小的batch.size配置。 使这个较大的可导致更配料，但需要更多的存储器（因为我们通常将具有这些缓冲液对每个活动分区中的一个）。
 * 默认情况下，缓冲区可立即发送即使在缓冲额外的未使用的空间。 但是，如果你想减少请求的数量，你可以设置linger.ms的东西大于0这将指示生产者需要等待这个数字的毫秒希望发送请求之前，更多的记录将到达填补同一批次。 这类似于内格尔在TCP算法。 例如，在上面的代码片断，可能所有的100个记录将在单个请求被发送，因为我们确立了逗留时间为1毫秒。 然而，这将设置1毫秒的延迟添加到我们的请求，等待更多的记录，如果我们没有填满缓冲到达。 需要注意的是，在时间上紧密到达记录将通常批次甚至连同linger.ms=0这样在重负载配料将不管绕梁配置的发生; 然而，这设置为一些大于0会导致在延迟量小的成本更少的，更有效的请求时，最大载荷不之下。
 * 所述buffer.memory控制的存储器提供给生产者用于缓冲的总量。 如果记录发送速度比它们可以被发送到服务器，那么该缓冲区空间将被耗尽。 当缓冲空间耗尽其它发送调用将被阻塞。 对于时间块中的阈值由下式确定max.block.ms之后将其抛出一个TimeoutException。
 * 该key.serializer和value.serializer请示如何打开键和值对象的用户与他们提供ProducerRecord成字节。 您可以使用附带的org.apache.kafka.common.serialization.ByteArraySerializer或org.apache.kafka.common.serialization.StringSerializer简单的字符串或字节类型。
 *
 * The producer consists of a pool of buffer space that holds records that haven't yet been transmitted to the server
 * as well as a background I/O thread that is responsible for turning these records into requests and transmitting them
 * to the cluster. Failure to close the producer after use will leak these resources.
 * <p>
 * The {@link #send(ProducerRecord) send()} method is asynchronous. When called it adds the record to a buffer of pending record sends
 * and immediately returns. This allows the producer to batch together individual records for efficiency.
 * <p>
 * The <code>acks</code> config controls the criteria under which requests are considered complete. The "all" setting
 * we have specified will result in blocking on the full commit of the record, the slowest but most durable setting.
 * <p>
 * If the request fails, the producer can automatically retry, though since we have specified <code>retries</code>
 * as 0 it won't. Enabling retries also opens up the possibility of duplicates (see the documentation on
 * <a href="http://kafka.apache.org/documentation.html#semantics">message delivery semantics</a> for details).
 * <p>
 * The producer maintains buffers of unsent records for each partition. These buffers are of a size specified by
 * the <code>batch.size</code> config. Making this larger can result in more batching, but requires more memory (since we will
 * generally have one of these buffers for each active partition).
 * <p>
 * By default a buffer is available to send immediately even if there is additional unused space in the buffer. However if you
 * want to reduce the number of requests you can set <code>linger.ms</code> to something greater than 0. This will
 * instruct the producer to wait up to that number of milliseconds before sending a request in hope that more records will
 * arrive to fill up the same batch. This is analogous to Nagle's algorithm in TCP. For example, in the code snippet above,
 * likely all 100 records would be sent in a single request since we set our linger time to 1 millisecond. However this setting
 * would add 1 millisecond of latency to our request waiting for more records to arrive if we didn't fill up the buffer. Note that
 * records that arrive close together in time will generally batch together even with <code>linger.ms=0</code> so under heavy load
 * batching will occur regardless of the linger configuration; however setting this to something larger than 0 can lead to fewer, more
 * efficient requests when not under maximal load at the cost of a small amount of latency.
 * <p>
 * The <code>buffer.memory</code> controls the total amount of memory available to the producer for buffering. If records
 * are sent faster than they can be transmitted to the server then this buffer space will be exhausted. When the buffer space is
 * exhausted additional send calls will block. The threshold for time to block is determined by <code>max.block.ms</code> after which it throws
 * a TimeoutException.
 * <p>
 * The <code>key.serializer</code> and <code>value.serializer</code> instruct how to turn the key and value objects the user provides with
 * their <code>ProducerRecord</code> into bytes. You can use the included {@link org.apache.kafka.common.serialization.ByteArraySerializer} or
 * {@link org.apache.kafka.common.serialization.StringSerializer} for simple string or byte types.
 */
public class KafkaProducer<K, V> implements Producer<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    //生产者客户端ID序号，如果没有指定client.id就根据这个计数器生成对应的client.id
    private static final AtomicInteger PRODUCER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
    //JMX前缀
    private static final String JMX_PREFIX = "kafka.producer";

    //客户端id
    private String clientId;
    //分区器,根据一定的策略，将消息路由到合适的分区。
    private final Partitioner partitioner;
    //消息的最大长度，这个长度包含消息头、序列化后的key和value的长度
    private final int maxRequestSize;
    //发送单个消息的缓冲区大小
    private final long totalMemorySize;
    //元数据
    private final Metadata metadata;
    //用于收集并缓存消息，等到Sender线程发送
    private final RecordAccumulator accumulator;
    //发送消息的sender任务，在IoThread中执行
    private final Sender sender;
    //指标
    private final Metrics metrics;
    //指定sender任务发送消息的线程，称为"Sender线程"
    private final Thread ioThread;
    //压缩类型“可选项有none、gzip、 snappy、lz4。这是针对RecordAccumulator中多条消息进行的压缩，所以消息越多，压缩效果越好。”
    private final CompressionType compressionType;
    //收集错误指标
    private final Sensor errors;
    //请求耗时
    private final Time time;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    //生产者配置
    private final ProducerConfig producerConfig;
    //指定了在调用send()方法或使用partitionFor()方法获取元数据时生产者的阻塞时间。
    //当生产者的发送缓冲区已满，或者没有可用的元数据时，这些方法就会阻塞。在阻塞时间达到max.block.ms时，生产者会抛出异常。
    private final long maxBlockTimeMs;
    //消息的超时事件，也就是从消息发送到收到ACK响应的最长时长
    private final int requestTimeoutMs;
    //“ProducerInterceptor集合，ProducerInterceptor可以在消息发送之前对其进行拦截或修改；
    // 也可以先于用户的Callback，对ACK响应进行预处理。”
    private final ProducerInterceptors<K, V> interceptors;

    /**
     * 一个生产者生产者是通过提供一组键-值对作为配置的实例化。 有效配置串都记录在这里  。 值可以是字符串或相应的类型的对象（例如一数字配置将接受任一字符串“42”或整数42）
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>. Values can be
     * either strings or Objects of the appropriate type (for example a numeric configuration would accept either the
     * string "42" or the integer 42).
     * @param configs   The producer configs
     *
     */
    public KafkaProducer(Map<String, Object> configs) {
        this(new ProducerConfig(configs), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * Values can be either strings or Objects of the appropriate type (for example a numeric configuration would accept
     * either the string "42" or the integer 42).
     * @param configs   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(configs, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration. Valid configuration strings
     * are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     */
    public KafkaProducer(Properties properties) {
        this(new ProducerConfig(properties), null, null);
    }

    /**
     * A producer is instantiated by providing a set of key-value pairs as configuration, a key and a value {@link Serializer}.
     * Valid configuration strings are documented <a href="http://kafka.apache.org/documentation.html#producerconfigs">here</a>.
     * @param properties   The producer configs
     * @param keySerializer  The serializer for key that implements {@link Serializer}. The configure() method won't be
     *                       called in the producer when the serializer is passed in directly.
     * @param valueSerializer  The serializer for value that implements {@link Serializer}. The configure() method won't
     *                         be called in the producer when the serializer is passed in directly.
     */
    public KafkaProducer(Properties properties, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this(new ProducerConfig(ProducerConfig.addSerializerToConfig(properties, keySerializer, valueSerializer)),
             keySerializer, valueSerializer);
    }

    /**
     * 创建Producer构造方法
     * @param config 生产者配置
     * @param keySerializer key序列化器
     * @param valueSerializer value序列化器
     */
    @SuppressWarnings({"unchecked", "deprecation"})
    private KafkaProducer(ProducerConfig config, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        try {
            log.trace("Starting the Kafka producer");
            //得到用户提供的配置集合
            Map<String, Object> userProvidedConfigs = config.originals();
            //生产者配置
            this.producerConfig = config;
            //得到一个系统时钟
            this.time = new SystemTime();

            //从配置中得到clientId
            clientId = config.getString(ProducerConfig.CLIENT_ID_CONFIG);
            //如果不存在则递增生成
            if (clientId.length() <= 0)
                clientId = "producer-" + PRODUCER_CLIENT_ID_SEQUENCE.getAndIncrement();
            //指标比噢钱
            Map<String, String> metricTags = new LinkedHashMap<String, String>();
            //clientid添加到指标标签中
            metricTags.put("client-id", clientId);
            //得到指标配置，设置指标样本
            MetricConfig metricConfig = new MetricConfig().samples(config.getInt(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG))
                    //时间窗口
                    .timeWindow(config.getLong(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
                    //指标标签
                    .tags(metricTags);
            //根据配置得到指标上报器
            List<MetricsReporter> reporters = config.getConfiguredInstances(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG,
                    MetricsReporter.class);
            //添加默认指标上报器JMX
            reporters.add(new JmxReporter(JMX_PREFIX));
            //生成指标
            this.metrics = new Metrics(metricConfig, reporters, time);
            //通过反射拿到分区器
            this.partitioner = config.getConfiguredInstance(ProducerConfig.PARTITIONER_CLASS_CONFIG, Partitioner.class);
            //得到每次重试后需要等待的时间
            long retryBackoffMs = config.getLong(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
            //得到元数据配置，将每次重试等待的时间和最大的metadata的生命周期
            this.metadata = new Metadata(retryBackoffMs, config.getLong(ProducerConfig.METADATA_MAX_AGE_CONFIG));
            //最大请求大小 max.request.size
            this.maxRequestSize = config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
            //buffer.memory大小，缓冲区大小  设置生产者内存缓冲区的大小，生产者用它缓存要发送到服务器的消息。如果应用程序发送消息的速度超过发送到服务器的速度，会导致生产者空间不足。
            // 这个时候，send()方法调用要么被阻塞，要么抛出异常，取决于如何设置max.block.ms(0.9.0.0之前是block.on.buffer.full)参数。
            this.totalMemorySize = config.getLong(ProducerConfig.BUFFER_MEMORY_CONFIG);
            //压缩的类型
            this.compressionType = CompressionType.forName(config.getString(ProducerConfig.COMPRESSION_TYPE_CONFIG));
            /* check for user defined settings.
             * If the BLOCK_ON_BUFFER_FULL is set to true,we do not honor METADATA_FETCH_TIMEOUT_CONFIG.
             * This should be removed with release 0.9 when the deprecated configs are removed.
             */
            /*检查用户定义的设置。如果将BLOCK_ON_BUFFER_FULL设置为true，则我们不接受METADATA_FETCH_TIMEOUT_CONFIG。 *删除不赞成使用的配置时，应在0.9版中删除。
             */
            //指定了在调用send()方法或使用partitionFor()方法获取元数据时生产者的阻塞时间。
            // 当生产者的发送缓冲区已满，或者没有可用的元数据时，这些方法就会阻塞。在阻塞时间达到max.block.ms时，生产者会抛出异常。
            if (userProvidedConfigs.containsKey(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG)) {
                log.warn(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                boolean blockOnBufferFull = config.getBoolean(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG);
                if (blockOnBufferFull) {
                    this.maxBlockTimeMs = Long.MAX_VALUE;
                } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                    log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                            "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
                } else {
                    this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
                }
            } else if (userProvidedConfigs.containsKey(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG + " config is deprecated and will be removed soon. " +
                        "Please use " + ProducerConfig.MAX_BLOCK_MS_CONFIG);
                this.maxBlockTimeMs = config.getLong(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
            } else {
                //得到maxBlockTimeMs
                this.maxBlockTimeMs = config.getLong(ProducerConfig.MAX_BLOCK_MS_CONFIG);
            }

            /* check for user defined settings.
             * If the TIME_OUT config is set use that for request timeout.
             * This should be removed with release 0.9
             */
            if (userProvidedConfigs.containsKey(ProducerConfig.TIMEOUT_CONFIG)) {
                log.warn(ProducerConfig.TIMEOUT_CONFIG + " config is deprecated and will be removed soon. Please use " +
                        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                this.requestTimeoutMs = config.getInt(ProducerConfig.TIMEOUT_CONFIG);
            } else {
                //指定了生产者在发送数据时等待服务器返回的响应的时间，
                this.requestTimeoutMs = config.getInt(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
            }
            //根据batch.size和buffer.size和压缩类型和linger.ms和重试等待时长得到记录收集器
            this.accumulator = new RecordAccumulator(config.getInt(ProducerConfig.BATCH_SIZE_CONFIG),
                    this.totalMemorySize,
                    this.compressionType,
                    config.getLong(ProducerConfig.LINGER_MS_CONFIG),
                    retryBackoffMs,
                    metrics,
                    time);
            //校验并得到broker地址
            List<InetSocketAddress> addresses = ClientUtils.parseAndValidateAddresses(config.getList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            //更新metadata数据
            this.metadata.update(Cluster.bootstrap(addresses), time.milliseconds());
            //创建ChannelBuilder
            ChannelBuilder channelBuilder = ClientUtils.createChannelBuilder(config.values());
            //创建NetworkClient，Kafkaproducer网络I/O的核心
            NetworkClient client = new NetworkClient(
                    new Selector(config.getLong(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG), this.metrics, time, "producer", channelBuilder),
                    this.metadata,
                    clientId,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION),
                    config.getLong(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG),
                    config.getInt(ProducerConfig.SEND_BUFFER_CONFIG),
                    config.getInt(ProducerConfig.RECEIVE_BUFFER_CONFIG),
                    this.requestTimeoutMs, time);
            //创建Sender任务
            this.sender = new Sender(client,
                    this.metadata,
                    this.accumulator,
                    config.getInt(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION) == 1,
                    config.getInt(ProducerConfig.MAX_REQUEST_SIZE_CONFIG),
                    (short) parseAcks(config.getString(ProducerConfig.ACKS_CONFIG)),
                    config.getInt(ProducerConfig.RETRIES_CONFIG),
                    this.metrics,
                    new SystemTime(),
                    clientId,
                    this.requestTimeoutMs);
            String ioThreadName = "kafka-producer-network-thread" + (clientId.length() > 0 ? " | " + clientId : "");
            //封装io线程，将sender任务放入，以守护进程方式启动
            this.ioThread = new KafkaThread(ioThreadName, this.sender, true);
            this.ioThread.start();

            this.errors = this.metrics.sensor("errors");

            if (keySerializer == null) {
                this.keySerializer = config.getConfiguredInstance(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.keySerializer.configure(config.originals(), true);
            } else {
                config.ignore(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
                this.keySerializer = keySerializer;
            }
            if (valueSerializer == null) {
                this.valueSerializer = config.getConfiguredInstance(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        Serializer.class);
                this.valueSerializer.configure(config.originals(), false);
            } else {
                config.ignore(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
                this.valueSerializer = valueSerializer;
            }

            // load interceptors and make sure they get clientId
            userProvidedConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
            //反射得到ProducerInterceptor的子类
            List<ProducerInterceptor<K, V>> interceptorList = (List) (new ProducerConfig(userProvidedConfigs)).getConfiguredInstances(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                    ProducerInterceptor.class);
            this.interceptors = interceptorList.isEmpty() ? null : new ProducerInterceptors<>(interceptorList);

            //打印配置到控制台
            config.logUnused();
            //注册app信息
            AppInfoParser.registerAppInfo(JMX_PREFIX, clientId);
            log.debug("Kafka producer started");
        } catch (Throwable t) {
            // call close methods if internal objects are already constructed
            // this is to prevent resource leak. see KAFKA-2121
            close(0, TimeUnit.MILLISECONDS, true);
            // now propagate the exception
            throw new KafkaException("Failed to construct kafka producer", t);
        }
    }

    private static int parseAcks(String acksString) {
        try {
            return acksString.trim().equalsIgnoreCase("all") ? -1 : Integer.parseInt(acksString.trim());
        } catch (NumberFormatException e) {
            throw new ConfigException("Invalid configuration value for 'acks': " + acksString);
        }
    }

    /**
     * 异步发送一个记录到topic  相当于send(record, null);
     * Asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * 异步发送记录到topic并调用提供的回调当发送已经被确认时
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     * <p>
     * 发送是异步的，一旦记录已被存储在等待被发送记录的缓冲区（MemoryRecords）此方法将立即返回。 这允许并行发送多条记录而不阻塞等待每一个之后的响应。
     * The send is asynchronous and this method will return immediately once the record has been stored in the buffer of
     * records waiting to be sent. This allows sending many records in parallel without blocking to wait for the
     * response after each one.
     * <p>
     *
     * 发送的结果是一个RecordMetadata指定记录被发送到分区，偏移它被分配和记录的时间戳。
     * 如果topic是使用的CreateTime，时间戳将是用户提供时间戳或记录发送时间如果用户没有指定记录的时间戳。
     *  如果LogAppendTime用于topic，时间戳将是kafka broker的本地时间当追加的消息时。
     * The result of the send is a {@link RecordMetadata} specifying the partition the record was sent to, the offset
     * it was assigned and the timestamp of the record. If
     * {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME CreateTime} is used by the topic, the timestamp
     * will be the user provided timestamp or the record send time if the user did not specify a timestamp for the
     * record. If {@link org.apache.kafka.common.record.TimestampType#LOG_APPEND_TIME LogAppendTime} is used for the
     * topic, the timestamp will be the Kafka broker local time when the message is appended.
     * <p>
     * 自从调用send是异步的返回一个Future存储RecordMeta将会分配这个记录。调用get在这个future将会阻塞直到关联的请求完成然后返回这个record的元数据。
     * 抛出在发送记录时发生的任何异常。
     * Since the send call is asynchronous it returns a {@link java.util.concurrent.Future Future} for the
     * {@link RecordMetadata} that will be assigned to this record. Invoking {@link java.util.concurrent.Future#get()
     * get()} on this future will block until the associated request completes and then return the metadata for the record
     * or throw any exception that occurred while sending the record.
     * <p>
     * If you want to simulate a simple blocking call you can call the <code>get()</code> method immediately:
     *
     * <pre>
     * {@code
     * byte[] key = "key".getBytes();
     * byte[] value = "value".getBytes();
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("my-topic", key, value)
     * producer.send(record).get();
     * }</pre>
     * <p>
     * Fully non-blocking usage can make use of the {@link Callback} parameter to provide a callback that
     * will be invoked when the request is complete.
     *
     * <pre>
     * {@code
     * ProducerRecord<byte[],byte[]> record = new ProducerRecord<byte[],byte[]>("the-topic", key, value);
     * producer.send(myRecord,
     *               new Callback() {
     *                   public void onCompletion(RecordMetadata metadata, Exception e) {
     *                       if(e != null)
     *                           e.printStackTrace();
     *                       System.out.println("The offset of the record we just sent is: " + metadata.offset());
     *                   }
     *               });
     * }
     * </pre>
     *
     * Callbacks for records being sent to the same partition are guaranteed to execute in order. That is, in the
     * following example <code>callback1</code> is guaranteed to execute before <code>callback2</code>:
     *
     * <pre>
     * {@code
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key1, value1), callback1);
     * producer.send(new ProducerRecord<byte[],byte[]>(topic, partition, key2, value2), callback2);
     * }
     * </pre>
     * <p>
     *     这些callback将会通常只想在生产者的IO线程上，因此应该相当快，否则它们将延迟其他线程的消息发送。如果要执行阻塞或计算量大的回调，
     *     建议在回调主体中使用自己的{@link java.util.concurrent.Executor}来并行化处理。
     * Note that callbacks will generally execute in the I/O thread of the producer and so should be reasonably fast or
     * they will delay the sending of messages from other threads. If you want to execute blocking or computationally
     * expensive callbacks it is recommended to use your own {@link java.util.concurrent.Executor} in the callback body
     * to parallelize processing.
     *
     * @param record The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged by the server (null
     *        indicates no callback)
     *
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws SerializationException If the key or value are not valid objects given the configured serializers
     * @throws TimeoutException if the time taken for fetching metadata or allocating memory for the record has surpassed <code>max.block.ms</code>.
     *
     */
    @Override
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        // intercept the record, which can be potentially modified; this method does not throw exceptions
        ProducerRecord<K, V> interceptedRecord = this.interceptors == null ? record : this.interceptors.onSend(record);
        return doSend(interceptedRecord, callback);
    }

    /**
     * Implementation of asynchronously send a record to a topic. Equivalent to <code>send(record, null)</code>.
     * See {@link #send(ProducerRecord, Callback)} for details.
     */
    private Future<RecordMetadata> doSend(ProducerRecord<K, V> record, Callback callback) {
        TopicPartition tp = null;
        try {
            // first make sure the metadata for the topic is available
            long waitedOnMetadataMs = waitOnMetadata(record.topic(), this.maxBlockTimeMs);
            long remainingWaitMs = Math.max(0, this.maxBlockTimeMs - waitedOnMetadataMs);
            byte[] serializedKey;
            try {
                //序列化key
                serializedKey = keySerializer.serialize(record.topic(), record.key());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert key of class " + record.key().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in key.serializer");
            }
            byte[] serializedValue;
            try {
                //序列化value
                serializedValue = valueSerializer.serialize(record.topic(), record.value());
            } catch (ClassCastException cce) {
                throw new SerializationException("Can't convert value of class " + record.value().getClass().getName() +
                        " to class " + producerConfig.getClass(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG).getName() +
                        " specified in value.serializer");
            }
            //基于key 分区
            int partition = partition(record, serializedKey, serializedValue, metadata.fetch());
            int serializedSize = Records.LOG_OVERHEAD + Record.recordSize(serializedKey, serializedValue);
            ensureValidRecordSize(serializedSize);
            tp = new TopicPartition(record.topic(), partition);
            long timestamp = record.timestamp() == null ? time.milliseconds() : record.timestamp();
            log.trace("Sending record {} with callback {} to topic {} partition {}", record, callback, record.topic(), partition);
            // producer callback will make sure to call both 'callback' and interceptor callback
            Callback interceptCallback = this.interceptors == null ? callback : new InterceptorCallback<>(callback, this.interceptors, tp);
            //记录累积
            RecordAccumulator.RecordAppendResult result = accumulator.append(tp, timestamp, serializedKey, serializedValue, interceptCallback, remainingWaitMs);
            if (result.batchIsFull || result.newBatchCreated) {
                log.trace("Waking up the sender since topic {} partition {} is either full or getting a new batch", record.topic(), partition);
                this.sender.wakeup();
            }
            return result.future;
            // handling exceptions and record the errors;
            // for API exceptions return them in the future,
            // for other exceptions throw directly
        } catch (ApiException e) {
            log.debug("Exception occurred during message send:", e);
            if (callback != null)
                callback.onCompletion(null, e);
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            return new FutureFailure(e);
        } catch (InterruptedException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw new InterruptException(e);
        } catch (BufferExhaustedException e) {
            this.errors.record();
            this.metrics.sensor("buffer-exhausted-records").record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (KafkaException e) {
            this.errors.record();
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        } catch (Exception e) {
            // we notify interceptor about all exceptions, since onSend is called before anything else in this method
            if (this.interceptors != null)
                this.interceptors.onSendError(record, tp, e);
            throw e;
        }
    }

    /**
     * Wait for cluster metadata including partitions for the given topic to be available.
     * @param topic The topic we want metadata for
     * @param maxWaitMs The maximum time in ms for waiting on the metadata
     * @return The amount of time we waited in ms
     */
    private long waitOnMetadata(String topic, long maxWaitMs) throws InterruptedException {
        // add topic to metadata topic list if it is not there already.
        if (!this.metadata.containsTopic(topic))
            this.metadata.add(topic);

        if (metadata.fetch().partitionsForTopic(topic) != null)
            return 0;

        long begin = time.milliseconds();
        long remainingWaitMs = maxWaitMs;
        while (metadata.fetch().partitionsForTopic(topic) == null) {
            log.trace("Requesting metadata update for topic {}.", topic);
            int version = metadata.requestUpdate();
            sender.wakeup();
            metadata.awaitUpdate(version, remainingWaitMs);
            long elapsed = time.milliseconds() - begin;
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            if (metadata.fetch().unauthorizedTopics().contains(topic))
                throw new TopicAuthorizationException(topic);
            remainingWaitMs = maxWaitMs - elapsed;
        }
        return time.milliseconds() - begin;
    }

    /**
     * Validate that the record size isn't too large
     */
    private void ensureValidRecordSize(int size) {
        if (size > this.maxRequestSize)
            throw new RecordTooLargeException("The message is " + size +
                                              " bytes when serialized which is larger than the maximum request size you have configured with the " +
                                              ProducerConfig.MAX_REQUEST_SIZE_CONFIG +
                                              " configuration.");
        if (size > this.totalMemorySize)
            throw new RecordTooLargeException("The message is " + size +
                                              " bytes when serialized which is larger than the total memory buffer you have configured with the " +
                                              ProducerConfig.BUFFER_MEMORY_CONFIG +
                                              " configuration.");
    }

    /**
     * Invoking this method makes all buffered records immediately available to send (even if <code>linger.ms</code> is
     * greater than 0) and blocks on the completion of the requests associated with these records. The post-condition
     * of <code>flush()</code> is that any previously sent record will have completed (e.g. <code>Future.isDone() == true</code>).
     * A request is considered completed when it is successfully acknowledged
     * according to the <code>acks</code> configuration you have specified or else it results in an error.
     * <p>
     * Other threads can continue sending records while one thread is blocked waiting for a flush call to complete,
     * however no guarantee is made about the completion of records sent after the flush call begins.
     * <p>
     * This method can be useful when consuming from some input system and producing into Kafka. The <code>flush()</code> call
     * gives a convenient way to ensure all previously sent messages have actually completed.
     * <p>
     * This example shows how to consume from one Kafka topic and produce to another Kafka topic:
     * <pre>
     * {@code
     * for(ConsumerRecord<String, String> record: consumer.poll(100))
     *     producer.send(new ProducerRecord("my-topic", record.key(), record.value());
     * producer.flush();
     * consumer.commit();
     * }
     * </pre>
     *
     * Note that the above example may drop records if the produce request fails. If we want to ensure that this does not occur
     * we need to set <code>retries=&lt;large_number&gt;</code> in our config.
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void flush() {
        log.trace("Flushing accumulated records in producer.");
        this.accumulator.beginFlush();
        this.sender.wakeup();
        try {
            this.accumulator.awaitFlushCompletion();
        } catch (InterruptedException e) {
            throw new InterruptException("Flush interrupted.", e);
        }
    }

    /**
     * Get the partition metadata for the give topic. This can be used for custom partitioning.
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        try {
            waitOnMetadata(topic, this.maxBlockTimeMs);
        } catch (InterruptedException e) {
            throw new InterruptException(e);
        }
        return this.metadata.fetch().partitionsForTopic(topic);
    }

    /**
     * Get the full set of internal metrics maintained by the producer.
     */
    @Override
    public Map<MetricName, ? extends Metric> metrics() {
        return Collections.unmodifiableMap(this.metrics.metrics());
    }

    /**
     * Close this producer. This method blocks until all previously sent requests complete.
     * This method is equivalent to <code>close(Long.MAX_VALUE, TimeUnit.MILLISECONDS)</code>.
     * <p>
     * <strong>If close() is called from {@link Callback}, a warning message will be logged and close(0, TimeUnit.MILLISECONDS)
     * will be called instead. We do this because the sender thread would otherwise try to join itself and
     * block forever.</strong>
     * <p>
     *
     * @throws InterruptException If the thread is interrupted while blocked
     */
    @Override
    public void close() {
        close(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    /**
     * This method waits up to <code>timeout</code> for the producer to complete the sending of all incomplete requests.
     * <p>
     * If the producer is unable to complete all requests before the timeout expires, this method will fail
     * any unsent and unacknowledged records immediately.
     * <p>
     * If invoked from within a {@link Callback} this method will not block and will be equivalent to
     * <code>close(0, TimeUnit.MILLISECONDS)</code>. This is done since no further sending will happen while
     * blocking the I/O thread of the producer.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests. The value should be
     *                non-negative. Specifying a timeout of zero means do not wait for pending send requests to complete.
     * @param timeUnit The time unit for the <code>timeout</code>
     * @throws InterruptException If the thread is interrupted while blocked
     * @throws IllegalArgumentException If the <code>timeout</code> is negative.
     */
    @Override
    public void close(long timeout, TimeUnit timeUnit) {
        close(timeout, timeUnit, false);
    }

    private void close(long timeout, TimeUnit timeUnit, boolean swallowException) {
        if (timeout < 0)
            throw new IllegalArgumentException("The timeout cannot be negative.");

        log.info("Closing the Kafka producer with timeoutMillis = {} ms.", timeUnit.toMillis(timeout));
        // this will keep track of the first encountered exception
        AtomicReference<Throwable> firstException = new AtomicReference<Throwable>();
        boolean invokedFromCallback = Thread.currentThread() == this.ioThread;
        if (timeout > 0) {
            if (invokedFromCallback) {
                log.warn("Overriding close timeout {} ms to 0 ms in order to prevent useless blocking due to self-join. " +
                    "This means you have incorrectly invoked close with a non-zero timeout from the producer call-back.", timeout);
            } else {
                // Try to close gracefully.
                if (this.sender != null)
                    this.sender.initiateClose();
                if (this.ioThread != null) {
                    try {
                        this.ioThread.join(timeUnit.toMillis(timeout));
                    } catch (InterruptedException t) {
                        firstException.compareAndSet(null, t);
                        log.error("Interrupted while joining ioThread", t);
                    }
                }
            }
        }

        if (this.sender != null && this.ioThread != null && this.ioThread.isAlive()) {
            log.info("Proceeding to force close the producer since pending requests could not be completed " +
                "within timeout {} ms.", timeout);
            this.sender.forceClose();
            // Only join the sender thread when not calling from callback.
            if (!invokedFromCallback) {
                try {
                    this.ioThread.join();
                } catch (InterruptedException e) {
                    firstException.compareAndSet(null, e);
                }
            }
        }

        ClientUtils.closeQuietly(interceptors, "producer interceptors", firstException);
        ClientUtils.closeQuietly(metrics, "producer metrics", firstException);
        ClientUtils.closeQuietly(keySerializer, "producer keySerializer", firstException);
        ClientUtils.closeQuietly(valueSerializer, "producer valueSerializer", firstException);
        AppInfoParser.unregisterAppInfo(JMX_PREFIX, clientId);
        log.debug("The Kafka producer has closed.");
        if (firstException.get() != null && !swallowException)
            throw new KafkaException("Failed to close kafka producer", firstException.get());
    }

    /**
     * computes partition for given record.
     * if the record has partition returns the value otherwise
     * calls configured partitioner class to compute the partition.
     */
    private int partition(ProducerRecord<K, V> record, byte[] serializedKey , byte[] serializedValue, Cluster cluster) {
        Integer partition = record.partition();
        if (partition != null) {
            List<PartitionInfo> partitions = cluster.partitionsForTopic(record.topic());
            int lastPartition = partitions.size() - 1;
            // they have given us a partition, use it
            if (partition < 0 || partition > lastPartition) {
                throw new IllegalArgumentException(String.format("Invalid partition given with record: %d is not in the range [0...%d].", partition, lastPartition));
            }
            return partition;
        }
        return this.partitioner.partition(record.topic(), record.key(), serializedKey, record.value(), serializedValue,
            cluster);
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

    /**
     * A callback called when producer request is complete. It in turn calls user-supplied callback (if given) and
     * notifies producer interceptors about the request completion.
     */
    private static class InterceptorCallback<K, V> implements Callback {
        private final Callback userCallback;
        private final ProducerInterceptors<K, V> interceptors;
        private final TopicPartition tp;

        public InterceptorCallback(Callback userCallback, ProducerInterceptors<K, V> interceptors,
                                   TopicPartition tp) {
            this.userCallback = userCallback;
            this.interceptors = interceptors;
            this.tp = tp;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (this.interceptors != null) {
                if (metadata == null) {
                    this.interceptors.onAcknowledgement(new RecordMetadata(tp, -1, -1, Record.NO_TIMESTAMP, -1, -1, -1),
                                                        exception);
                } else {
                    this.interceptors.onAcknowledgement(metadata, exception);
                }
            }
            if (this.userCallback != null)
                this.userCallback.onCompletion(metadata, exception);
        }
    }
}
