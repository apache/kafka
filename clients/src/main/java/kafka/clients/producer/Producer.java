package kafka.clients.producer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import kafka.common.Metric;
import kafka.common.PartitionInfo;

/**
 * The interface for the {@link KafkaProducer}
 * 
 * @see KafkaProducer
 * @see MockProducer
 */
public interface Producer extends Closeable {

    /**
     * Send the given record asynchronously and return a future which will eventually contain the response information.
     * 
     * @param record The record to send
     * @return A future which will eventually contain the response information
     */
    public Future<RecordMetadata> send(ProducerRecord record);

    /**
     * Send a record and invoke the given callback when the record has been acknowledged by the server
     */
    public Future<RecordMetadata> send(ProducerRecord record, Callback callback);

    /**
     * Get a list of partitions for the given topic for custom partition assignment. The partition metadata will change
     * over time so this list should not be cached.
     */
    public List<PartitionInfo> partitionsFor(String topic);

    /**
     * Return a map of metrics maintained by the producer
     */
    public Map<String, ? extends Metric> metrics();

    /**
     * Close this producer
     */
    public void close();

}
