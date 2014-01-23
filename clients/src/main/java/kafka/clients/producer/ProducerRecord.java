package kafka.clients.producer;

/**
 * An unserialized key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent,
 * a value (which can be null) which is the contents of the record and an optional key (which can also be null). In
 * cases the key used for choosing a partition is going to be different the user can specify a partition key which will
 * be used only for computing the partition to which this record will be sent and will not be retained with the record.
 */
public final class ProducerRecord {

    private final String topic;
    private final Object key;
    private final Object partitionKey;
    private final Object value;

    /**
     * Creates a record to be sent to Kafka using a special override key for partitioning that is different form the key
     * retained in the record
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param partitionKey An override for the key to be used only for partitioning purposes in the client. This key
     *        will not be retained or available to downstream consumers.
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object key, Object partitionKey, Object value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.key = key;
        this.partitionKey = partitionKey;
        this.value = value;
    }

    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object key, Object value) {
        this(topic, key, key, value);
    }

    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, Object value) {
        this(topic, null, value);
    }

    /**
     * The topic this record is being sent to
     */
    public String topic() {
        return topic;
    }

    /**
     * The key (or null if no key is specified)
     */
    public Object key() {
        return key;
    }

    /**
     * An override key to use instead of the main record key
     */
    public Object partitionKey() {
        return partitionKey;
    }

    /**
     * @return The value
     */
    public Object value() {
        return value;
    }

}
