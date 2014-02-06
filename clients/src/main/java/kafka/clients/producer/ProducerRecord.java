package kafka.clients.producer;

/**
 * A key/value pair to be sent to Kafka. This consists of a topic name to which the record is being sent, an optional
 * partition number, and an optional key and value.
 * <p>
 * If a valid partition number is specified that partition will be used when sending the record. If no partition is
 * specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is
 * present a partition will be assigned in a round-robin fashion.
 */
public final class ProducerRecord {

    private final String topic;
    private final Integer partition;
    private final byte[] key;
    private final byte[] value;

    /**
     * Creates a record to be sent to a specified topic and partition
     * 
     * @param topic The topic the record will be appended to
     * @param partition The partition to which the record should be sent
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, Integer partition, byte[] key, byte[] value) {
        if (topic == null)
            throw new IllegalArgumentException("Topic cannot be null");
        this.topic = topic;
        this.partition = partition;
        this.key = key;
        this.value = value;
    }

    /**
     * Create a record to be sent to Kafka
     * 
     * @param topic The topic the record will be appended to
     * @param key The key that will be included in the record
     * @param value The record contents
     */
    public ProducerRecord(String topic, byte[] key, byte[] value) {
        this(topic, null, key, value);
    }

    /**
     * Create a record with no key
     * 
     * @param topic The topic this record should be sent to
     * @param value The record contents
     */
    public ProducerRecord(String topic, byte[] value) {
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
    public byte[] key() {
        return key;
    }

    /**
     * @return The value
     */
    public byte[] value() {
        return value;
    }

    /**
     * The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partition() {
        return partition;
    }

}
