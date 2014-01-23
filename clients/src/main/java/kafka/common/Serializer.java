package kafka.common;

/**
 * A class that controls how an object is turned into bytes. Classes implementing this interface will generally be
 * instantiated by the framework.
 * <p>
 * An implementation should handle null inputs.
 * <p>
 * An implementation that requires special configuration parameters can implement {@link Configurable}
 */
public interface Serializer {

    /**
     * Translate an object into bytes. The serializer must handle null inputs, and will generally just return null in
     * this case.
     * @param o The object to serialize, can be null
     * @return The serialized bytes for the object or null
     */
    public byte[] toBytes(Object o);

}
