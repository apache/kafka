package kafka.common;

/**
 * A class that controls how an object is turned into bytes. Classes implementing this interface will generally be
 * instantiated by the framework.
 * <p>
 * An implementation that requires special configuration parameters can implement {@link Configurable}
 */
public interface Deserializer {

    /**
     * Map a byte[] to an object
     * @param bytes The bytes for the object (can be null)
     * @return The deserialized object (can return null)
     */
    public Object fromBytes(byte[] bytes);

}
