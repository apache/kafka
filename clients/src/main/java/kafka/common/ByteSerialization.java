package kafka.common;

/**
 * A serialization implementation that just retains the provided byte array unchanged
 */
public class ByteSerialization implements Serializer, Deserializer {

    @Override
    public Object fromBytes(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] toBytes(Object o) {
        return (byte[]) o;
    }

}
