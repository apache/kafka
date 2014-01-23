package kafka.common;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * A serializer and deserializer for strings.
 * <p>
 * This class accepts a configuration parameter string.encoding which can take the string name of any supported
 * encoding. If no encoding is specified the default will be UTF-8.
 */
public class StringSerialization implements Serializer, Deserializer, Configurable {

    private final static String ENCODING_CONFIG = "string.encoding";

    private String encoding;

    public StringSerialization(String encoding) {
        super();
        this.encoding = encoding;
    }

    public StringSerialization() {
        this("UTF8");
    }

    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(ENCODING_CONFIG))
            this.encoding = (String) configs.get(ENCODING_CONFIG);
    }

    @Override
    public Object fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            try {
                return new String(bytes, encoding);
            } catch (UnsupportedEncodingException e) {
                throw new KafkaException(e);
            }
        }
    }

    @Override
    public byte[] toBytes(Object o) {
        if (o == null) {
            return null;
        } else {
            try {
                return ((String) o).getBytes(encoding);
            } catch (UnsupportedEncodingException e) {
                throw new KafkaException(e);
            }
        }
    }

}
