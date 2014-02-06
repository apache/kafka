package kafka.common.requests;

import static kafka.common.protocol.Protocol.RESPONSE_HEADER;

import java.nio.ByteBuffer;

import kafka.common.protocol.Protocol;
import kafka.common.protocol.types.Field;
import kafka.common.protocol.types.Struct;

/**
 * A response header in the kafka protocol.
 */
public class ResponseHeader {

    private static Field CORRELATION_KEY_FIELD = RESPONSE_HEADER.get("correlation_id");

    private final Struct header;

    public ResponseHeader(Struct header) {
        this.header = header;
    }

    public ResponseHeader(int correlationId) {
        this(new Struct(Protocol.RESPONSE_HEADER));
        this.header.set(CORRELATION_KEY_FIELD, correlationId);
    }

    public int correlationId() {
        return (Integer) header.get(CORRELATION_KEY_FIELD);
    }

    public void writeTo(ByteBuffer buffer) {
        header.writeTo(buffer);
    }

    public int sizeOf() {
        return header.sizeOf();
    }

    public static ResponseHeader parse(ByteBuffer buffer) {
        return new ResponseHeader(((Struct) Protocol.RESPONSE_HEADER.read(buffer)));
    }

}
