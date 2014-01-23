package kafka.common.requests;

import java.nio.ByteBuffer;

import kafka.common.network.NetworkSend;
import kafka.common.protocol.types.Struct;

/**
 * A send object for a kafka request
 */
public class RequestSend extends NetworkSend {

    private final RequestHeader header;
    private final Struct body;

    public RequestSend(int destination, RequestHeader header, Struct body) {
        super(destination, serialize(header, body));
        this.header = header;
        this.body = body;
    }

    private static ByteBuffer serialize(RequestHeader header, Struct body) {
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + body.sizeOf());
        header.writeTo(buffer);
        body.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public RequestHeader header() {
        return this.header;
    }

    public Struct body() {
        return body;
    }

}
