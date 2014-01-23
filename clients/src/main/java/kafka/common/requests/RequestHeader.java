package kafka.common.requests;

import static kafka.common.protocol.Protocol.REQUEST_HEADER;

import java.nio.ByteBuffer;

import kafka.common.protocol.ProtoUtils;
import kafka.common.protocol.Protocol;
import kafka.common.protocol.types.Field;
import kafka.common.protocol.types.Struct;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader {

    private static Field API_KEY_FIELD = REQUEST_HEADER.get("api_key");
    private static Field API_VERSION_FIELD = REQUEST_HEADER.get("api_version");
    private static Field CLIENT_ID_FIELD = REQUEST_HEADER.get("client_id");
    private static Field CORRELATION_ID_FIELD = REQUEST_HEADER.get("correlation_id");

    private final Struct header;

    public RequestHeader(Struct header) {
        super();
        this.header = header;
    }

    public RequestHeader(short apiKey, String client, int correlation) {
        this(apiKey, ProtoUtils.latestVersion(apiKey), client, correlation);
    }

    public RequestHeader(short apiKey, short version, String client, int correlation) {
        this(new Struct(Protocol.REQUEST_HEADER));
        this.header.set(API_KEY_FIELD, apiKey);
        this.header.set(API_VERSION_FIELD, version);
        this.header.set(CLIENT_ID_FIELD, client);
        this.header.set(CORRELATION_ID_FIELD, correlation);
    }

    public short apiKey() {
        return (Short) this.header.get(API_KEY_FIELD);
    }

    public short apiVersion() {
        return (Short) this.header.get(API_VERSION_FIELD);
    }

    public String clientId() {
        return (String) this.header.get(CLIENT_ID_FIELD);
    }

    public int correlationId() {
        return (Integer) this.header.get(CORRELATION_ID_FIELD);
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        return new RequestHeader((Struct) Protocol.REQUEST_HEADER.read(buffer));
    }

    public void writeTo(ByteBuffer buffer) {
        header.writeTo(buffer);
    }

    public int sizeOf() {
        return header.sizeOf();
    }
}
