package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

// This should be removed once all requests are converted to use the generated request classes
public abstract class LegacyAbstractRequest extends AbstractRequest {

    protected LegacyAbstractRequest(ApiKeys apiKeys, short version) {
        super(apiKeys, version);
    }

    // Once this method is removed, we should make the superclass method final
    public final ByteBuffer serializeWithHeader(RequestHeader header) {
        Struct headerStruct = header.data().toStruct(header.apiVersion());
        return RequestUtils.serialize(headerStruct, toStruct());
    }

    protected abstract Struct toStruct();

    @Override
    protected ByteBuffer serializeBody() {
        Struct bodyStruct = toStruct();
        ByteBuffer buffer = ByteBuffer.allocate(bodyStruct.sizeOf());
        bodyStruct.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }

    public String toString(boolean verbose) {
        return toStruct().toString();
    }
}
