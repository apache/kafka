package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;

// This should be removed once all responses are converted to use the generated response classes
public abstract class LegacyAbstractResponse extends AbstractResponse {

    protected LegacyAbstractResponse(ApiKeys apiKey) {
        super(apiKey);
    }

    protected abstract Struct toStruct(short version);

    // Once this method is removed, we should make the superclass method private
    protected ByteBuffer serializeWithHeader(ResponseHeader header, short version) {
        Struct headerStruct = header.data().toStruct(version);
        return RequestUtils.serialize(headerStruct, toStruct(version));
    }

    public String toString(short version) {
        return toStruct(version).toString();
    }
}
