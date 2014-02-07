package org.apache.kafka.common.requests;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ProtoUtils;
import org.apache.kafka.common.protocol.types.Struct;

public class MetadataRequest {

    private final List<String> topics;

    public MetadataRequest(List<String> topics) {
        this.topics = topics;
    }

    public Struct toStruct() {
        String[] ts = new String[topics.size()];
        topics.toArray(ts);
        Struct body = new Struct(ProtoUtils.currentRequestSchema(ApiKeys.METADATA.id));
        body.set("topics", topics.toArray());
        return body;
    }

}
