package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MetadataRequestTest {

    @Test
    public void testEmptyMeansAllTopicsV0() {
        Struct rawRequest = new Struct(MetadataRequest.schemaVersions()[0]);
        rawRequest.set("topics", new Object[0]);
        MetadataRequest parsedRequest = new MetadataRequest(rawRequest, (short) 0);
        assertTrue(parsedRequest.isAllTopics());
        assertNull(parsedRequest.topics());
    }

    @Test
    public void testEmptyMeansEmptyForVersionsAboveV0() {
        for (int i = 1; i < MetadataRequest.schemaVersions().length; i++) {
            Schema schema = MetadataRequest.schemaVersions()[i];
            Struct rawRequest = new Struct(schema);
            rawRequest.set("topics", new Object[0]);
            if (rawRequest.hasField("allow_auto_topic_creation"))
                rawRequest.set("allow_auto_topic_creation", true);
            MetadataRequest parsedRequest = new MetadataRequest(rawRequest, (short) i);
            assertFalse(parsedRequest.isAllTopics());
            assertEquals(Collections.emptyList(), parsedRequest.topics());
        }
    }

}
