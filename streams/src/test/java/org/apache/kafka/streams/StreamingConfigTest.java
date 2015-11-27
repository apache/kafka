package org.apache.kafka.streams;

import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.examples.WallclockTimestampExtractor;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertNull;
import static org.hamcrest.CoreMatchers.is;


/**
 * Created by bbejeck on 11/27/15.
 * Test for StreamingConfig
 */
public class StreamingConfigTest {

    private Properties props = new Properties();
    private StreamingConfig streamingConfig;
    private StreamThread streamThreadPlaceHolder = null;


    @Before
    public void setUp() {
        props.put(StreamingConfig.CLIENT_ID_CONFIG, "Example-Processor-Job");
        props.put("group.id","test-consumer-group");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        streamingConfig = new StreamingConfig(props);
    }



    @Test
    public void testGetConsumerConfigs() throws Exception {
        Map<String,Object> returnedProps = streamingConfig.getConsumerConfigs(streamThreadPlaceHolder);
        assertThat(returnedProps.get("group.id"),is("test-consumer-group"));

    }

    @Test
    public void testGetRestoreConsumerConfigs() throws Exception {
        Map<String,Object> returnedProps = streamingConfig.getRestoreConsumerConfigs();
        assertNull(returnedProps.get("group.id"));
    }
}