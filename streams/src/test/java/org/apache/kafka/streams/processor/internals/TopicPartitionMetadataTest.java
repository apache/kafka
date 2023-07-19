/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor.internals;

import java.nio.ByteBuffer;
import java.util.Base64;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TopicPartitionMetadataTest {

    @Test
    public void shouldGetPartitionTimeAndProcessorMeta() {
        final ProcessorMetadata metadata = new ProcessorMetadata();
        final String key = "some_key";
        final long value = 100L;
        metadata.put(key, value);

        final TopicPartitionMetadata topicMeta = new TopicPartitionMetadata(100L, metadata);

        assertThat(topicMeta.partitionTime(), is(100L));
        assertThat(topicMeta.processorMetadata(), is(metadata));
    }

    @Test
    public void shouldDecodeVersionOne() {
        final byte[] serialized = ByteBuffer.allocate(Byte.BYTES + Long.BYTES)
            .put((byte) 1)
            .putLong(100L)
            .array();
        final String serializedString = Base64.getEncoder().encodeToString(serialized);

        final TopicPartitionMetadata topicMeta = TopicPartitionMetadata.decode(serializedString);

        assertThat(topicMeta.partitionTime(), is(100L));
        assertThat(topicMeta.processorMetadata(), is(new ProcessorMetadata()));
    }

    @Test
    public void shouldEncodeDecodeVersionTwo() {
        final ProcessorMetadata metadata = new ProcessorMetadata();
        final String key = "some_key";
        final long value = 100L;
        metadata.put(key, value);

        final TopicPartitionMetadata expected = new TopicPartitionMetadata(100L, metadata);
        final String serializedString = expected.encode();
        final TopicPartitionMetadata topicMeta = TopicPartitionMetadata.decode(serializedString);

        assertThat(topicMeta, is(expected));
    }

    @Test
    public void shouldEncodeDecodeEmptyMetaVersionTwo() {
        final TopicPartitionMetadata expected = new TopicPartitionMetadata(100L, new ProcessorMetadata());
        final String serializedString = expected.encode();
        final TopicPartitionMetadata topicMeta = TopicPartitionMetadata.decode(serializedString);

        assertThat(topicMeta, is(expected));
    }

    @Test
    public void shouldDecodeEmptyStringVersionTwo() {
        final TopicPartitionMetadata expected = new TopicPartitionMetadata(RecordQueue.UNKNOWN, new ProcessorMetadata());
        final TopicPartitionMetadata topicMeta = TopicPartitionMetadata.decode("");

        assertThat(topicMeta, is(expected));
    }

    @Test
    public void shouldReturnUnknownTimestampIfUnknownVersion() {
        final byte[] emptyMessage = {TopicPartitionMetadata.LATEST_MAGIC_BYTE + 1};
        final String encodedString = Base64.getEncoder().encodeToString(emptyMessage);

        final TopicPartitionMetadata decoded = TopicPartitionMetadata.decode(encodedString);

        assertThat(decoded.partitionTime(), is(RecordQueue.UNKNOWN));
        assertThat(decoded.processorMetadata(), is(new ProcessorMetadata()));
    }

    @Test
    public void shouldReturnUnknownTimestampIfInvalidMetadata() {
        final String invalidBase64String = "{}";

        final TopicPartitionMetadata decoded = TopicPartitionMetadata.decode(invalidBase64String);

        assertThat(decoded.partitionTime(), is(RecordQueue.UNKNOWN));
        assertThat(decoded.processorMetadata(), is(new ProcessorMetadata()));
    }
}
