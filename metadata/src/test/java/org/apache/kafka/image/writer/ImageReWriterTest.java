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

package org.apache.kafka.image.writer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.apache.kafka.metadata.RecordTestUtils.testRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


@Timeout(value = 40)
public class ImageReWriterTest {
    @Test
    public void testWrite() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        ImageReWriter writer = new ImageReWriter(delta);
        writer.write(testRecord(0));
        writer.write(testRecord(1));
        writer.close(true);
        assertEquals(2, delta.getOrCreateTopicsDelta().changedTopics().size());
        assertEquals(2, writer.image().topics().topicsById().size());
    }

    @Test
    public void testCloseWithoutFreeze() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        ImageReWriter writer = new ImageReWriter(delta);
        writer.close();
        assertNull(writer.image());
    }

    @Test
    public void testWriteAfterClose() {
        MetadataDelta delta = new MetadataDelta.Builder().build();
        ImageReWriter writer = new ImageReWriter(delta);
        writer.close(true);
        assertThrows(ImageWriterClosedException.class, () ->
                writer.write(0, new TopicRecord().
                        setName("foo").
                        setTopicId(Uuid.fromString("3B134hrsQgKtz8Sp6QBIfg"))));
    }
}
