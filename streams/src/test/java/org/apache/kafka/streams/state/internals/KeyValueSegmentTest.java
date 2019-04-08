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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.test.TestUtils;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class KeyValueSegmentTest {

    @Test
    public void shouldDeleteStateDirectoryOnDestroy() throws Exception {
        final KeyValueSegment segment = new KeyValueSegment("segment", "window", 0L);
        final String directoryPath = TestUtils.tempDirectory().getAbsolutePath();
        final File directory = new File(directoryPath);

        final ProcessorContext mockContext = mock(ProcessorContext.class);
        expect(mockContext.appConfigs()).andReturn(emptyMap());
        expect(mockContext.stateDir()).andReturn(directory);
        replay(mockContext);

        segment.openDB(mockContext);

        assertTrue(new File(directoryPath, "window").exists());
        assertTrue(new File(directoryPath + File.separator + "window", "segment").exists());
        assertTrue(new File(directoryPath + File.separator + "window", "segment").list().length > 0);
        segment.destroy();
        assertFalse(new File(directoryPath + File.separator + "window", "segment").exists());
        assertTrue(new File(directoryPath, "window").exists());
    }

    @Test
    public void shouldBeEqualIfIdIsEqual() {
        final KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
        final KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
        final KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

        assertThat(segment, equalTo(segment));
        assertThat(segment, equalTo(segmentSameId));
        assertThat(segment, not(equalTo(segmentDifferentId)));
        assertThat(segment, not(equalTo(null)));
        assertThat(segment, not(equalTo("anyName")));
    }

    @Test
    public void shouldHashOnSegmentIdOnly() {
        final KeyValueSegment segment = new KeyValueSegment("anyName", "anyName", 0L);
        final KeyValueSegment segmentSameId = new KeyValueSegment("someOtherName", "someOtherName", 0L);
        final KeyValueSegment segmentDifferentId = new KeyValueSegment("anyName", "anyName", 1L);

        final Set<KeyValueSegment> set = new HashSet<>();
        assertTrue(set.add(segment));
        assertFalse(set.add(segmentSameId));
        assertTrue(set.add(segmentDifferentId));
    }

    @Test
    public void shouldCompareSegmentIdOnly() {
        final KeyValueSegment segment1 = new KeyValueSegment("a", "C", 50L);
        final KeyValueSegment segment2 = new KeyValueSegment("b", "B", 100L);
        final KeyValueSegment segment3 = new KeyValueSegment("c", "A", 0L);

        assertThat(segment1.compareTo(segment1), equalTo(0));
        assertThat(segment1.compareTo(segment2), equalTo(-1));
        assertThat(segment2.compareTo(segment1), equalTo(1));
        assertThat(segment1.compareTo(segment3), equalTo(1));
        assertThat(segment3.compareTo(segment1), equalTo(-1));
        assertThat(segment2.compareTo(segment3), equalTo(1));
        assertThat(segment3.compareTo(segment2), equalTo(-1));
    }
}
