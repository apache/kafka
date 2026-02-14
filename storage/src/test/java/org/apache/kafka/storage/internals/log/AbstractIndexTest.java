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

package org.apache.kafka.storage.internals.log;

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AbstractIndexTest {
    private static class TestIndex extends AbstractIndex {
        private boolean unmapInvoked = false;
        private MappedByteBuffer unmappedBuffer = null;
        public TestIndex(File file, long baseOffset, int maxIndexSize, boolean writable) throws IOException {
            super(file, baseOffset, maxIndexSize, writable);
        }

        @Override
        protected int entrySize() {
            return 1;
        }

        @Override
        protected IndexEntry parseEntry(ByteBuffer buffer, int n) {
            return null;
        }

        @Override
        public void sanityCheck() {
            // unused
        }

        @Override
        protected void truncate() {
            // unused
        }

        @Override
        public void truncateTo(long offset) {
            // unused
        }

        @Override
        protected void forceUnmap() throws IOException {
            unmapInvoked = true;
            unmappedBuffer = mmap();
        }
    }

    @Test
    public void testResizeInvokeUnmap() throws IOException {
        File f = new File(TestUtils.tempDirectory(), "test-index");
        TestIndex idx = new TestIndex(f, 0L, 100, true);
        MappedByteBuffer oldMmap = idx.mmap();
        assertNotNull(idx.mmap(), "MappedByteBuffer should not be null");
        assertFalse(idx.unmapInvoked, "Unmap should not have been invoked yet");

        boolean changed = idx.resize(80);
        assertTrue(changed);
        assertTrue(idx.unmapInvoked, "Unmap should have been invoked after resize");
        assertSame(oldMmap, idx.unmappedBuffer, "old mmap should be unmapped");
        assertNotSame(idx.unmappedBuffer, idx.mmap());
    }
}
