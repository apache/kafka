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
package org.apache.kafka.common.record;

import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.IdentityHashMap;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

public class KafkaGzipTest {

    private long seed;

    @Rule(order = Integer.MIN_VALUE)
    public TestWatcher watcher = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            throw new AssertionError("seed was " + seed, e);
        }

        @Override
        protected void succeeded(Description description) {
            //not interested
        }
    };

    @Before
    public void setup() {
        seed = System.currentTimeMillis();
    }

    @Test
    public void testCompressibleInput() throws Exception {
        Random random = new Random(seed);
        byte[] payload = generateCompressibleBytes(random, 1, 24 * 1024);
        byte[] compressedByVanilla = compressUsingJDK(payload);
        byte[] decompressed = decompressUsingBufferSupplier(random, compressedByVanilla);
        Assert.assertArrayEquals("decompressed contents differ from input", payload, decompressed);
    }

    /**
     * generates a random compressible sequence of bytes of a random size (between bounds).
     * sequence would be composed of subsequences, each subsequence would be composed of a
     * single value repeated 1-10 times.
     */
    private static byte[] generateCompressibleBytes(Random random, int minSize, int maxSize) {
        if (maxSize < minSize) {
            throw new IllegalArgumentException();
        }
        int size = minSize + random.nextInt(maxSize - minSize);
        byte[] output = new byte[size];
        byte[] oneByte = new byte[1];
        int remaining = size;
        int position = 0;
        while (remaining > 0) {
            int sequenceSize = 1 + random.nextInt(10); //[1-10]
            if (sequenceSize > remaining) {
                sequenceSize = remaining;
            }
            random.nextBytes(oneByte);
            for (int i = 0; i < sequenceSize; i++) {
                output[position++] = oneByte[0];
            }
            remaining -= sequenceSize;
        }
        return output;
    }

    private static byte[] compressUsingJDK(byte[] payload) throws Exception {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream(payload.length);
                GZIPOutputStream gos = new GZIPOutputStream(baos, 8 * 1024) //kafka default for gzip
        ) {
            gos.write(payload);
            gos.flush();
            gos.close();
            baos.flush();
            return baos.toByteArray();
        }
    }

    private static byte[] decompressUsingBufferSupplier(Random random, byte[] compressed) throws Exception {
        try (ValidatingBufferSupplier supplier = new ValidatingBufferSupplier(random)) {
            ByteBuffer compressedBuffer = ByteBuffer.wrap(compressed);
            InputStream is = new KafkaBufferedInputStream(
                    new KafkaGZIPInputStream(
                            new ByteBufferInputStream(compressedBuffer), supplier, 8 * 1024
                    ),
                    supplier,
                    16 * 1024
            );
            //2 data buffers and a skip buffer
            Assert.assertEquals(3, supplier.numOutstandingBuffers());
            ByteArrayOutputStream os = new ByteArrayOutputStream(compressed.length);
            byte[] copyBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(copyBuffer)) >= 0) {
                if (bytesRead > 0) {
                    os.write(copyBuffer, 0, bytesRead);
                }
            }
            is.close();
            Assert.assertEquals(0, supplier.numOutstandingBuffers());
            os.flush();
            return os.toByteArray();
        }
    }

    private static class ValidatingBufferSupplier extends BufferSupplier {
        private final IdentityHashMap<ByteBuffer, RuntimeException> buffersOutstanding = new IdentityHashMap<>();
        private final Random random;
        private volatile AssertionError issue;

        public ValidatingBufferSupplier(Random random) {
            this.random = random;
        }

        @Override
        public ByteBuffer get(int capacity) {
            RuntimeException pointOfAllocation = new RuntimeException("this is where the buffer was requested");
            int deliveredCapacity = capacity + random.nextInt(10 * capacity + 1); //1x-10x requested capacity, inclusive
            ByteBuffer buffer = ByteBuffer.allocate(deliveredCapacity);
            buffersOutstanding.put(buffer, pointOfAllocation);
            return buffer;
        }

        @Override
        public void release(ByteBuffer buffer) {
            if (buffer == null) {
                issue = new AssertionError("null buffer released");
                throw issue;
            }
            RuntimeException pointOfAllocation = buffersOutstanding.remove(buffer);
            if (pointOfAllocation == null) {
                issue = new AssertionError("foreign buffer released");
                throw issue;
            }
        }

        public int numOutstandingBuffers() {
            return buffersOutstanding.size();
        }

        @Override
        public void close() {
            if (issue != null) {
                throw issue;
            }
            int leakedBuffers = buffersOutstanding.size();
            if (leakedBuffers != 0) {
                AssertionError error = new AssertionError(leakedBuffers + " buffers allocated yet never released");
                buffersOutstanding.values().forEach(error::addSuppressed);
                throw error;
            }
        }
    }
}
