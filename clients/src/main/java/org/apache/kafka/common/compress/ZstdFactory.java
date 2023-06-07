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

package org.apache.kafka.common.compress;

import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdBufferDecompressingStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class ZstdFactory {

    private ZstdFactory() { }

    public static OutputStream wrapForOutput(ByteBufferOutputStream buffer) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) to ensure reasonable performance
            // in cases where the caller passes a small number of bytes to write (potentially a single byte).
            return new BufferedOutputStream(new ZstdOutputStreamNoFinalizer(buffer, RecyclingBufferPool.INSTANCE), 16 * 1024);
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }

    public static InputStream wrapForInput(ByteBuffer compressedData, BufferSupplier decompressionBufferSupplier) {
        try {
            // The responsibility of closing the stream is pushed to the caller of this method.
            final ZstdBufferDecompressingStreamNoFinalizer stream = new ZstdBufferDecompressingStreamNoFinalizer(compressedData);
            // We need to convert the underlying stream, ZstdBufferDecompressingStreamNoFinalizer to InputStream interface,
            // hence this wrapper.
            return new InputStream() {
                @Override
                public int read() throws IOException {
                    // prevent a call to underlying stream if no data is remaining
                    if (!stream.hasRemaining())
                        return -1;

                    ByteBuffer temp = null;
                    try {
                        temp = decompressionBufferSupplier.get(1);
                        int res = stream.read(temp);
                        if (res <= 0) {
                            return -1;
                        }
                        return Byte.toUnsignedInt(temp.get());
                    } finally {
                        if (temp != null)
                            decompressionBufferSupplier.release(temp);
                    }
                }
                @Override
                public int read(byte[] b, int off, int len) throws IOException {
                    // prevent a call to underlying stream if no data is remaining
                    if (!stream.hasRemaining())
                        return -1;

                    int res = stream.read(ByteBuffer.wrap(b, off, len));
                    if (res <= 0) {
                        return -1;
                    }
                    return res;
                }
                @Override
                public void close() {
                    stream.close();
                }
            };
        } catch (Throwable e) {
            throw new KafkaException(e);
        }
    }
}
