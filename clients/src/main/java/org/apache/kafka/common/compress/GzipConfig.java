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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionConfig;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class GzipConfig extends CompressionConfig {
    public CompressionType getType() {
        return CompressionType.GZIP;
    }

    @Override
    public OutputStream wrapForOutput(ByteBufferOutputStream buffer, byte messageVersion) {
        try {
            // Set input buffer (uncompressed) to 16 KB (none by default) and output buffer (compressed) to
            // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller passes a small
            // number of bytes to write (potentially a single byte)
            return new BufferedOutputStream(new GZIPOutputStream(buffer, 8 * 1024), 16 * 1024);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    @Override
    public InputStream wrapForInput(ByteBuffer buffer, byte messageVersion, BufferSupplier decompressionBufferSupplier) {
        try {
            // Set output buffer (uncompressed) to 16 KB (none by default) and input buffer (compressed) to
            // 8 KB (0.5 KB by default) to ensure reasonable performance in cases where the caller reads a small
            // number of bytes (potentially a single byte)
            return new BufferedInputStream(new GZIPInputStream(new ByteBufferInputStream(buffer), 8 * 1024),
                16 * 1024);
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public static class Builder extends CompressionConfig.Builder<GzipConfig> {
        @Override
        public GzipConfig build() {
            return new GzipConfig();
        }
    }
}
