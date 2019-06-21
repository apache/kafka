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

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * An extension of {@link GZIPOutputStream}, with compression level, block size configuration functionality.
 */
public class GZipOutputStream extends GZIPOutputStream {
    /**
     * Creates a new {@link OutputStream} with the specified buffer size and compression level.
     *
     * @param out   the output stream
     * @param size  the output buffer size
     * @param level the compression level
     * @throws IOException If an I/O error has occurred.
     */
    private GZipOutputStream(OutputStream out, int size, int level) throws IOException {
        super(out, size);
        setLevel(level);
    }

    /**
     * Sets the compression level.
     *
     * @param level the compression level
     * @throws IllegalArgumentException If given level is not valid (e.g, not between {@link Deflater#BEST_SPEED} and  {@link Deflater#BEST_COMPRESSION})
     */
    private void setLevel(int level) {
        // Given compression level is not in the valid range, nor default one.
        if ((level < Deflater.BEST_SPEED || Deflater.BEST_COMPRESSION < level) && level != Deflater.DEFAULT_COMPRESSION) {
            throw new IllegalArgumentException("Gzip doesn't support given compression level: " + level);
        }

        def.setLevel(level);
    }

    /**
     * Create a new {@link OutputStream} that will compress data using the GZip algorithm the specified buffer size and compression level.
     * <p>
     * The default buffer size is 8192(=8KB) for historical reason. For details, see {@link CompressionType#GZIP}.
     *
     * @param out              The output stream to compress.
     * @param compressionLevel The compression level to use. If null, it falls back to the default level.
     * @param blockSize        The buffer size to use during compression. If null, it falls back to the default block size.
     * @throws IllegalArgumentException If given level is not valid (e.g, not between {@link Deflater#BEST_SPEED} and  {@link Deflater#BEST_COMPRESSION})
     * @throws IOException              If an I/O error has occurred.
     */
    public static GZipOutputStream of(OutputStream out, Integer compressionLevel, Integer blockSize) throws IOException {
        if (null == blockSize) {
            return new GZipOutputStream(out, 8 * 1024, compressionLevel == null ? Deflater.DEFAULT_COMPRESSION : compressionLevel);
        } else {
            return new GZipOutputStream(out, blockSize, compressionLevel == null ? Deflater.DEFAULT_COMPRESSION : compressionLevel);
        }
    }
}
