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

import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.io.DataOutputStream;

/**
 * This class holds all compression configurations: compression type, compression level and the size of compression buffer.
 */
public class CompressionConfig {
    private final CompressionType type;
    private final Integer level;
    private final Integer bufferSize;

    private CompressionConfig(CompressionType type, Integer level, Integer bufferSize) {
        this.type = type;
        this.level = level;
        this.bufferSize = bufferSize;
    }

    public CompressionType getType() {
        return type;
    }

    public Integer getLevel() {
        return level;
    }

    public Integer getBufferSize() {
        return bufferSize;
    }

    /**
     * Returns an {@link DataOutputStream} that compresses given bytes into <code>output</code> {@link ByteBufferOutputStream}
     * with specified <code>magic</code>.
     */
    public DataOutputStream outputStream(ByteBufferOutputStream output, byte magic) {
        return new DataOutputStream(type.wrapForOutput(output, magic, this.level, this.bufferSize));
    }

    /**
     * Creates a not-compressing configuration.
     */
    public static CompressionConfig none() {
        return of(CompressionType.NONE);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, default compression level, and compression buffer size.
     */
    public static CompressionConfig of(CompressionType type) {
        return of(type, null);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, specified compression level, and default compression buffer size.
     */
    public static CompressionConfig of(CompressionType type, Integer level) {
        return of(type, level, null);
    }

    /**
     * Creates a configuration of specified {@link CompressionType}, compression level, and compression buffer size.
     */
    public static CompressionConfig of(CompressionType type, Integer level, Integer bufferSize) {
        return new CompressionConfig(type, level, bufferSize);
    }
}
