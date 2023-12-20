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
package org.apache.kafka.server.record;

import org.apache.kafka.common.record.CompressionType;

import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public enum BrokerCompressionType {
    UNCOMPRESSED("uncompressed") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return CompressionType.NONE;
        }
    },
    ZSTD("zstd") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return CompressionType.ZSTD;
        }
    },
    LZ4("lz4") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return CompressionType.LZ4;
        }
    },
    SNAPPY("snappy") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return CompressionType.SNAPPY;
        }
    },
    GZIP("gzip") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return CompressionType.GZIP;
        }
    },
    PRODUCER("producer") {
        @Override
        public CompressionType targetCompressionType(CompressionType producerCompressionType) {
            return producerCompressionType;
        }
    };

    private static final List<BrokerCompressionType> VALUES = asList(values());

    public final String name;

    BrokerCompressionType(String name) {
        this.name = name;
    }

    public abstract CompressionType targetCompressionType(CompressionType producerCompressionType);

    public static List<String> names() {
        return VALUES.stream().map(v -> v.name).collect(Collectors.toList());
    }

    public static BrokerCompressionType forName(String n) {
        String name = n.toLowerCase(Locale.ROOT);
        return VALUES.stream().filter(v -> v.name.equals(name)).findFirst().orElseThrow(() ->
            new IllegalArgumentException("Unknown broker compression type name: " + name)
        );
    }
}
