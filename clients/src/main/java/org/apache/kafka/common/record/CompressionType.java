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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.zip.Deflater;

import static org.apache.kafka.common.config.ConfigDef.Range.between;

/**
 * The compression type to use
 */
public enum CompressionType {
    NONE((byte) 0, "none", 1.0f),

    // Shipped with the JDK
    GZIP((byte) 1, "gzip", 1.0f) {
        public static final int MIN_LEVEL = Deflater.BEST_SPEED;
        public static final int MAX_LEVEL = Deflater.BEST_COMPRESSION;
        public static final int DEFAULT_LEVEL = Deflater.DEFAULT_COMPRESSION;

        @Override
        public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        public ConfigDef.Validator levelValidator() {
            return ConfigDef.LambdaValidator.with((name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int level = ((Number) value).intValue();
                if (level > MAX_LEVEL || (level < MIN_LEVEL && level != DEFAULT_LEVEL)) {
                    throw new ConfigException(name, value, "Value must be between " + MIN_LEVEL + " and " + MAX_LEVEL + " or equal to " + DEFAULT_LEVEL);
                }
            }, () -> "[" + MIN_LEVEL + ",...," + MAX_LEVEL + "] or " + DEFAULT_LEVEL);
        }
    },

    // We should only load classes from a given compression library when we actually use said compression library. This
    // is because compression libraries include native code for a set of platforms and we want to avoid errors
    // in case the platform is not supported and the compression library is not actually used.
    // To ensure this, we only reference compression library code from classes that are only invoked when actual usage
    // happens.
    SNAPPY((byte) 2, "snappy", 1.0f),
    LZ4((byte) 3, "lz4", 1.0f) {
        // These values come from net.jpountz.lz4.LZ4Constants
        // We may need to update them if the lz4 library changes these values.
        private static final int MIN_LEVEL = 1;
        private static final int MAX_LEVEL = 17;
        private static final int DEFAULT_LEVEL = 9;

        @Override
        public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        public ConfigDef.Validator levelValidator() {
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    },
    ZSTD((byte) 4, "zstd", 1.0f) {
        // These values come from the zstd library. We don't use the Zstd.minCompressionLevel(),
        // Zstd.maxCompressionLevel() and Zstd.defaultCompressionLevel() methods to not load the Zstd library
        // while parsing configuration.
        // See ZSTD_minCLevel in https://github.com/facebook/zstd/blob/dev/lib/compress/zstd_compress.c#L6987
        // and ZSTD_TARGETLENGTH_MAX https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L1249
        private static final int MIN_LEVEL = -131072;
        // See ZSTD_MAX_CLEVEL in https://github.com/facebook/zstd/blob/dev/lib/compress/clevels.h#L19
        private static final int MAX_LEVEL = 22;
        // See ZSTD_CLEVEL_DEFAULT in https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L129
        private static final int DEFAULT_LEVEL = 3;

        @Override
        public int defaultLevel() {
            return DEFAULT_LEVEL;
        }

        @Override
        public int maxLevel() {
            return MAX_LEVEL;
        }

        @Override
        public int minLevel() {
            return MIN_LEVEL;
        }

        @Override
        public ConfigDef.Validator levelValidator() {
            return between(MIN_LEVEL, MAX_LEVEL);
        }
    };

    // compression type is represented by two bits in the attributes field of the record batch header, so `byte` is
    // large enough
    public final byte id;
    public final String name;
    public final float rate;

    CompressionType(byte id, String name, float rate) {
        this.id = id;
        this.name = name;
        this.rate = rate;
    }

    public static CompressionType forId(int id) {
        switch (id) {
            case 0:
                return NONE;
            case 1:
                return GZIP;
            case 2:
                return SNAPPY;
            case 3:
                return LZ4;
            case 4:
                return ZSTD;
            default:
                throw new IllegalArgumentException("Unknown compression type id: " + id);
        }
    }

    public static CompressionType forName(String name) {
        if (NONE.name.equals(name))
            return NONE;
        else if (GZIP.name.equals(name))
            return GZIP;
        else if (SNAPPY.name.equals(name))
            return SNAPPY;
        else if (LZ4.name.equals(name))
            return LZ4;
        else if (ZSTD.name.equals(name))
            return ZSTD;
        else
            throw new IllegalArgumentException("Unknown compression name: " + name);
    }

    public int defaultLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public int maxLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public int minLevel() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator levelValidator() {
        throw new UnsupportedOperationException("Compression levels are not defined for this compression type: " + name);
    }

    @Override
    public String toString() {
        return name;
    }

}
