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
        // As of KIP-780, the default compression buffer size of GZIPOutputStream is 512 bytes.
        // see: java.util.zip.GZIPOutputStream#Constructor(OutputStream)
        public static final int DEFAULT_BUFFER = 8 * 1024;
        public static final int DEFAULT_STRATEGY = Deflater.DEFAULT_STRATEGY;

        @Override
        public int defaultBuffer() {
            return DEFAULT_BUFFER;
        }

        @Override
        public ConfigDef.Validator bufferValidator() {
            return ConfigDef.Range.atLeast(512); // matches GZIPOutputStream default
        }

        @Override
        public int defaultStrategy() {
            return DEFAULT_STRATEGY;
        }

        @Override
        public ConfigDef.Validator strategyValidator() {
            // Accept only DEFAULT_STRATEGY, FILTERED, HUFFMAN_ONLY
            return ConfigDef.LambdaValidator.with((name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int s = ((Number) value).intValue();
                if (s != Deflater.DEFAULT_STRATEGY &&
                    s != Deflater.FILTERED &&
                    s != Deflater.HUFFMAN_ONLY) {
                    throw new ConfigException(
                        name, value,
                        "Value must be one of Deflater.DEFAULT_STRATEGY (" + Deflater.DEFAULT_STRATEGY + "), " +
                            "Deflater.FILTERED (" + Deflater.FILTERED + "), or Deflater.HUFFMAN_ONLY (" + Deflater.HUFFMAN_ONLY + ")"
                    );
                }
            }, () -> "one of {DEFAULT_STRATEGY=" + Deflater.DEFAULT_STRATEGY +
                ", FILTERED=" + Deflater.FILTERED +
                ", HUFFMAN_ONLY=" + Deflater.HUFFMAN_ONLY + "}");
        }

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
    SNAPPY((byte) 2, "snappy", 1.0f) {
        // As of KIP-780, the compression block size of SnappyOutputStream is allowed within [1024, 536870912].
        // see: org.xerial.snappy,SnappyOutputStream#Constructor(OutputStream, int, BufferAllocatorFactory)
        public static final int MIN_BLOCK = 1024;
        public static final int MAX_BLOCK = 536870912;
        // As of KIP-780, the default buffer size ofSnappyOutputStream is 32768 bytes.
        // see: org.xerial.snappy,SnappyOutputStream#Constructor(OutputStream)
        public static final int DEFAULT_BLOCK = 32768;

        @Override
        public int minBlockSize() {
            return MIN_BLOCK;
        }

        @Override
        public int maxBlockSize() {
            return MAX_BLOCK;
        }

        @Override
        public int defaultBlockSize() {
            return DEFAULT_BLOCK;
        }

        @Override
        public ConfigDef.Validator blockSizeValidator() {
            return between(MIN_BLOCK, MAX_BLOCK);
        }
    },
    LZ4((byte) 3, "lz4", 1.0f) {
        // These values come from net.jpountz.lz4.LZ4Constants
        // We may need to update them if the lz4 library changes these values.
        private static final int MIN_LEVEL = 1;
        private static final int MAX_LEVEL = 17;
        private static final int DEFAULT_LEVEL = 9;

        // LZ4 blocks: 4=64kb, 5=256kb, 6=1mb, 7=4mb.
        public static final int MIN_BLOCK = 4;
        public static final int MAX_BLOCK = 7;
        public static final int DEFAULT_BLOCK = 4;

        @Override
        public int minBlockSize() {
            return MIN_BLOCK;
        }

        @Override
        public int maxBlockSize() {
            return MAX_BLOCK;
        }

        @Override
        public int defaultBlockSize() {
            return DEFAULT_BLOCK;
        }

        @Override
        public ConfigDef.Validator blockSizeValidator() {
            return between(MIN_BLOCK, MAX_BLOCK);
        }

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
        // See ZSTD_CLEVEL_DEFAULT in https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L134
        private static final int DEFAULT_LEVEL = 3;

        // Advanced compression parameters :
        // It's possible to pin down compression parameters to some specific values.
        // In which case, these values are no longer dynamically selected by the compressor */
        // ZSTD_c_windowLog=101, - Maximum allowed back-reference distance, expressed as power of 2.
        // This will set a memory budget for streaming decompression,
        // with larger values requiring more memory
        // and typically compressing more.
        // Must be clamped between ZSTD_WINDOWLOG_MIN and ZSTD_WINDOWLOG_MAX.// Special: value 0 means "use default windowLog".
        // Note: Using a windowLog greater than ZSTD_WINDOWLOG_LIMIT_DEFAULT
        // requires explicitly allowing such size at streaming decompression stage.
        public static final int DEFAULT_WINDOW = 0;
        // ZSTD_WINDOWLOG_MIN - https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L1266
        public static final int MIN_WINDOW = 10;
        // ZSTD_WINDOWLOG_LIMIT_DEFAULT - https://github.com/facebook/zstd/blob/dev/lib/zstd.h#L1287
        public static final int MAX_WINDOW = 27;

        // Default value is `0`, aka "single-threaded mode" : no worker is spawned,
        // compression is performed inside Caller's thread, and all invocations are blocking */
        public static final int DEFAULT_WORKERS = 0;

        // More workers improve speed, but also increase memory usage.
        public static final int MIN_WORKERS = 2;
        public static final int MAX_WORKERS = 16;

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

        @Override
        public int minWindowSize() {
            return MIN_WINDOW;
        }

        @Override
        public int maxWindowSize() {
            return MAX_WINDOW;
        }

        @Override
        public int defaultWindowSize() {
            return DEFAULT_WINDOW;
        }


        @Override
        public ConfigDef.Validator windowSizeValidator() {
            // Accept 0 (sentinel) or a value in [MIN_WINDOW, MAX_WINDOW]
            return (name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int v;
                try {
                    v = (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new ConfigException(name, value, "Value must be an integer");
                }
                if (v == DEFAULT_WINDOW) return; // allow 0 sentinel
                ConfigDef.Range.between(MIN_WINDOW, MAX_WINDOW).ensureValid(name, v);
            };
        }

        @Override
        public int defaultWorkers() {
            return DEFAULT_WORKERS;
        }

        @Override
        public int minWorkers() {
            return MIN_WORKERS;
        }

        @Override
        public int maxWorkers() {
            return MAX_WORKERS;
        }

        @Override
        public ConfigDef.Validator workersValidator() {
            return (name, value) -> {
                if (value == null)
                    throw new ConfigException(name, null, "Value must be non-null");
                int v;
                try {
                    v = (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new ConfigException(name, value, "Value must be an integer");
                }
                if (v == DEFAULT_WORKERS) {
                    // accept default i.e., 0
                    return;
                }
                ConfigDef.Range.between(MIN_WORKERS, MAX_WORKERS).ensureValid(name, v);
            };
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

    public int defaultBuffer() {
        throw new UnsupportedOperationException("Compression buffers are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator bufferValidator() {
        throw new UnsupportedOperationException("Compression buffers are not defined for this compression type: " + name);
    }

    public int defaultStrategy() {
        throw new UnsupportedOperationException("Compression strategy are not defined for this compression type: " + name);
    }

    public int defaultBlockSize() {
        throw new UnsupportedOperationException("Compression block size are not defined for this compression type: " + name);
    }

    public int minBlockSize() {
        throw new UnsupportedOperationException("Compression block size are not defined for this compression type: " + name);
    }

    public int maxBlockSize() {
        throw new UnsupportedOperationException("Compression block size are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator blockSizeValidator() {
        throw new UnsupportedOperationException("Compression block size are not defined for this compression type: " + name);
    }

    public int minWindowSize() {
        throw new UnsupportedOperationException("Compression window size are not defined for this compression type: " + name);
    }

    public int maxWindowSize() {
        throw new UnsupportedOperationException("Compression window size are not defined for this compression type: " + name);
    }

    public int defaultWindowSize() {
        throw new UnsupportedOperationException("Compression window size are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator windowSizeValidator() {
        throw new UnsupportedOperationException("Compression window size are not defined for this compression type: " + name);
    }

    public int defaultWorkers() {
        throw new UnsupportedOperationException("Compression workers are not defined in this compression type: " + name);
    }

    public int minWorkers() {
        throw new UnsupportedOperationException("Compression workers are not defined for this compression type: " + name);
    }

    public int maxWorkers() {
        throw new UnsupportedOperationException("Compression workers are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator workersValidator() {
        throw new UnsupportedOperationException("Compression workers are not defined for this compression type: " + name);
    }

    public ConfigDef.Validator strategyValidator() {
        throw new UnsupportedOperationException("Compression strategy is not defined for this compression type: " + name);
    }

    @Override
    public String toString() {
        return name;
    }

}
