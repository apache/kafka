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
package org.apache.kafka.coordinator.common.runtime;

import java.nio.ByteBuffer;

/**
 * Deserializer to translates bytes to T.
 *
 * @param <T> The record type.
 */
public interface Deserializer<T> {
    /**
     * UnknownRecordTypeException is thrown when the Deserializer encounters
     * an unknown record type.
     */
    class UnknownRecordTypeException extends RuntimeException {
        private final short unknownType;

        public UnknownRecordTypeException(short unknownType) {
            super(String.format("Found an unknown record type %d", unknownType));
            this.unknownType = unknownType;
        }

        public short unknownType() {
            return unknownType;
        }
    }

    class UnknownRecordVersionException extends RuntimeException {
        private final short type;
        private final short unknownVersion;

        public UnknownRecordVersionException(short type, short unknownVersion) {
            super(String.format("Found an unknown record version %d for %d type", unknownVersion, type));
            this.type = type;
            this.unknownVersion = unknownVersion;
        }

        public short type() {
            return type;
        }

        public short unknownVersion() {
            return unknownVersion;
        }
    }

    /**
     * Deserializes the key and the value.
     *
     * @param key   The key or null if not present.
     * @param value The value or null if not present.
     * @return The record.
     */
    T deserialize(ByteBuffer key, ByteBuffer value) throws RuntimeException;
}
