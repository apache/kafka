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

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package kafka.internals.generated;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageSizeAccumulator;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;


public class TransactionLogValue implements ApiMessage {
    long producerId;
    short producerEpoch;
    int transactionTimeoutMs;
    byte transactionStatus;
    List<PartitionsSchema> transactionPartitions;
    long transactionLastUpdateTimestampMs;
    long transactionStartTimestampMs;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("producer_id", Type.INT64, "Producer id in use by the transactional id"),
            new Field("producer_epoch", Type.INT16, "Epoch associated with the producer id"),
            new Field("transaction_timeout_ms", Type.INT32, "Transaction timeout in milliseconds"),
            new Field("transaction_status", Type.INT8, "TransactionState the transaction is in"),
            new Field("transaction_partitions", ArrayOf.nullable(PartitionsSchema.SCHEMA_0), "Set of partitions involved in the transaction"),
            new Field("transaction_last_update_timestamp_ms", Type.INT64, "Time the transaction was last updated"),
            new Field("transaction_start_timestamp_ms", Type.INT64, "Time the transaction was started")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public TransactionLogValue(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public TransactionLogValue() {
        this.producerId = 0L;
        this.producerEpoch = (short) 0;
        this.transactionTimeoutMs = 0;
        this.transactionStatus = (byte) 0;
        this.transactionPartitions = new ArrayList<PartitionsSchema>(0);
        this.transactionLastUpdateTimestampMs = 0L;
        this.transactionStartTimestampMs = 0L;
    }
    
    @Override
    public short apiKey() {
        return -1;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        this.producerId = _readable.readLong();
        this.producerEpoch = _readable.readShort();
        this.transactionTimeoutMs = _readable.readInt();
        this.transactionStatus = _readable.readByte();
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                this.transactionPartitions = null;
            } else {
                ArrayList<PartitionsSchema> newCollection = new ArrayList<PartitionsSchema>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new PartitionsSchema(_readable, _version));
                }
                this.transactionPartitions = newCollection;
            }
        }
        this.transactionLastUpdateTimestampMs = _readable.readLong();
        this.transactionStartTimestampMs = _readable.readLong();
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeLong(producerId);
        _writable.writeShort(producerEpoch);
        _writable.writeInt(transactionTimeoutMs);
        _writable.writeByte(transactionStatus);
        if (transactionPartitions == null) {
            _writable.writeInt(-1);
        } else {
            _writable.writeInt(transactionPartitions.size());
            for (PartitionsSchema transactionPartitionsElement : transactionPartitions) {
                transactionPartitionsElement.write(_writable, _cache, _version);
            }
        }
        _writable.writeLong(transactionLastUpdateTimestampMs);
        _writable.writeLong(transactionStartTimestampMs);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _size.addBytes(8);
        _size.addBytes(2);
        _size.addBytes(4);
        _size.addBytes(1);
        if (transactionPartitions == null) {
            _size.addBytes(4);
        } else {
            _size.addBytes(4);
            for (PartitionsSchema transactionPartitionsElement : transactionPartitions) {
                transactionPartitionsElement.addSize(_size, _cache, _version);
            }
        }
        _size.addBytes(8);
        _size.addBytes(8);
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                _size.addBytes(_field.size());
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TransactionLogValue)) return false;
        TransactionLogValue other = (TransactionLogValue) obj;
        if (producerId != other.producerId) return false;
        if (producerEpoch != other.producerEpoch) return false;
        if (transactionTimeoutMs != other.transactionTimeoutMs) return false;
        if (transactionStatus != other.transactionStatus) return false;
        if (this.transactionPartitions == null) {
            if (other.transactionPartitions != null) return false;
        } else {
            if (!this.transactionPartitions.equals(other.transactionPartitions)) return false;
        }
        if (transactionLastUpdateTimestampMs != other.transactionLastUpdateTimestampMs) return false;
        if (transactionStartTimestampMs != other.transactionStartTimestampMs) return false;
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + ((int) (producerId >> 32) ^ (int) producerId);
        hashCode = 31 * hashCode + producerEpoch;
        hashCode = 31 * hashCode + transactionTimeoutMs;
        hashCode = 31 * hashCode + transactionStatus;
        hashCode = 31 * hashCode + (transactionPartitions == null ? 0 : transactionPartitions.hashCode());
        hashCode = 31 * hashCode + ((int) (transactionLastUpdateTimestampMs >> 32) ^ (int) transactionLastUpdateTimestampMs);
        hashCode = 31 * hashCode + ((int) (transactionStartTimestampMs >> 32) ^ (int) transactionStartTimestampMs);
        return hashCode;
    }
    
    @Override
    public TransactionLogValue duplicate() {
        TransactionLogValue _duplicate = new TransactionLogValue();
        _duplicate.producerId = producerId;
        _duplicate.producerEpoch = producerEpoch;
        _duplicate.transactionTimeoutMs = transactionTimeoutMs;
        _duplicate.transactionStatus = transactionStatus;
        if (transactionPartitions == null) {
            _duplicate.transactionPartitions = null;
        } else {
            ArrayList<PartitionsSchema> newTransactionPartitions = new ArrayList<PartitionsSchema>(transactionPartitions.size());
            for (PartitionsSchema _element : transactionPartitions) {
                newTransactionPartitions.add(_element.duplicate());
            }
            _duplicate.transactionPartitions = newTransactionPartitions;
        }
        _duplicate.transactionLastUpdateTimestampMs = transactionLastUpdateTimestampMs;
        _duplicate.transactionStartTimestampMs = transactionStartTimestampMs;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "TransactionLogValue("
            + "producerId=" + producerId
            + ", producerEpoch=" + producerEpoch
            + ", transactionTimeoutMs=" + transactionTimeoutMs
            + ", transactionStatus=" + transactionStatus
            + ", transactionPartitions=" + ((transactionPartitions == null) ? "null" : MessageUtil.deepToString(transactionPartitions.iterator()))
            + ", transactionLastUpdateTimestampMs=" + transactionLastUpdateTimestampMs
            + ", transactionStartTimestampMs=" + transactionStartTimestampMs
            + ")";
    }
    
    public long producerId() {
        return this.producerId;
    }
    
    public short producerEpoch() {
        return this.producerEpoch;
    }
    
    public int transactionTimeoutMs() {
        return this.transactionTimeoutMs;
    }
    
    public byte transactionStatus() {
        return this.transactionStatus;
    }
    
    public List<PartitionsSchema> transactionPartitions() {
        return this.transactionPartitions;
    }
    
    public long transactionLastUpdateTimestampMs() {
        return this.transactionLastUpdateTimestampMs;
    }
    
    public long transactionStartTimestampMs() {
        return this.transactionStartTimestampMs;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public TransactionLogValue setProducerId(long v) {
        this.producerId = v;
        return this;
    }
    
    public TransactionLogValue setProducerEpoch(short v) {
        this.producerEpoch = v;
        return this;
    }
    
    public TransactionLogValue setTransactionTimeoutMs(int v) {
        this.transactionTimeoutMs = v;
        return this;
    }
    
    public TransactionLogValue setTransactionStatus(byte v) {
        this.transactionStatus = v;
        return this;
    }
    
    public TransactionLogValue setTransactionPartitions(List<PartitionsSchema> v) {
        this.transactionPartitions = v;
        return this;
    }
    
    public TransactionLogValue setTransactionLastUpdateTimestampMs(long v) {
        this.transactionLastUpdateTimestampMs = v;
        return this;
    }
    
    public TransactionLogValue setTransactionStartTimestampMs(long v) {
        this.transactionStartTimestampMs = v;
        return this;
    }
    
    public static class PartitionsSchema implements Message {
        String topic;
        List<Integer> partitionIds;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("topic", Type.STRING, ""),
                new Field("partition_ids", new ArrayOf(Type.INT32), "")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public PartitionsSchema(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public PartitionsSchema() {
            this.topic = "";
            this.partitionIds = new ArrayList<Integer>(0);
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field topic was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field topic had invalid length " + length);
                } else {
                    this.topic = _readable.readString(length);
                }
            }
            {
                int arrayLength;
                arrayLength = _readable.readInt();
                if (arrayLength < 0) {
                    throw new RuntimeException("non-nullable field partitionIds was serialized as null");
                } else {
                    ArrayList<Integer> newCollection = new ArrayList<Integer>(arrayLength);
                    for (int i = 0; i < arrayLength; i++) {
                        newCollection.add(_readable.readInt());
                    }
                    this.partitionIds = newCollection;
                }
            }
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(topic);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeInt(partitionIds.size());
            for (Integer partitionIdsElement : partitionIds) {
                _writable.writeInt(partitionIdsElement);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @Override
        public void addSize(MessageSizeAccumulator _size, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = topic.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'topic' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(topic, _stringBytes);
                _size.addBytes(_stringBytes.length + 2);
            }
            {
                _size.addBytes(4);
                _size.addBytes(partitionIds.size() * 4);
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.tag()));
                    _size.addBytes(ByteUtils.sizeOfUnsignedVarint(_field.size()));
                    _size.addBytes(_field.size());
                }
            }
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof PartitionsSchema)) return false;
            PartitionsSchema other = (PartitionsSchema) obj;
            if (this.topic == null) {
                if (other.topic != null) return false;
            } else {
                if (!this.topic.equals(other.topic)) return false;
            }
            if (this.partitionIds == null) {
                if (other.partitionIds != null) return false;
            } else {
                if (!this.partitionIds.equals(other.partitionIds)) return false;
            }
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (topic == null ? 0 : topic.hashCode());
            hashCode = 31 * hashCode + (partitionIds == null ? 0 : partitionIds.hashCode());
            return hashCode;
        }
        
        @Override
        public PartitionsSchema duplicate() {
            PartitionsSchema _duplicate = new PartitionsSchema();
            _duplicate.topic = topic;
            ArrayList<Integer> newPartitionIds = new ArrayList<Integer>(partitionIds.size());
            for (Integer _element : partitionIds) {
                newPartitionIds.add(_element);
            }
            _duplicate.partitionIds = newPartitionIds;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "PartitionsSchema("
                + "topic=" + ((topic == null) ? "null" : "'" + topic.toString() + "'")
                + ", partitionIds=" + MessageUtil.deepToString(partitionIds.iterator())
                + ")";
        }
        
        public String topic() {
            return this.topic;
        }
        
        public List<Integer> partitionIds() {
            return this.partitionIds;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public PartitionsSchema setTopic(String v) {
            this.topic = v;
            return this;
        }
        
        public PartitionsSchema setPartitionIds(List<Integer> v) {
            this.partitionIds = v;
            return this;
        }
    }
}
