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

package org.apache.kafka.raft.generated;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.CompactArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;

import static java.util.Map.Entry;
import static org.apache.kafka.common.protocol.types.Field.TaggedFieldsSection;


public class QuorumStateData implements ApiMessage {
    String clusterId;
    int leaderId;
    int leaderEpoch;
    int votedId;
    long appliedOffset;
    List<Voter> currentVoters;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("cluster_id", Type.COMPACT_STRING, ""),
            new Field("leader_id", Type.INT32, ""),
            new Field("leader_epoch", Type.INT32, ""),
            new Field("voted_id", Type.INT32, ""),
            new Field("applied_offset", Type.INT64, ""),
            new Field("current_voters", CompactArrayOf.nullable(Voter.SCHEMA_0), ""),
            TaggedFieldsSection.of(
            )
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 0;
    
    public QuorumStateData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public QuorumStateData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public QuorumStateData() {
        this.clusterId = "";
        this.leaderId = -1;
        this.leaderEpoch = -1;
        this.votedId = -1;
        this.appliedOffset = 0L;
        this.currentVoters = new ArrayList<Voter>(0);
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
        {
            int length;
            length = _readable.readUnsignedVarint() - 1;
            if (length < 0) {
                throw new RuntimeException("non-nullable field clusterId was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field clusterId had invalid length " + length);
            } else {
                this.clusterId = _readable.readString(length);
            }
        }
        this.leaderId = _readable.readInt();
        this.leaderEpoch = _readable.readInt();
        this.votedId = _readable.readInt();
        this.appliedOffset = _readable.readLong();
        {
            int arrayLength;
            arrayLength = _readable.readUnsignedVarint() - 1;
            if (arrayLength < 0) {
                this.currentVoters = null;
            } else {
                ArrayList<Voter> newCollection = new ArrayList<Voter>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new Voter(_readable, _version));
                }
                this.currentVoters = newCollection;
            }
        }
        this._unknownTaggedFields = null;
        int _numTaggedFields = _readable.readUnsignedVarint();
        for (int _i = 0; _i < _numTaggedFields; _i++) {
            int _tag = _readable.readUnsignedVarint();
            int _size = _readable.readUnsignedVarint();
            switch (_tag) {
                default:
                    this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                    break;
            }
        }
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = _cache.getSerializedValue(clusterId);
            _writable.writeUnsignedVarint(_stringBytes.length + 1);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeInt(leaderId);
        _writable.writeInt(leaderEpoch);
        _writable.writeInt(votedId);
        _writable.writeLong(appliedOffset);
        if (currentVoters == null) {
            _writable.writeUnsignedVarint(0);
        } else {
            _writable.writeUnsignedVarint(currentVoters.size() + 1);
            for (Voter currentVotersElement : currentVoters) {
                currentVotersElement.write(_writable, _cache, _version);
            }
        }
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        _writable.writeUnsignedVarint(_numTaggedFields);
        _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        NavigableMap<Integer, Object> _taggedFields = null;
        this._unknownTaggedFields = null;
        _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
        this.clusterId = struct.getString("cluster_id");
        this.leaderId = struct.getInt("leader_id");
        this.leaderEpoch = struct.getInt("leader_epoch");
        this.votedId = struct.getInt("voted_id");
        this.appliedOffset = struct.getLong("applied_offset");
        {
            Object[] _nestedObjects = struct.getArray("current_voters");
            if (_nestedObjects == null) {
                this.currentVoters = null;
            } else {
                this.currentVoters = new ArrayList<Voter>(_nestedObjects.length);
                for (Object nestedObject : _nestedObjects) {
                    this.currentVoters.add(new Voter((Struct) nestedObject, _version));
                }
            }
        }
        if (!_taggedFields.isEmpty()) {
            this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
            for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
            }
        }
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        _taggedFields = new TreeMap<>();
        Struct struct = new Struct(SCHEMAS[_version]);
        struct.set("cluster_id", this.clusterId);
        struct.set("leader_id", this.leaderId);
        struct.set("leader_epoch", this.leaderEpoch);
        struct.set("voted_id", this.votedId);
        struct.set("applied_offset", this.appliedOffset);
        {
            if (currentVoters == null) {
                struct.set("current_voters", null);
            } else {
                Struct[] _nestedObjects = new Struct[currentVoters.size()];
                int i = 0;
                for (Voter element : this.currentVoters) {
                    _nestedObjects[i++] = element.toStruct(_version);
                }
                struct.set("current_voters", (Object[]) _nestedObjects);
            }
        }
        struct.set("_tagged_fields", _taggedFields);
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        {
            byte[] _stringBytes = clusterId.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'clusterId' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(clusterId, _stringBytes);
            _size += _stringBytes.length + ByteUtils.sizeOfUnsignedVarint(_stringBytes.length + 1);
        }
        _size += 4;
        _size += 4;
        _size += 4;
        _size += 8;
        if (currentVoters == null) {
            _size += 1;
        } else {
            int _arraySize = 0;
            _arraySize += ByteUtils.sizeOfUnsignedVarint(currentVoters.size() + 1);
            for (Voter currentVotersElement : currentVoters) {
                _arraySize += currentVotersElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof QuorumStateData)) return false;
        QuorumStateData other = (QuorumStateData) obj;
        if (this.clusterId == null) {
            if (other.clusterId != null) return false;
        } else {
            if (!this.clusterId.equals(other.clusterId)) return false;
        }
        if (leaderId != other.leaderId) return false;
        if (leaderEpoch != other.leaderEpoch) return false;
        if (votedId != other.votedId) return false;
        if (appliedOffset != other.appliedOffset) return false;
        if (this.currentVoters == null) {
            if (other.currentVoters != null) return false;
        } else {
            if (!this.currentVoters.equals(other.currentVoters)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (clusterId == null ? 0 : clusterId.hashCode());
        hashCode = 31 * hashCode + leaderId;
        hashCode = 31 * hashCode + leaderEpoch;
        hashCode = 31 * hashCode + votedId;
        hashCode = 31 * hashCode + ((int) (appliedOffset >> 32) ^ (int) appliedOffset);
        hashCode = 31 * hashCode + (currentVoters == null ? 0 : currentVoters.hashCode());
        return hashCode;
    }
    
    @Override
    public QuorumStateData duplicate() {
        QuorumStateData _duplicate = new QuorumStateData();
        _duplicate.clusterId = clusterId;
        _duplicate.leaderId = leaderId;
        _duplicate.leaderEpoch = leaderEpoch;
        _duplicate.votedId = votedId;
        _duplicate.appliedOffset = appliedOffset;
        if (currentVoters == null) {
            _duplicate.currentVoters = null;
        } else {
            ArrayList<Voter> newCurrentVoters = new ArrayList<Voter>(currentVoters.size());
            for (Voter _element : currentVoters) {
                newCurrentVoters.add(_element.duplicate());
            }
            _duplicate.currentVoters = newCurrentVoters;
        }
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "QuorumStateData("
            + "clusterId=" + ((clusterId == null) ? "null" : "'" + clusterId.toString() + "'")
            + ", leaderId=" + leaderId
            + ", leaderEpoch=" + leaderEpoch
            + ", votedId=" + votedId
            + ", appliedOffset=" + appliedOffset
            + ", currentVoters=" + ((currentVoters == null) ? "null" : MessageUtil.deepToString(currentVoters.iterator()))
            + ")";
    }
    
    public String clusterId() {
        return this.clusterId;
    }
    
    public int leaderId() {
        return this.leaderId;
    }
    
    public int leaderEpoch() {
        return this.leaderEpoch;
    }
    
    public int votedId() {
        return this.votedId;
    }
    
    public long appliedOffset() {
        return this.appliedOffset;
    }
    
    public List<Voter> currentVoters() {
        return this.currentVoters;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public QuorumStateData setClusterId(String v) {
        this.clusterId = v;
        return this;
    }
    
    public QuorumStateData setLeaderId(int v) {
        this.leaderId = v;
        return this;
    }
    
    public QuorumStateData setLeaderEpoch(int v) {
        this.leaderEpoch = v;
        return this;
    }
    
    public QuorumStateData setVotedId(int v) {
        this.votedId = v;
        return this;
    }
    
    public QuorumStateData setAppliedOffset(long v) {
        this.appliedOffset = v;
        return this;
    }
    
    public QuorumStateData setCurrentVoters(List<Voter> v) {
        this.currentVoters = v;
        return this;
    }
    
    public static class Voter implements Message {
        int voterId;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("voter_id", Type.INT32, ""),
                TaggedFieldsSection.of(
                )
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 0;
        
        public Voter(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public Voter(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public Voter() {
            this.voterId = 0;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 32767;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            this.voterId = _readable.readInt();
            this._unknownTaggedFields = null;
            int _numTaggedFields = _readable.readUnsignedVarint();
            for (int _i = 0; _i < _numTaggedFields; _i++) {
                int _tag = _readable.readUnsignedVarint();
                int _size = _readable.readUnsignedVarint();
                switch (_tag) {
                    default:
                        this._unknownTaggedFields = _readable.readUnknownTaggedField(this._unknownTaggedFields, _tag, _size);
                        break;
                }
            }
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            _writable.writeInt(voterId);
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            _writable.writeUnsignedVarint(_numTaggedFields);
            _rawWriter.writeRawTags(_writable, Integer.MAX_VALUE);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            NavigableMap<Integer, Object> _taggedFields = null;
            this._unknownTaggedFields = null;
            _taggedFields = (NavigableMap<Integer, Object>) struct.get("_tagged_fields");
            this.voterId = struct.getInt("voter_id");
            if (!_taggedFields.isEmpty()) {
                this._unknownTaggedFields = new ArrayList<>(_taggedFields.size());
                for (Entry<Integer, Object> entry : _taggedFields.entrySet()) {
                    this._unknownTaggedFields.add((RawTaggedField) entry.getValue());
                }
            }
        }
        
        @Override
        public Struct toStruct(short _version) {
            TreeMap<Integer, Object> _taggedFields = null;
            _taggedFields = new TreeMap<>();
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("voter_id", this.voterId);
            struct.set("_tagged_fields", _taggedFields);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            _size += 4;
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            _size += ByteUtils.sizeOfUnsignedVarint(_numTaggedFields);
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Voter)) return false;
            Voter other = (Voter) obj;
            if (voterId != other.voterId) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + voterId;
            return hashCode;
        }
        
        @Override
        public Voter duplicate() {
            Voter _duplicate = new Voter();
            _duplicate.voterId = voterId;
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "Voter("
                + "voterId=" + voterId
                + ")";
        }
        
        public int voterId() {
            return this.voterId;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public Voter setVoterId(int v) {
            this.voterId = v;
            return this;
        }
    }
}
