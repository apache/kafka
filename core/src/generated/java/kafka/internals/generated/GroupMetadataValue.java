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
import java.util.Arrays;
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
import org.apache.kafka.common.utils.Bytes;


public class GroupMetadataValue implements ApiMessage {
    String protocolType;
    int generation;
    String protocol;
    String leader;
    long currentStateTimestamp;
    List<MemberMetadata> members;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("protocol_type", Type.STRING, ""),
            new Field("generation", Type.INT32, ""),
            new Field("protocol", Type.NULLABLE_STRING, ""),
            new Field("leader", Type.NULLABLE_STRING, ""),
            new Field("members", new ArrayOf(MemberMetadata.SCHEMA_0), "")
        );
    
    public static final Schema SCHEMA_1 =
        new Schema(
            new Field("protocol_type", Type.STRING, ""),
            new Field("generation", Type.INT32, ""),
            new Field("protocol", Type.NULLABLE_STRING, ""),
            new Field("leader", Type.NULLABLE_STRING, ""),
            new Field("members", new ArrayOf(MemberMetadata.SCHEMA_1), "")
        );
    
    public static final Schema SCHEMA_2 =
        new Schema(
            new Field("protocol_type", Type.STRING, ""),
            new Field("generation", Type.INT32, ""),
            new Field("protocol", Type.NULLABLE_STRING, ""),
            new Field("leader", Type.NULLABLE_STRING, ""),
            new Field("current_state_timestamp", Type.INT64, ""),
            new Field("members", new ArrayOf(MemberMetadata.SCHEMA_1), "")
        );
    
    public static final Schema SCHEMA_3 =
        new Schema(
            new Field("protocol_type", Type.STRING, ""),
            new Field("generation", Type.INT32, ""),
            new Field("protocol", Type.NULLABLE_STRING, ""),
            new Field("leader", Type.NULLABLE_STRING, ""),
            new Field("current_state_timestamp", Type.INT64, ""),
            new Field("members", new ArrayOf(MemberMetadata.SCHEMA_3), "")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0,
        SCHEMA_1,
        SCHEMA_2,
        SCHEMA_3
    };
    
    public static final short LOWEST_SUPPORTED_VERSION = 0;
    public static final short HIGHEST_SUPPORTED_VERSION = 3;
    
    public GroupMetadataValue(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public GroupMetadataValue() {
        this.protocolType = "";
        this.generation = 0;
        this.protocol = "";
        this.leader = "";
        this.currentStateTimestamp = -1L;
        this.members = new ArrayList<MemberMetadata>(0);
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
        return 3;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                throw new RuntimeException("non-nullable field protocolType was serialized as null");
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field protocolType had invalid length " + length);
            } else {
                this.protocolType = _readable.readString(length);
            }
        }
        this.generation = _readable.readInt();
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                this.protocol = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field protocol had invalid length " + length);
            } else {
                this.protocol = _readable.readString(length);
            }
        }
        {
            int length;
            length = _readable.readShort();
            if (length < 0) {
                this.leader = null;
            } else if (length > 0x7fff) {
                throw new RuntimeException("string field leader had invalid length " + length);
            } else {
                this.leader = _readable.readString(length);
            }
        }
        if (_version >= 2) {
            this.currentStateTimestamp = _readable.readLong();
        } else {
            this.currentStateTimestamp = -1L;
        }
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field members was serialized as null");
            } else {
                ArrayList<MemberMetadata> newCollection = new ArrayList<MemberMetadata>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new MemberMetadata(_readable, _version));
                }
                this.members = newCollection;
            }
        }
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        {
            byte[] _stringBytes = _cache.getSerializedValue(protocolType);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        _writable.writeInt(generation);
        if (protocol == null) {
            _writable.writeShort((short) -1);
        } else {
            byte[] _stringBytes = _cache.getSerializedValue(protocol);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        if (leader == null) {
            _writable.writeShort((short) -1);
        } else {
            byte[] _stringBytes = _cache.getSerializedValue(leader);
            _writable.writeShort((short) _stringBytes.length);
            _writable.writeByteArray(_stringBytes);
        }
        if (_version >= 2) {
            _writable.writeLong(currentStateTimestamp);
        }
        _writable.writeInt(members.size());
        for (MemberMetadata membersElement : members) {
            membersElement.write(_writable, _cache, _version);
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
            byte[] _stringBytes = protocolType.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'protocolType' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(protocolType, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        _size.addBytes(4);
        if (protocol == null) {
            _size.addBytes(2);
        } else {
            byte[] _stringBytes = protocol.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'protocol' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(protocol, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        if (leader == null) {
            _size.addBytes(2);
        } else {
            byte[] _stringBytes = leader.getBytes(StandardCharsets.UTF_8);
            if (_stringBytes.length > 0x7fff) {
                throw new RuntimeException("'leader' field is too long to be serialized");
            }
            _cache.cacheSerializedValue(leader, _stringBytes);
            _size.addBytes(_stringBytes.length + 2);
        }
        if (_version >= 2) {
            _size.addBytes(8);
        }
        {
            _size.addBytes(4);
            for (MemberMetadata membersElement : members) {
                membersElement.addSize(_size, _cache, _version);
            }
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
        if (!(obj instanceof GroupMetadataValue)) return false;
        GroupMetadataValue other = (GroupMetadataValue) obj;
        if (this.protocolType == null) {
            if (other.protocolType != null) return false;
        } else {
            if (!this.protocolType.equals(other.protocolType)) return false;
        }
        if (generation != other.generation) return false;
        if (this.protocol == null) {
            if (other.protocol != null) return false;
        } else {
            if (!this.protocol.equals(other.protocol)) return false;
        }
        if (this.leader == null) {
            if (other.leader != null) return false;
        } else {
            if (!this.leader.equals(other.leader)) return false;
        }
        if (currentStateTimestamp != other.currentStateTimestamp) return false;
        if (this.members == null) {
            if (other.members != null) return false;
        } else {
            if (!this.members.equals(other.members)) return false;
        }
        return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (protocolType == null ? 0 : protocolType.hashCode());
        hashCode = 31 * hashCode + generation;
        hashCode = 31 * hashCode + (protocol == null ? 0 : protocol.hashCode());
        hashCode = 31 * hashCode + (leader == null ? 0 : leader.hashCode());
        hashCode = 31 * hashCode + ((int) (currentStateTimestamp >> 32) ^ (int) currentStateTimestamp);
        hashCode = 31 * hashCode + (members == null ? 0 : members.hashCode());
        return hashCode;
    }
    
    @Override
    public GroupMetadataValue duplicate() {
        GroupMetadataValue _duplicate = new GroupMetadataValue();
        _duplicate.protocolType = protocolType;
        _duplicate.generation = generation;
        if (protocol == null) {
            _duplicate.protocol = null;
        } else {
            _duplicate.protocol = protocol;
        }
        if (leader == null) {
            _duplicate.leader = null;
        } else {
            _duplicate.leader = leader;
        }
        _duplicate.currentStateTimestamp = currentStateTimestamp;
        ArrayList<MemberMetadata> newMembers = new ArrayList<MemberMetadata>(members.size());
        for (MemberMetadata _element : members) {
            newMembers.add(_element.duplicate());
        }
        _duplicate.members = newMembers;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "GroupMetadataValue("
            + "protocolType=" + ((protocolType == null) ? "null" : "'" + protocolType.toString() + "'")
            + ", generation=" + generation
            + ", protocol=" + ((protocol == null) ? "null" : "'" + protocol.toString() + "'")
            + ", leader=" + ((leader == null) ? "null" : "'" + leader.toString() + "'")
            + ", currentStateTimestamp=" + currentStateTimestamp
            + ", members=" + MessageUtil.deepToString(members.iterator())
            + ")";
    }
    
    public String protocolType() {
        return this.protocolType;
    }
    
    public int generation() {
        return this.generation;
    }
    
    public String protocol() {
        return this.protocol;
    }
    
    public String leader() {
        return this.leader;
    }
    
    public long currentStateTimestamp() {
        return this.currentStateTimestamp;
    }
    
    public List<MemberMetadata> members() {
        return this.members;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public GroupMetadataValue setProtocolType(String v) {
        this.protocolType = v;
        return this;
    }
    
    public GroupMetadataValue setGeneration(int v) {
        this.generation = v;
        return this;
    }
    
    public GroupMetadataValue setProtocol(String v) {
        this.protocol = v;
        return this;
    }
    
    public GroupMetadataValue setLeader(String v) {
        this.leader = v;
        return this;
    }
    
    public GroupMetadataValue setCurrentStateTimestamp(long v) {
        this.currentStateTimestamp = v;
        return this;
    }
    
    public GroupMetadataValue setMembers(List<MemberMetadata> v) {
        this.members = v;
        return this;
    }
    
    public static class MemberMetadata implements Message {
        String memberId;
        String groupInstanceId;
        String clientId;
        String clientHost;
        int rebalanceTimeout;
        int sessionTimeout;
        byte[] subscription;
        byte[] assignment;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("member_id", Type.STRING, ""),
                new Field("client_id", Type.STRING, ""),
                new Field("client_host", Type.STRING, ""),
                new Field("session_timeout", Type.INT32, ""),
                new Field("subscription", Type.BYTES, ""),
                new Field("assignment", Type.BYTES, "")
            );
        
        public static final Schema SCHEMA_1 =
            new Schema(
                new Field("member_id", Type.STRING, ""),
                new Field("client_id", Type.STRING, ""),
                new Field("client_host", Type.STRING, ""),
                new Field("rebalance_timeout", Type.INT32, ""),
                new Field("session_timeout", Type.INT32, ""),
                new Field("subscription", Type.BYTES, ""),
                new Field("assignment", Type.BYTES, "")
            );
        
        public static final Schema SCHEMA_2 = SCHEMA_1;
        
        public static final Schema SCHEMA_3 =
            new Schema(
                new Field("member_id", Type.STRING, ""),
                new Field("group_instance_id", Type.NULLABLE_STRING, ""),
                new Field("client_id", Type.STRING, ""),
                new Field("client_host", Type.STRING, ""),
                new Field("rebalance_timeout", Type.INT32, ""),
                new Field("session_timeout", Type.INT32, ""),
                new Field("subscription", Type.BYTES, ""),
                new Field("assignment", Type.BYTES, "")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0,
            SCHEMA_1,
            SCHEMA_2,
            SCHEMA_3
        };
        
        public static final short LOWEST_SUPPORTED_VERSION = 0;
        public static final short HIGHEST_SUPPORTED_VERSION = 3;
        
        public MemberMetadata(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public MemberMetadata() {
            this.memberId = "";
            this.groupInstanceId = null;
            this.clientId = "";
            this.clientHost = "";
            this.rebalanceTimeout = 0;
            this.sessionTimeout = 0;
            this.subscription = Bytes.EMPTY;
            this.assignment = Bytes.EMPTY;
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 3;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field memberId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field memberId had invalid length " + length);
                } else {
                    this.memberId = _readable.readString(length);
                }
            }
            if (_version >= 3) {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    this.groupInstanceId = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field groupInstanceId had invalid length " + length);
                } else {
                    this.groupInstanceId = _readable.readString(length);
                }
            } else {
                this.groupInstanceId = null;
            }
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field clientId was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field clientId had invalid length " + length);
                } else {
                    this.clientId = _readable.readString(length);
                }
            }
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field clientHost was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field clientHost had invalid length " + length);
                } else {
                    this.clientHost = _readable.readString(length);
                }
            }
            if (_version >= 1) {
                this.rebalanceTimeout = _readable.readInt();
            } else {
                this.rebalanceTimeout = 0;
            }
            this.sessionTimeout = _readable.readInt();
            {
                int length;
                length = _readable.readInt();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field subscription was serialized as null");
                } else {
                    byte[] newBytes = new byte[length];
                    _readable.readArray(newBytes);
                    this.subscription = newBytes;
                }
            }
            {
                int length;
                length = _readable.readInt();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field assignment was serialized as null");
                } else {
                    byte[] newBytes = new byte[length];
                    _readable.readArray(newBytes);
                    this.assignment = newBytes;
                }
            }
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(memberId);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            if (_version >= 3) {
                if (groupInstanceId == null) {
                    _writable.writeShort((short) -1);
                } else {
                    byte[] _stringBytes = _cache.getSerializedValue(groupInstanceId);
                    _writable.writeShort((short) _stringBytes.length);
                    _writable.writeByteArray(_stringBytes);
                }
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientId);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            {
                byte[] _stringBytes = _cache.getSerializedValue(clientHost);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            if (_version >= 1) {
                _writable.writeInt(rebalanceTimeout);
            }
            _writable.writeInt(sessionTimeout);
            _writable.writeInt(subscription.length);
            _writable.writeByteArray(subscription);
            _writable.writeInt(assignment.length);
            _writable.writeByteArray(assignment);
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
                byte[] _stringBytes = memberId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'memberId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(memberId, _stringBytes);
                _size.addBytes(_stringBytes.length + 2);
            }
            if (_version >= 3) {
                if (groupInstanceId == null) {
                    _size.addBytes(2);
                } else {
                    byte[] _stringBytes = groupInstanceId.getBytes(StandardCharsets.UTF_8);
                    if (_stringBytes.length > 0x7fff) {
                        throw new RuntimeException("'groupInstanceId' field is too long to be serialized");
                    }
                    _cache.cacheSerializedValue(groupInstanceId, _stringBytes);
                    _size.addBytes(_stringBytes.length + 2);
                }
            }
            {
                byte[] _stringBytes = clientId.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientId' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientId, _stringBytes);
                _size.addBytes(_stringBytes.length + 2);
            }
            {
                byte[] _stringBytes = clientHost.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'clientHost' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(clientHost, _stringBytes);
                _size.addBytes(_stringBytes.length + 2);
            }
            if (_version >= 1) {
                _size.addBytes(4);
            }
            _size.addBytes(4);
            {
                _size.addBytes(subscription.length);
                _size.addBytes(4);
            }
            {
                _size.addBytes(assignment.length);
                _size.addBytes(4);
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
            if (!(obj instanceof MemberMetadata)) return false;
            MemberMetadata other = (MemberMetadata) obj;
            if (this.memberId == null) {
                if (other.memberId != null) return false;
            } else {
                if (!this.memberId.equals(other.memberId)) return false;
            }
            if (this.groupInstanceId == null) {
                if (other.groupInstanceId != null) return false;
            } else {
                if (!this.groupInstanceId.equals(other.groupInstanceId)) return false;
            }
            if (this.clientId == null) {
                if (other.clientId != null) return false;
            } else {
                if (!this.clientId.equals(other.clientId)) return false;
            }
            if (this.clientHost == null) {
                if (other.clientHost != null) return false;
            } else {
                if (!this.clientHost.equals(other.clientHost)) return false;
            }
            if (rebalanceTimeout != other.rebalanceTimeout) return false;
            if (sessionTimeout != other.sessionTimeout) return false;
            if (!Arrays.equals(this.subscription, other.subscription)) return false;
            if (!Arrays.equals(this.assignment, other.assignment)) return false;
            return MessageUtil.compareRawTaggedFields(_unknownTaggedFields, other._unknownTaggedFields);
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (memberId == null ? 0 : memberId.hashCode());
            hashCode = 31 * hashCode + (groupInstanceId == null ? 0 : groupInstanceId.hashCode());
            hashCode = 31 * hashCode + (clientId == null ? 0 : clientId.hashCode());
            hashCode = 31 * hashCode + (clientHost == null ? 0 : clientHost.hashCode());
            hashCode = 31 * hashCode + rebalanceTimeout;
            hashCode = 31 * hashCode + sessionTimeout;
            hashCode = 31 * hashCode + Arrays.hashCode(subscription);
            hashCode = 31 * hashCode + Arrays.hashCode(assignment);
            return hashCode;
        }
        
        @Override
        public MemberMetadata duplicate() {
            MemberMetadata _duplicate = new MemberMetadata();
            _duplicate.memberId = memberId;
            if (groupInstanceId == null) {
                _duplicate.groupInstanceId = null;
            } else {
                _duplicate.groupInstanceId = groupInstanceId;
            }
            _duplicate.clientId = clientId;
            _duplicate.clientHost = clientHost;
            _duplicate.rebalanceTimeout = rebalanceTimeout;
            _duplicate.sessionTimeout = sessionTimeout;
            _duplicate.subscription = MessageUtil.duplicate(subscription);
            _duplicate.assignment = MessageUtil.duplicate(assignment);
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "MemberMetadata("
                + "memberId=" + ((memberId == null) ? "null" : "'" + memberId.toString() + "'")
                + ", groupInstanceId=" + ((groupInstanceId == null) ? "null" : "'" + groupInstanceId.toString() + "'")
                + ", clientId=" + ((clientId == null) ? "null" : "'" + clientId.toString() + "'")
                + ", clientHost=" + ((clientHost == null) ? "null" : "'" + clientHost.toString() + "'")
                + ", rebalanceTimeout=" + rebalanceTimeout
                + ", sessionTimeout=" + sessionTimeout
                + ", subscription=" + Arrays.toString(subscription)
                + ", assignment=" + Arrays.toString(assignment)
                + ")";
        }
        
        public String memberId() {
            return this.memberId;
        }
        
        public String groupInstanceId() {
            return this.groupInstanceId;
        }
        
        public String clientId() {
            return this.clientId;
        }
        
        public String clientHost() {
            return this.clientHost;
        }
        
        public int rebalanceTimeout() {
            return this.rebalanceTimeout;
        }
        
        public int sessionTimeout() {
            return this.sessionTimeout;
        }
        
        public byte[] subscription() {
            return this.subscription;
        }
        
        public byte[] assignment() {
            return this.assignment;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public MemberMetadata setMemberId(String v) {
            this.memberId = v;
            return this;
        }
        
        public MemberMetadata setGroupInstanceId(String v) {
            this.groupInstanceId = v;
            return this;
        }
        
        public MemberMetadata setClientId(String v) {
            this.clientId = v;
            return this;
        }
        
        public MemberMetadata setClientHost(String v) {
            this.clientHost = v;
            return this;
        }
        
        public MemberMetadata setRebalanceTimeout(int v) {
            this.rebalanceTimeout = v;
            return this;
        }
        
        public MemberMetadata setSessionTimeout(int v) {
            this.sessionTimeout = v;
            return this;
        }
        
        public MemberMetadata setSubscription(byte[] v) {
            this.subscription = v;
            return this;
        }
        
        public MemberMetadata setAssignment(byte[] v) {
            this.assignment = v;
            return this;
        }
    }
}
