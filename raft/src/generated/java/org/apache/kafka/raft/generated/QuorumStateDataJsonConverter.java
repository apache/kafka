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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.util.ArrayList;
import org.apache.kafka.common.protocol.MessageUtil;

import static org.apache.kafka.raft.generated.QuorumStateData.*;

public class QuorumStateDataJsonConverter {
    public static QuorumStateData read(JsonNode _node, short _version) {
        QuorumStateData _object = new QuorumStateData();
        JsonNode _clusterIdNode = _node.get("clusterId");
        if (_clusterIdNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'clusterId', which is mandatory in version " + _version);
        } else {
            if (!_clusterIdNode.isTextual()) {
                throw new RuntimeException("QuorumStateData expected a string type, but got " + _node.getNodeType());
            }
            _object.clusterId = _clusterIdNode.asText();
        }
        JsonNode _leaderIdNode = _node.get("leaderId");
        if (_leaderIdNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'leaderId', which is mandatory in version " + _version);
        } else {
            _object.leaderId = MessageUtil.jsonNodeToInt(_leaderIdNode, "QuorumStateData");
        }
        JsonNode _leaderEpochNode = _node.get("leaderEpoch");
        if (_leaderEpochNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'leaderEpoch', which is mandatory in version " + _version);
        } else {
            _object.leaderEpoch = MessageUtil.jsonNodeToInt(_leaderEpochNode, "QuorumStateData");
        }
        JsonNode _votedIdNode = _node.get("votedId");
        if (_votedIdNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'votedId', which is mandatory in version " + _version);
        } else {
            _object.votedId = MessageUtil.jsonNodeToInt(_votedIdNode, "QuorumStateData");
        }
        JsonNode _appliedOffsetNode = _node.get("appliedOffset");
        if (_appliedOffsetNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'appliedOffset', which is mandatory in version " + _version);
        } else {
            _object.appliedOffset = MessageUtil.jsonNodeToLong(_appliedOffsetNode, "QuorumStateData");
        }
        JsonNode _currentVotersNode = _node.get("currentVoters");
        if (_currentVotersNode == null) {
            throw new RuntimeException("QuorumStateData: unable to locate field 'currentVoters', which is mandatory in version " + _version);
        } else {
            if (_currentVotersNode.isNull()) {
                _object.currentVoters = null;
            } else {
                if (!_currentVotersNode.isArray()) {
                    throw new RuntimeException("QuorumStateData expected a JSON array, but got " + _node.getNodeType());
                }
                ArrayList<Voter> _collection = new ArrayList<Voter>();
                _object.currentVoters = _collection;
                for (JsonNode _element : _currentVotersNode) {
                    _collection.add(VoterJsonConverter.read(_element, _version));
                }
            }
        }
        return _object;
    }
    public static JsonNode write(QuorumStateData _object, short _version) {
        ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
        _node.set("clusterId", new TextNode(_object.clusterId));
        _node.set("leaderId", new IntNode(_object.leaderId));
        _node.set("leaderEpoch", new IntNode(_object.leaderEpoch));
        _node.set("votedId", new IntNode(_object.votedId));
        _node.set("appliedOffset", new LongNode(_object.appliedOffset));
        if (_object.currentVoters == null) {
            _node.set("currentVoters", NullNode.instance);
        } else {
            ArrayNode _currentVotersArray = new ArrayNode(JsonNodeFactory.instance);
            for (Voter _element : _object.currentVoters) {
                _currentVotersArray.add(VoterJsonConverter.write(_element, _version));
            }
            _node.set("currentVoters", _currentVotersArray);
        }
        return _node;
    }
    
    public static class VoterJsonConverter {
        public static Voter read(JsonNode _node, short _version) {
            Voter _object = new Voter();
            JsonNode _voterIdNode = _node.get("voterId");
            if (_voterIdNode == null) {
                throw new RuntimeException("Voter: unable to locate field 'voterId', which is mandatory in version " + _version);
            } else {
                _object.voterId = MessageUtil.jsonNodeToInt(_voterIdNode, "Voter");
            }
            return _object;
        }
        public static JsonNode write(Voter _object, short _version) {
            ObjectNode _node = new ObjectNode(JsonNodeFactory.instance);
            _node.set("voterId", new IntNode(_object.voterId));
            return _node;
        }
    }
}
