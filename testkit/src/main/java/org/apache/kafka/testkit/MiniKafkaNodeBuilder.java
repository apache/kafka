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

package org.apache.kafka.testkit;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds a MiniKafkaNode.
 */
public class MiniKafkaNodeBuilder {
    static final int INVALID_NODE_ID = -1;

    int id = INVALID_NODE_ID;

    Map<String, String> configs = new HashMap<>();

    Collection<MiniKafkaLogDirBuilder> logDirBlds = new ArrayList<>();

    public MiniKafkaNodeBuilder() {
        configs.put("controlled.shutdown.enable", "false"); //KafkaConfig$.MODULE$.ControlledShutdownEnableProp
    }

    public MiniKafkaNodeBuilder config(String key, String value) {
        this.configs.put(key, value);
        return this;
    }

    public MiniKafkaNodeBuilder configs(Map<String, String> configs) {
        this.configs.putAll(configs);
        return this;
    }

    public MiniKafkaNodeBuilder id(int id) {
        this.id = id;
        return this;
    }

    public MiniKafkaNodeBuilder rack(String rack) {
        if (rack == null) {
            configs.remove("broker.rack"); //KafkaConfig$.MODULE$.RackProp
        } else {
            configs.put("broker.rack", rack);
        }
        return this;
    }

    public MiniKafkaNodeBuilder enableControlledShutdown(boolean enabled) {
        configs.put("controlled.shutdown.enable", Boolean.toString(enabled)); //KafkaConfig$.MODULE$.ControlledShutdownEnableProp
        return this;
    }

    public MiniKafkaNodeBuilder addLogDir(MiniKafkaLogDirBuilder logDirBld) {
        this.logDirBlds.add(logDirBld);
        return this;
    }

    MiniKafkaNode build(MiniKafkaClusterBuilder clusterBld, String name, String zkString) {
        return new MiniKafkaNode(clusterBld, this, name, zkString);
    }
}
