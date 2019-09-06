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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ApplicableTransformationConfig extends AbstractConfig {

    public static final String TOPICS_CONFIG = "topics";
    private static final String TOPICS_DOC = "Topic names for which this transformation will be applied.";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS_CONFIG, Type.LIST, Collections.emptyList(), Importance.HIGH, TOPICS_DOC);

    public ApplicableTransformationConfig(Map<String, ?> props) {
        super(CONFIG_DEF, props, true);
    }

    public List<String> getTopics() {
        return this.getList(TOPICS_CONFIG);
    }

}
