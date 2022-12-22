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
package org.apache.kafka.storage.internals.log;

import java.util.Collections;
import java.util.Set;

//todo-refactoring Add respective config changes once https://issues.apache.org/jira/browse/KAFKA-14613 is completed
public class ProducerStateManagerConfig { // extends BrokerReconfigurable
    //todo-refactoring replace later with KafkaConfig.ProducerIdExpirationMsProp
    public static final Set<String> RECONFIGURABLE_CONFIGS = Collections.singleton("producer.id.expiration.ms");
    private volatile int producerIdExpirationMs;

    public ProducerStateManagerConfig(int producerIdExpirationMs) {
        this.producerIdExpirationMs = producerIdExpirationMs;
    }

    public static Set<String> reconfigurableConfigs() {
        //Set(KafkaConfig.ProducerIdExpirationMsProp)
        return null;
    }

    // todo-refactoring
//    public void reconfigure(KafkaConfig oldConfig, KafkaConfig newConfig) {
//        if (producerIdExpirationMs != newConfig.producerIdExpirationMs) {
//            info("Reconfigure ${KafkaConfig.ProducerIdExpirationMsProp} from $producerIdExpirationMs to ${newConfig.producerIdExpirationMs}");
//            producerIdExpirationMs = newConfig.producerIdExpirationMs;
//        }
//    }

//    public void validateReconfiguration(KafkaConfig newConfig) {
//        if (newConfig.producerIdExpirationMs < 0)
//            throw new ConfigException(s"${KafkaConfig.ProducerIdExpirationMsProp} cannot be less than 0, current value is $producerIdExpirationMs")
//    }

    public int producerIdExpirationMs() {
        return producerIdExpirationMs;
    }
}