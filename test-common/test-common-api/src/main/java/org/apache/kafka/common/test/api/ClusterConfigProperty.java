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

package org.apache.kafka.common.test.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Target({ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface ClusterConfigProperty {
    /**
     * The config applies to the controller/broker with specified id. Default is -1, indicating the property applied to
     * all controller/broker servers. Note that the "controller" here refers to the KRaft quorum controller.
     * The id can vary depending on the different {@link org.apache.kafka.common.test.api.Type}.
     * <ul>
     *  <li> Under {@link org.apache.kafka.common.test.api.Type#KRAFT}, the broker id starts from
     *  {@link org.apache.kafka.common.test.TestKitNodes#BROKER_ID_OFFSET 0}, the controller id
     *  starts from {@link org.apache.kafka.common.test.TestKitNodes#CONTROLLER_ID_OFFSET 3000}
     *  and increases by 1 with each addition broker/controller.</li>
     *  <li> Under {@link org.apache.kafka.common.test.api.Type#CO_KRAFT}, the broker id and controller id both start from
     *  {@link org.apache.kafka.common.test.TestKitNodes#BROKER_ID_OFFSET 0}
     *  and increases by 1 with each additional broker/controller.</li>
     * </ul>
     *
     * If the id doesn't correspond to any broker/controller server, throw IllegalArgumentException
     * @return the controller/broker id
     */
    int id() default -1;
    String key();
    String value();
}
