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

package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Contains a percent value represented as an integer between 1 and 100 and a PayloadGenerator to specify
 * how often that PayloadGenerator should be used. 
 */
public class RandomComponent {
    private final int percent;
    private final PayloadGenerator component;
    

    @JsonCreator
    public RandomComponent(@JsonProperty("percent") int percent,
                           @JsonProperty("component") PayloadGenerator component) {
        this.percent = percent;
        this.component = component;
    }

    @JsonProperty
    public int percent() {
        return percent;
    }

    @JsonProperty
    public PayloadGenerator component() {
        return component;
    }
}

