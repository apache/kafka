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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


/**
 * A PayloadGenerator which generates pseudo-random payloads based on other PayloadGenerators.
 *
 * Given a seed and non-null list of RandomComponents, RandomComponentPayloadGenerator
 * will use any given generator in its list of components a percentage of the time based on the 
 * percent field in the RandomComponent. These percent fields must be integers greater than 0 
 * and together add up to 100. The payloads generated can be reproduced from run to run.
 * 
 * An example of how to include this generator in a Trogdor taskSpec is shown below.
 * #{@code
 *    "keyGenerator": {
 *        "type": "randomComponent",
 *        "seed": 456,
 *        "components": [
 *          {
 *            "percent": 50,
 *            "component": {
 *              "type": "null"
 *            }
 *          },
 *          {
 *            "percent": 50,
 *            "component": {
 *              "type": "uniformRandom",
 *              "size": 4,
 *              "seed": 123,
 *              "padding": 0
 *            }
 *          }
 *        ]
 *    }
 * }
 */
public class RandomComponentPayloadGenerator implements PayloadGenerator {
    private final long seed;
    private final List<RandomComponent> components;
    private final Random random = new Random();

    @JsonCreator
    public RandomComponentPayloadGenerator(@JsonProperty("seed") long seed,
                                           @JsonProperty("components") List<RandomComponent> components) {
        this.seed = seed;
        if (components == null || components.isEmpty()) {
            throw new IllegalArgumentException("Components must be a specified, non-empty list of RandomComponents.");
        }
        int sum = 0;
        for (RandomComponent component : components) {
            if (component.percent() < 1) {
                throw new IllegalArgumentException("Percent value must be greater than zero.");
            }
            sum += component.percent();
        }
        if (sum != 100) {
            throw new IllegalArgumentException("Components must be a list of RandomComponents such that the percent fields sum to 100");
        }
        this.components = new ArrayList<>(components);
    }

    @JsonProperty
    public long seed() {
        return seed;
    }

    @JsonProperty
    public List<RandomComponent> components() {
        return components;
    }

    @Override
    public byte[] generate(long position) {
        int randPercent;
        synchronized (random) {
            random.setSeed(seed + position);
            randPercent = random.nextInt(100);
        }
        int curPercent = 0;
        RandomComponent com = components.get(0);
        for (RandomComponent component : components) {
            curPercent += component.percent();
            if (curPercent > randPercent) {
                com = component;
                break;
            }
        }
        return com.component().generate(position);
    }
}
