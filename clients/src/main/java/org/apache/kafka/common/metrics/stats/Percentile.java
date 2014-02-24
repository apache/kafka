/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

public class Percentile {

    private final String name;
    private final String description;
    private final double percentile;

    public Percentile(String name, double percentile) {
        this(name, "", percentile);
    }

    public Percentile(String name, String description, double percentile) {
        super();
        this.name = name;
        this.description = description;
        this.percentile = percentile;
    }

    public String name() {
        return this.name;
    }

    public String description() {
        return this.description;
    }

    public double percentile() {
        return this.percentile;
    }

}
