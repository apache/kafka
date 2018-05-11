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
package org.apache.kafka.connect.runtime.errors;

import org.apache.kafka.connect.data.Struct;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class Stage implements Structable {

    private final StageType type;
    private final Map<String, Object> config;
    private final Class<?> klass;

    public static Builder newBuilder(StageType type) {
        return new Builder(type);
    }

    private Stage(StageType type, Map<String, Object> config, Class<?> klass) {
        this.type = type;
        this.config = config;
        this.klass = klass;
    }

    /**
     * @return at what stage in processing did the error happen
     */
    @Field("type")
    public StageType type() {
        return type;
    }

    /**
     * @return name of the class executing this stage.
     */
    @Field("class")
    public Class<?> executingClass() {
        return klass;
    }

    /**
     * @return properties used to configure this stage
     */
    @Field("config")
    public Map<String, Object> config() {
        return config;
    }

    @Override
    public Struct toStruct() {
        return null;
    }

    @Override
    public String toString() {
        return "Stage{" + toStruct() + "}";
    }

    public static class Builder {
        private StageType type;
        private Map<String, Object> config;
        private Class<?> klass;

        private Builder(StageType type) {
            this.type = type;
        }

        public Builder setExecutingClass(Class<?> klass) {
            this.klass = klass;
            return this;
        }

        public Builder setConfig(Map<String, Object> config) {
            this.config = config;
            return this;
        }

        public Stage build() {
            Objects.requireNonNull(type);
            return new Stage(type, config == null ? Collections.<String, Object>emptyMap() : config, klass);
        }
    }

}
