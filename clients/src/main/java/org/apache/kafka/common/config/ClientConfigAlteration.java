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

package org.apache.kafka.common.config;

import org.apache.kafka.common.quota.ClientQuotaEntity;
import java.util.Collection;
import java.util.Objects;

/**
 * Describes a configuration alteration to be made to a client config entity.
 */
public class ClientConfigAlteration {

    public static class Op {
        private final String key;
        private final String value;

        /**
         * @param key the config type to alter
         * @param value if set then the existing value is updated,
         *              otherwise if null, the existing value is cleared
         */
        public Op(String key, String value) {
            this.key = key;
            this.value = value;
        }

        /**
         * @return the config type to alter
         */
        public String key() {
            return this.key;
        }

        /**
         * @return if set then the existing value is updated,
         *         otherwise if null, the existing value is cleared
         */
        public String value() {
            return this.value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Op that = (Op) o;
            return Objects.equals(key, that.key) && Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "ClientConfigAlteration.Op(key=" + key + ", value=" + value + ")";
        }
    }

    private final ClientQuotaEntity entity;
    private final Collection<Op> ops;

    /**
     * @param entity the entity whose config will be modified
     * @param ops the alteration to perform
     */
    public ClientConfigAlteration(ClientQuotaEntity entity, Collection<Op> ops) {
        this.entity = entity;
        this.ops = ops;
    }

    /**
     * @return the entity whose config will be modified
     */
    public ClientQuotaEntity entity() {
        return this.entity;
    }

    /**
     * @return the alteration to perform
     */
    public Collection<Op> ops() {
        return this.ops;
    }

    @Override
    public String toString() {
        return "ClientConfigAlteration(entity=" + entity + ", ops=" + ops + ")";
    }
}
