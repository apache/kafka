/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.config.types;

/**
 * A wrapper class for passwords to hide them while logging a config
 */
public class Password {

    public static final String HIDDEN = "[hidden]";

    private final String value;

    /**
     * Construct a new Password object
     * @param value The value of a password
     */
    public Password(String value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Password))
            return false;
        Password other = (Password) obj;
        return value.equals(other.value);
    }

    /**
     * Returns hidden password string
     *
     * @return hidden password string
     */
    @Override
    public String toString() {
        return HIDDEN;
    }

    /**
     * Returns real password string
     *
     * @return real password string
     */
    public String value() {
        return value;
    }
}
