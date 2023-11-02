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

package org.apache.kafka.controller;

import java.util.Objects;


class BrokerControlStates {
    private final BrokerControlState current;
    private final BrokerControlState next;

    BrokerControlStates(BrokerControlState current, BrokerControlState next) {
        this.current = current;
        this.next = next;
    }

    BrokerControlState current() {
        return current;
    }

    BrokerControlState next() {
        return next;
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, next);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof BrokerControlStates)) return false;
        BrokerControlStates other = (BrokerControlStates) o;
        return other.current == current && other.next == next;
    }

    @Override
    public String toString() {
        return "BrokerControlStates(current=" + current + ", next=" + next + ")";
    }
}
