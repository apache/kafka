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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.common.requests.FindCoordinatorRequest;

import java.util.Objects;

public class CoordinatorKey {
    public final String idValue;
    public final FindCoordinatorRequest.CoordinatorType type;

    private CoordinatorKey(FindCoordinatorRequest.CoordinatorType type, String idValue) {
        this.idValue = idValue;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoordinatorKey that = (CoordinatorKey) o;
        return Objects.equals(idValue, that.idValue) &&
            type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(idValue, type);
    }

    @Override
    public String toString() {
        return "CoordinatorKey(" +
            "idValue='" + idValue + '\'' +
            ", type=" + type +
            ')';
    }

    public static CoordinatorKey byGroupId(String groupId) {
        return new CoordinatorKey(FindCoordinatorRequest.CoordinatorType.GROUP, groupId);
    }

    public static CoordinatorKey byTransactionalId(String transactionalId) {
        return new CoordinatorKey(FindCoordinatorRequest.CoordinatorType.TRANSACTION, transactionalId);
    }

}
