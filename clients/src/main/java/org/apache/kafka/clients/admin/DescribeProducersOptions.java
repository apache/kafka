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
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.annotation.InterfaceStability;

import java.util.Collection;
import java.util.Objects;
import java.util.OptionalInt;

/**
 * Options for {@link Admin#describeProducers(Collection)}.
 *
 * The API of this class is evolving, see {@link Admin} for details.
 */
@InterfaceStability.Evolving
public class DescribeProducersOptions extends AbstractOptions<DescribeProducersOptions> {
    private OptionalInt brokerId = OptionalInt.empty();

    public DescribeProducersOptions brokerId(int brokerId) {
        this.brokerId = OptionalInt.of(brokerId);
        return this;
    }

    public OptionalInt brokerId() {
        return brokerId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DescribeProducersOptions that = (DescribeProducersOptions) o;
        return Objects.equals(brokerId, that.brokerId) &&
            Objects.equals(timeoutMs, that.timeoutMs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerId, timeoutMs);
    }

    @Override
    public String toString() {
        return "DescribeProducersOptions(" +
            "brokerId=" + brokerId +
            ", timeoutMs=" + timeoutMs +
            ')';
    }
}
