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

package org.apache.kafka.common.acl;

import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

import java.util.Objects;

public class ResourceIndex {
    private final AccessControlEntry ace;
    private final ResourceType rtype;
    private final PatternType ptype;

    public ResourceIndex(AccessControlEntry ace,
                         ResourceType rtype,
                         PatternType ptype) {
        this.ace = ace;
        this.rtype = rtype;
        this.ptype = ptype;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResourceIndex that = (ResourceIndex) o;
        return ace.equals(that.ace) &&
            rtype == that.rtype &&
            ptype == that.ptype;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ace, rtype, ptype);
    }

    @Override
    public String toString() {
        return "ResourceIndex{" +
            "ace=" + ace +
            ", rtype=" + rtype +
            ", ptype=" + ptype +
            '}';
    }
}
