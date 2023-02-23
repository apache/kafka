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


package kafka.admin;

public class IgnorePrefixRackIdMapper implements RackAwareReplicaAssignmentRackIdMapper {
    /**
     * A simple transformer that returns everything after the first "::"
     * For example, "::123", "123", "AA::123" would all return 123, and "A::B::123" would return "B::123"
     */
    @Override
    public String apply(String rackId) {
        final String[] split = rackId.split("::", 2);
        return split[split.length - 1];
    }
}
