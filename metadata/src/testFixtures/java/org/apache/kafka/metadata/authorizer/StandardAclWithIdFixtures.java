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
package org.apache.kafka.metadata.authorizer;

import org.apache.kafka.common.Uuid;

import java.util.List;

public final class StandardAclWithIdFixtures {
    public static final List<StandardAclWithId> TEST_ACLS = List.of(
        new StandardAclWithId(Uuid.fromString("QZDDv-R7SyaPgetDPGd0Mw"), StandardAclFixtures.TEST_ACLS.get(0)),
        new StandardAclWithId(Uuid.fromString("SdDjEdlbRmy2__WFKe3RMg"), StandardAclFixtures.TEST_ACLS.get(1)),
        new StandardAclWithId(Uuid.fromString("wQzt5gkSTwuQNXZF5gIw7A"), StandardAclFixtures.TEST_ACLS.get(2)),
        new StandardAclWithId(Uuid.fromString("ab_5xjJXSbS1o5jGfhgQXg"), StandardAclFixtures.TEST_ACLS.get(3)),
        new StandardAclWithId(Uuid.fromString("wP_cCK0LTEGSX9oDRInJHQ"), StandardAclFixtures.TEST_ACLS.get(4))
    );

    private StandardAclWithIdFixtures() {
    }
}
