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
package org.apache.kafka.image;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.metadata.authorizer.StandardAclWithIdFixtures;
import org.apache.kafka.server.common.ApiMessageAndVersion;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class AclsImageFixtures {

    public static final AclsImage IMAGE1 = new AclsImage(createAclMap(0, 4));

    public static final List<ApiMessageAndVersion> DELTA1_RECORDS = List.of(
            new ApiMessageAndVersion(new RemoveAccessControlEntryRecord().
                    setId(Uuid.fromString("QZDDv-R7SyaPgetDPGd0Mw")), (short) 0),
            new ApiMessageAndVersion(StandardAclWithIdFixtures.TEST_ACLS.get(4).toRecord(), (short) 0)
    );

    public static final AclsDelta DELTA1 = RecordTestUtils.replayAll(new AclsDelta(IMAGE1), DELTA1_RECORDS);

    public static final AclsImage IMAGE2 = new AclsImage(createAclMap(1, 5));

    private static Map<Uuid, StandardAcl> createAclMap(int startInclusive, int endExclusive) {
        return IntStream.range(startInclusive, endExclusive)
                .mapToObj(StandardAclWithIdFixtures.TEST_ACLS::get)
                .collect(Collectors.toUnmodifiableMap(StandardAclWithId::id, StandardAclWithId::acl));
    }

    private AclsImageFixtures() {
    }
}
