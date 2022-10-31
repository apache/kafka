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
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.apache.kafka.metadata.authorizer.StandardAclWithId;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.metadata.authorizer.StandardAclWithIdTest.TEST_ACLS;
import static org.junit.jupiter.api.Assertions.assertEquals;


@Timeout(value = 40)
public class AclsImageTest {
    final static AclsImage IMAGE1;

    final static List<ApiMessageAndVersion> DELTA1_RECORDS;

    final static AclsDelta DELTA1;

    final static AclsImage IMAGE2;

    static {
        Map<Uuid, StandardAcl> map = new HashMap<>();
        for (int i = 0; i < 4; i++) {
            StandardAclWithId aclWithId = TEST_ACLS.get(i);
            map.put(aclWithId.id(), aclWithId.acl());
        }
        IMAGE1 = new AclsImage(map);

        DELTA1_RECORDS = new ArrayList<>();
        DELTA1_RECORDS.add(new ApiMessageAndVersion(new RemoveAccessControlEntryRecord().
            setId(Uuid.fromString("QZDDv-R7SyaPgetDPGd0Mw")), (short) 0));
        DELTA1_RECORDS.add(new ApiMessageAndVersion(TEST_ACLS.get(4).toRecord(), (short) 0));

        DELTA1 = new AclsDelta(IMAGE1);
        RecordTestUtils.replayAll(DELTA1, DELTA1_RECORDS);


        Map<Uuid, StandardAcl> map2 = new HashMap<>();
        for (int i = 1; i < 5; i++) {
            StandardAclWithId aclWithId = TEST_ACLS.get(i);
            map2.put(aclWithId.id(), aclWithId.acl());
        }
        IMAGE2 = new AclsImage(map2);
    }

    @Test
    public void testEmptyImageRoundTrip() throws Throwable {
        testToImageAndBack(AclsImage.EMPTY);
    }

    @Test
    public void testImage1RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE1);
    }

    @Test
    public void testApplyDelta1() throws Throwable {
        assertEquals(IMAGE2, DELTA1.apply());
    }

    @Test
    public void testImage2RoundTrip() throws Throwable {
        testToImageAndBack(IMAGE2);
    }

    private void testToImageAndBack(AclsImage image) throws Throwable {
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder().build());
        AclsDelta delta = new AclsDelta(AclsImage.EMPTY);
        RecordTestUtils.replayAll(delta, writer.records());
        AclsImage nextImage = delta.apply();
        assertEquals(image, nextImage);
    }
}
