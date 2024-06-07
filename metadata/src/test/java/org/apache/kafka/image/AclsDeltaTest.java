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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.AccessControlEntryRecord;
import org.apache.kafka.common.metadata.RemoveAccessControlEntryRecord;
import org.apache.kafka.metadata.authorizer.StandardAcl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Timeout(40)
public class AclsDeltaTest {

    private final Uuid aclId = Uuid.fromString("iOZpss6VQUmD6blnqzl50g");

    @Test
    public void testRemovesDeleteIfNotInImage() {
        AclsImage image = new AclsImage(Collections.emptyMap());
        AclsDelta delta = new AclsDelta(image);
        AccessControlEntryRecord inputAclRecord = testAccessControlEntryRecord();

        assertEquals(0, delta.changes().size());

        delta.replay(inputAclRecord);
        assertEquals(Optional.of(testStandardAcl()), delta.changes().get(aclId));

        RemoveAccessControlEntryRecord inputRemoveAclRecord = testRemoveAccessControlEntryRecord();
        delta.replay(inputRemoveAclRecord);

        assertFalse(delta.changes().containsKey(aclId));
    }

    @Test
    public void testKeepsDeleteIfInImage() {
        Map<Uuid, StandardAcl> initialImageMap = new HashMap<>();
        initialImageMap.put(aclId, testStandardAcl());
        AclsImage image = new AclsImage(initialImageMap);
        AclsDelta delta = new AclsDelta(image);

        assertEquals(0, delta.changes().size());

        RemoveAccessControlEntryRecord removeAccessControlEntryRecord = testRemoveAccessControlEntryRecord();
        delta.replay(removeAccessControlEntryRecord);

        assertTrue(delta.changes().containsKey(aclId));
        assertEquals(Optional.empty(), delta.changes().get(aclId));
    }

    @Test
    public void testThrowsExceptionOnInvalidStateWhenImageIsEmpty() {
        AclsImage image = new AclsImage(Collections.emptyMap());
        AclsDelta delta = new AclsDelta(image);

        RemoveAccessControlEntryRecord removeAccessControlEntryRecord = testRemoveAccessControlEntryRecord();
        assertThrows(IllegalStateException.class, () -> delta.replay(removeAccessControlEntryRecord));
    }

    @Test
    public void testThrowsExceptionOnInvalidStateWhenImageHasOtherAcls() {
        Uuid id = Uuid.fromString("nGiNMQHwRgmgsIlfu73aJQ");
        AccessControlEntryRecord record = new AccessControlEntryRecord();
        record.setId(id);
        record.setResourceType((byte) 1);
        record.setResourceName("foo");
        record.setPatternType((byte) 1);
        record.setPrincipal("User:user");
        record.setHost("host");
        record.setOperation((byte) 1);
        record.setPermissionType((byte) 1);

        Map<Uuid, StandardAcl> initialImageMap = new HashMap<>();
        initialImageMap.put(id, StandardAcl.fromRecord(record));
        AclsImage image = new AclsImage(initialImageMap);
        AclsDelta delta = new AclsDelta(image);

        RemoveAccessControlEntryRecord removeAccessControlEntryRecord = testRemoveAccessControlEntryRecord();
        assertThrows(IllegalStateException.class, () -> delta.replay(removeAccessControlEntryRecord));
    }

    private AccessControlEntryRecord testAccessControlEntryRecord() {
        AccessControlEntryRecord record = new AccessControlEntryRecord();
        record.setId(aclId);
        record.setResourceType((byte) 1);
        record.setResourceName("foo");
        record.setPatternType((byte) 1);
        record.setPrincipal("User:user");
        record.setHost("host");
        record.setOperation((byte) 1);
        record.setPermissionType((byte) 1);
        return record;
    }

    private RemoveAccessControlEntryRecord testRemoveAccessControlEntryRecord() {
        RemoveAccessControlEntryRecord record = new RemoveAccessControlEntryRecord();
        record.setId(aclId);
        return record;
    }

    private StandardAcl testStandardAcl() {
        return StandardAcl.fromRecord(testAccessControlEntryRecord());
    }
}
