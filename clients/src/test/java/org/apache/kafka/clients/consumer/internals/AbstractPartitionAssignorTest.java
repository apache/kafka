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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractPartitionAssignorTest {

    @Test
    public void testMemberInfoSortingWithoutGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.empty());
        MemberInfo m2 = new MemberInfo("b", Optional.empty());
        MemberInfo m3 = new MemberInfo("c", Optional.empty());

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(memberInfoList, Utils.sorted(memberInfoList));
    }

    @Test
    public void testMemberInfoSortingWithAllGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.of("y"));
        MemberInfo m2 = new MemberInfo("b", Optional.of("z"));
        MemberInfo m3 = new MemberInfo("c", Optional.of("x"));

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(Arrays.asList(m3, m1, m2), Utils.sorted(memberInfoList));
    }

    @Test
    public void testMemberInfoSortingSomeGroupInstanceId() {
        MemberInfo m1 = new MemberInfo("a", Optional.empty());
        MemberInfo m2 = new MemberInfo("b", Optional.of("y"));
        MemberInfo m3 = new MemberInfo("c", Optional.of("x"));

        List<MemberInfo> memberInfoList = Arrays.asList(m1, m2, m3);
        assertEquals(Arrays.asList(m3, m2, m1), Utils.sorted(memberInfoList));
    }

    @Test
    public void testMergeSortManyMemberInfo() {
        Random rand = new Random();
        int bound = 2;
        List<MemberInfo> memberInfoList = new ArrayList<>();
        List<MemberInfo> staticMemberList = new ArrayList<>();
        List<MemberInfo> dynamicMemberList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // Need to make sure all the ids are defined as 3-digits otherwise
            // the comparison result will break.
            String id = Integer.toString(i + 100);
            Optional<String> groupInstanceId = rand.nextInt(bound) < bound / 2 ?
                                                       Optional.of(id) : Optional.empty();
            MemberInfo m = new MemberInfo(id, groupInstanceId);
            memberInfoList.add(m);
            if (m.groupInstanceId.isPresent()) {
                staticMemberList.add(m);
            } else {
                dynamicMemberList.add(m);
            }
        }
        staticMemberList.addAll(dynamicMemberList);
        Collections.shuffle(memberInfoList);
        assertEquals(staticMemberList, Utils.sorted(memberInfoList));
    }
}
