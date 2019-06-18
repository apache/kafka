package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor.MemberInfo;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

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
}