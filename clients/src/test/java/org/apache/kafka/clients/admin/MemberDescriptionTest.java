package org.apache.kafka.clients.admin;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MemberDescriptionTest {

    private static final String memberId = "memberId";
    private static final Optional<String> groupInstanceId = Optional.of("instanceId");
    private static final String clientId = "clientId";
    private static final String host = "host";
    private static final MemberAssignment assignment;
    private static final MemberDescription staticMemberDescription;

    static {
       assignment = new MemberAssignment(Collections.singleton(new TopicPartition("topic", 1)));
       staticMemberDescription = new MemberDescription(memberId,
                                                       groupInstanceId,
                                                       clientId,
                                                       host,
                                                       assignment);
    }

    @Test
    public void testEqualsWithoutGroupInstanceId() {
        MemberDescription dynamicMemberDescription = new MemberDescription(memberId,
                                                                           clientId,
                                                                           host,
                                                                           assignment);

        MemberDescription identityDescription = new MemberDescription(memberId,
                                                                      clientId,
                                                                      host,
                                                                      assignment);

        assertNotEquals(staticMemberDescription, dynamicMemberDescription);
        assertNotEquals(staticMemberDescription.hashCode(), dynamicMemberDescription.hashCode());

        // Check self equality.
        assertEquals(dynamicMemberDescription, dynamicMemberDescription);
        assertEquals(dynamicMemberDescription, identityDescription);
        assertEquals(dynamicMemberDescription.hashCode(), identityDescription.hashCode());
    }

    @Test
    public void testEqualsWithGroupInstanceId() {
        // Check self equality.
        assertEquals(staticMemberDescription, staticMemberDescription);

        MemberDescription identityDescription = new MemberDescription(memberId,
                                                                      groupInstanceId,
                                                                      clientId,
                                                                      host,
                                                                      assignment);

        assertEquals(staticMemberDescription, identityDescription);
        assertEquals(staticMemberDescription.hashCode(), identityDescription.hashCode());
    }

    @Test
    public void testNonEqual() {
        MemberDescription newMemberDescription = new MemberDescription("new_member",
                                                                       groupInstanceId,
                                                                       clientId,
                                                                       host,
                                                                       assignment);

        assertNotEquals(staticMemberDescription, newMemberDescription);
        assertNotEquals(staticMemberDescription.hashCode(), newMemberDescription.hashCode());

        MemberDescription newInstanceDescription = new MemberDescription(memberId,
                                                                         Optional.of("new_instance"),
                                                                         clientId,
                                                                         host,
                                                                         assignment);

        assertNotEquals(staticMemberDescription, newInstanceDescription);
        assertNotEquals(staticMemberDescription.hashCode(), newInstanceDescription.hashCode());
    }
}