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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.InetAddress;

import static org.apache.kafka.metadata.authorizer.CidrUtils.isInRange;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@Timeout(value = 40)
public class CidrUtilsTest {

    @Test
    public void testIsInRangeNullOrNonCidr() {
        assertFalse(isInRange("192.168.1.1", null));
        assertFalse(isInRange("192.168.1.1", "192.168.1.1"));
    }

    @Test
    public void testIpv4MappedAddress() throws Exception {
        // JVM normalizes ::ffff:x.x.x.x to plain IPv4 Inet4Address,
        // so CidrUtils dispatches to SubnetUtils (IPv4 path), not SubnetUtils6.
        String normalizedHost = InetAddress.getByName("::ffff:192.168.1.5").getHostAddress();
        assertEquals("192.168.1.5", normalizedHost);
        assertTrue(isInRange(normalizedHost, "192.168.1.0/24"));
        assertFalse(isInRange(normalizedHost, "10.0.0.0/8"));
    }

    @Test
    public void testIpv4MappedAddressWithSubnet() {
        // ::ffff:x.x.x.x/N: JVM normalizes ::ffff:x.x.x.x to Inet4Address, isIpv6() returns false,
        // SubnetUtils rejects the ::ffff: prefix. No client can ever match.
        assertFalse(isInRange("192.168.1.5", "::ffff:192.168.1.0/24"));
        assertFalse(isInRange("::1", "::ffff:192.168.1.0/24"));
        assertFalse(isInRange("2001:db8::1", "::ffff:192.168.1.0/24"));

        // ::x.x.x.x/N (deprecated IPv4-compatible form): JVM keeps it as Inet6Address,
        // so CidrUtils dispatches to SubnetUtils6. Plain IPv4 clients won't match.
        assertFalse(isInRange("192.168.1.5", "::192.168.1.0/24"));
        // IPv6 clients within the /24 range do match.
        assertTrue(isInRange("::1", "::192.168.1.0/24"));
        assertTrue(isInRange("0:0:0:0:0:0:c0a8:105", "::192.168.1.0/24"));
        // IPv6 clients outside the /24 range do not match.
        assertFalse(isInRange("2001:db8::1", "::192.168.1.0/24"));
    }
}