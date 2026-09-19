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

package org.apache.kafka.server.util;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.test.TestUtils;

import com.yammer.metrics.core.Gauge;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public final class ServerTestUtils {

    /**
     * Clear all the yammer metrics.
     */
    public static void clearYammerMetrics() {
        KafkaYammerMetrics.defaultRegistry().allMetrics().keySet().forEach(
                metricName -> KafkaYammerMetrics.defaultRegistry().removeMetric(metricName));
    }

    /**
     * Fetch the gauge value from the yammer metrics.
     *
     * @param name The name of the metric.
     * @return The gauge value as a number.
     */
    public static Number yammerMetricValue(String name) {
        Gauge<?> gauge = (Gauge<?>) KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet().stream()
            .filter(e -> e.getKey().getMBeanName().contains(name))
            .findFirst()
            .orElseThrow()
            .getValue();
        return (Number) gauge.value();
    }

    /**
     * Wait until the ACLs for the given resource match the expected set.
     *
     * @param expected                 the expected set of access control entries.
     * @param authorizer               the authorizer to query.
     * @param resource                 the resource pattern to filter ACLs on.
     * @param accessControlEntryFilter additional filter for the access control entries.
     */
    public static void waitAndVerifyAcls(Set<AccessControlEntry> expected,
                                         Authorizer authorizer,
                                         ResourcePattern resource,
                                         AccessControlEntryFilter accessControlEntryFilter) throws InterruptedException {
        String newLine = System.lineSeparator();
        String delimiter = newLine + "\t";
        AclBindingFilter filter = new AclBindingFilter(resource.toFilter(), accessControlEntryFilter);
        TestUtils.waitForCondition(() -> StreamSupport.stream(authorizer.acls(filter).spliterator(), false)
                        .map(AclBinding::entry)
                        .collect(Collectors.toSet()).equals(expected),
                45000,
                () -> "expected acls:" + expected.stream().map(Object::toString).collect(Collectors.joining(delimiter, delimiter, newLine))
                        + "but got:" + StreamSupport.stream(authorizer.acls(filter).spliterator(), false)
                        .map(b -> b.entry().toString()).collect(Collectors.joining(delimiter, delimiter, newLine)));
    }

}
