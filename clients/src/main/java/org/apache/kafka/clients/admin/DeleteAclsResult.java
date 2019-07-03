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

package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The result of the {@link AdminClient#deleteAcls(Collection)} call.
 *
 * The API of this class is evolving, see {@link AdminClient} for details.
 */
@InterfaceStability.Evolving
public class DeleteAclsResult {

    /**
     * A class containing either the deleted ACL binding or an exception if the delete failed.
     */
    public static class FilterResult {
        private final AclBinding binding;
        private final ApiException exception;

        FilterResult(AclBinding binding, ApiException exception) {
            this.binding = binding;
            this.exception = exception;
        }

        /**
         * Return the deleted ACL binding or null if there was an error.
         */
        public AclBinding binding() {
            return binding;
        }

        /**
         * Return an exception if the ACL delete was not successful or null if it was.
         */
        public ApiException exception() {
            return exception;
        }
    }

    /**
     * A class containing the results of the delete ACLs operation.
     */
    public static class FilterResults {
        private final List<FilterResult> values;

        FilterResults(List<FilterResult> values) {
            this.values = values;
        }

        /**
         * Return a list of delete ACLs results for a given filter.
         */
        public List<FilterResult> values() {
            return values;
        }
    }

    private final Map<AclBindingFilter, KafkaFuture<FilterResults>> futures;

    DeleteAclsResult(Map<AclBindingFilter, KafkaFuture<FilterResults>> futures) {
        this.futures = futures;
    }

    /**
     * Return a map from acl filters to futures which can be used to check the status of the deletions by each
     * filter.
     */
    public Map<AclBindingFilter, KafkaFuture<FilterResults>> values() {
        return futures;
    }

    /**
     * Return a future which succeeds only if all the ACLs deletions succeed, and which contains all the deleted ACLs.
     * Note that it if the filters don't match any ACLs, this is not considered an error.
     */
    public KafkaFuture<Collection<AclBinding>> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0])).thenApply(v -> getAclBindings(futures));
    }

    private List<AclBinding> getAclBindings(Map<AclBindingFilter, KafkaFuture<FilterResults>> futures) {
        List<AclBinding> acls = new ArrayList<>();
        for (KafkaFuture<FilterResults> value: futures.values()) {
            FilterResults results;
            try {
                results = value.get();
            } catch (Throwable e) {
                // This should be unreachable, since the future returned by KafkaFuture#allOf should
                // have failed if any Future failed.
                throw new KafkaException("DeleteAclsResult#all: internal error", e);
            }
            for (FilterResult result : results.values()) {
                if (result.exception() != null)
                    throw result.exception();
                acls.add(result.binding());
            }
        }
        return acls;
    }
}
