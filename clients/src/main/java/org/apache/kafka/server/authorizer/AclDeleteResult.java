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

package org.apache.kafka.server.authorizer;

import java.util.Collections;
import java.util.Collection;
import java.util.Optional;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.ApiException;

@InterfaceStability.Evolving
public class AclDeleteResult {
    private final ApiException exception;
    private final Collection<AclBindingDeleteResult> aclBindingDeleteResults;

    public AclDeleteResult(ApiException exception) {
        this(Collections.emptySet(), exception);
    }

    public AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults) {
        this(deleteResults, null);
    }

    private AclDeleteResult(Collection<AclBindingDeleteResult> deleteResults, ApiException exception) {
        this.aclBindingDeleteResults = deleteResults;
        this.exception = exception;
    }

    /**
     * Returns any exception while attempting to match ACL filter to delete ACLs.
     * If exception is empty, filtering has succeeded. See {@link #aclBindingDeleteResults()}
     * for deletion results for each filter.
     */
    public Optional<ApiException> exception() {
        return exception == null ? Optional.empty() : Optional.of(exception);
    }

    /**
     * Returns delete result for each matching ACL binding.
     */
    public Collection<AclBindingDeleteResult> aclBindingDeleteResults() {
        return aclBindingDeleteResults;
    }


    /**
     * Delete result for each ACL binding that matched a delete filter.
     */
    public static class AclBindingDeleteResult {
        private final AclBinding aclBinding;
        private final ApiException exception;

        public AclBindingDeleteResult(AclBinding aclBinding) {
            this(aclBinding, null);
        }

        public AclBindingDeleteResult(AclBinding aclBinding, ApiException exception) {
            this.aclBinding = aclBinding;
            this.exception = exception;
        }

        /**
         * Returns ACL binding that matched the delete filter. If {@link #exception()} is
         * empty, the ACL binding was successfully deleted.
         */
        public AclBinding aclBinding() {
            return aclBinding;
        }

        /**
         * Returns any exception that resulted in failure to delete ACL binding.
         * If exception is empty, the ACL binding was successfully deleted.
         */
        public Optional<ApiException> exception() {
            return exception == null ? Optional.empty() : Optional.of(exception);
        }
    }
}
