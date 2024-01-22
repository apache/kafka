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

package org.apache.kafka.image.node;

import org.apache.kafka.common.security.token.delegation.TokenInformation;
import org.apache.kafka.image.node.printer.MetadataNodePrinter;
import org.apache.kafka.metadata.DelegationTokenData;


public class DelegationTokenDataNode implements MetadataNode {
    private final DelegationTokenData data;

    public DelegationTokenDataNode(DelegationTokenData data) {
        this.data = data;
    }

    @Override
    public boolean isDirectory() {
        return false;
    }

    @Override
    public void print(MetadataNodePrinter printer) {
        TokenInformation tokenInformation = data.tokenInformation();
        StringBuilder bld = new StringBuilder();
        bld.append("DelegationTokenData");
        bld.append("tokenId=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.tokenId());
        }
        bld.append("(owner=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.ownerAsString());
        }
        bld.append("(tokenRequester=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.tokenRequesterAsString());
        }
        bld.append("(renewers=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(String.join(" ", tokenInformation.renewersAsString()));
        }
        bld.append(", issueTimestamp=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.issueTimestamp());
        }
        bld.append(", maxTimestamp=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.maxTimestamp());
        }
        bld.append(")");
        bld.append(", expiryTimestamp=");
        if (printer.redactionCriteria().shouldRedactDelegationToken()) {
            bld.append("[redacted]");
        } else {
            bld.append(tokenInformation.expiryTimestamp());
        }
        printer.output(bld.toString());
    }
}
