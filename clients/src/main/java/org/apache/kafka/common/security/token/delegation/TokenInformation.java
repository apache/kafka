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
package org.apache.kafka.common.security.token.delegation;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A class representing a delegation token details.
 *
 */
@InterfaceStability.Evolving
public class TokenInformation {

    private KafkaPrincipal owner;
    private Collection<KafkaPrincipal> renewers;
    private long issueTimestamp;
    private long maxTimestamp;
    private long expiryTimestamp;
    private String tokenId;

    public TokenInformation(String tokenId, KafkaPrincipal owner, Collection<KafkaPrincipal> renewers,
                            long issueTimestamp, long maxTimestamp, long expiryTimestamp) {
        this.tokenId = tokenId;
        this.owner = owner;
        this.renewers = renewers;
        this.issueTimestamp =  issueTimestamp;
        this.maxTimestamp =  maxTimestamp;
        this.expiryTimestamp =  expiryTimestamp;
    }

    public KafkaPrincipal owner() {
        return owner;
    }

    public String ownerAsString() {
        return owner.toString();
    }

    public Collection<KafkaPrincipal> renewers() {
        return renewers;
    }

    public Collection<String> renewersAsString() {
        Collection<String> renewerList = new ArrayList<>();
        for (KafkaPrincipal renewer : renewers) {
            renewerList.add(renewer.toString());
        }
        return renewerList;
    }

    public long issueTimestamp() {
        return issueTimestamp;
    }

    public long expiryTimestamp() {
        return expiryTimestamp;
    }

    public void setExpiryTimestamp(long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
    }

    public String tokenId() {
        return tokenId;
    }

    public long maxTimestamp() {
        return maxTimestamp;
    }

    public boolean ownerOrRenewer(KafkaPrincipal principal) {
        return owner.equals(principal) || renewers.contains(principal);
    }

    @Override
    public String toString() {
        return "TokenInformation{" +
            "owner=" + owner +
            ", renewers=" + renewers +
            ", issueTimestamp=" + issueTimestamp +
            ", maxTimestamp=" + maxTimestamp +
            ", expiryTimestamp=" + expiryTimestamp +
            ", tokenId='" + tokenId + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TokenInformation that = (TokenInformation) o;

        if (issueTimestamp != that.issueTimestamp) {
            return false;
        }
        if (maxTimestamp != that.maxTimestamp) {
            return false;
        }
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) {
            return false;
        }
        if (renewers != null ? !renewers.equals(that.renewers) : that.renewers != null) {
            return false;
        }
        return tokenId != null ? tokenId.equals(that.tokenId) : that.tokenId == null;
    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (renewers != null ? renewers.hashCode() : 0);
        result = 31 * result + (int) (issueTimestamp ^ (issueTimestamp >>> 32));
        result = 31 * result + (int) (maxTimestamp ^ (maxTimestamp >>> 32));
        result = 31 * result + (tokenId != null ? tokenId.hashCode() : 0);
        return result;
    }
}
