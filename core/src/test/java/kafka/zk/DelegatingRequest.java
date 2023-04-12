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
package kafka.zk;

import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.metrics.Summary;
import org.apache.zookeeper.metrics.SummarySet;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.txn.TxnDigest;
import org.apache.zookeeper.txn.TxnHeader;

/**
 * A server-side Zookeeper request. Delegates everything to the actual request but the
 * connection. This allows the use of a MutedServerCxn in order to inject transmission-level failures.
 */
public class DelegatingRequest extends Request {
    private final Request delegate;

    public DelegatingRequest(ServerCnxn cnxn, Request delegate) {
        super(cnxn,
            delegate.sessionId,
            delegate.cxid,
            delegate.type,
            delegate.request,
            delegate.authInfo
        );
        this.delegate = delegate;
    }

    @Override
    public ServerCnxn getConnection() {
        return cnxn;
    }

    @Override
    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return "(DelegatingRequest) " + delegate.toString();
    }

    @Override
    public void setException(KeeperException e) {
        delegate.setException(e);
    }

    @Override
    public void setHdr(TxnHeader hdr) {
        delegate.setHdr(hdr);
    }

    @Override
    public void setLargeRequestSize(int size) {
        delegate.setLargeRequestSize(size);
    }

    @Override
    public void setLocalSession(boolean isLocalSession) {
        delegate.setLocalSession(isLocalSession);
    }

    @Override
    public void setOwner(Object owner) {
        delegate.setOwner(owner);
    }

    @Override
    public void setTxn(Record txn) {
        delegate.setTxn(txn);
    }

    @Override
    public void setTxnDigest(TxnDigest txnDigest) {
        delegate.setTxnDigest(txnDigest);
    }

    @Override
    public String getUsers() {
        return delegate.getUsers();
    }

    @Override
    public int getLargeRequestSize() {
        return delegate.getLargeRequestSize();
    }

    @Override
    public KeeperException getException() {
        return delegate.getException();
    }

    @Override
    public Object getOwner() {
        return delegate.getOwner();
    }

    @Override
    public Record getTxn() {
        return delegate.getTxn();
    }

    @Override
    public TxnDigest getTxnDigest() {
        return delegate.getTxnDigest();
    }

    @Override
    public TxnHeader getHdr() {
        return delegate.getHdr();
    }

    @Override
    public boolean isStale() {
        return delegate.isStale();
    }

    @Override
    public boolean isLocalSession() {
        return delegate.isLocalSession();
    }

    @Override
    public boolean isQuorum() {
        return delegate.isQuorum();
    }

    @Override
    public boolean mustDrop() {
        return delegate.mustDrop();
    }

    @Override
    public void logLatency(Summary metric) {
        delegate.logLatency(metric);
    }

    @Override
    public void logLatency(SummarySet metric, String key) {
        delegate.logLatency(metric, key);
    }

    @Override
    public void logLatency(Summary metric, long currentTime) {
        delegate.logLatency(metric, currentTime);
    }

    @Override
    public void logLatency(SummarySet metric, String key, long currentTime) {
        delegate.logLatency(metric, key, currentTime);
    }
}
