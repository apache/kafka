/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.security.auth;


import java.net.InetAddress;

public class Session {
    private KafkaPrincipal principal;
    private InetAddress clientAddress;

    public Session(KafkaPrincipal principal, InetAddress clientAddress) {
        if (principal == null || clientAddress == null) {
            throw new IllegalArgumentException("principal and clientAddress can not be null");
        }
        this.principal = principal;
        this.clientAddress = clientAddress;
    }

    public KafkaPrincipal principal() {
        return principal;
    }

    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Session session = (Session) o;

        if (principal != session.principal) return false;
        return clientAddress.equals(session.clientAddress);
    }

    @Override
    public int hashCode() {
        int result = principal.hashCode();
        result = 31 * result + clientAddress.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return principal + ", " + clientAddress;
    }
}
