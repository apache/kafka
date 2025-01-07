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
package org.apache.kafka.common.network;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public final class SslSender extends Thread {

    private final String tlsProtocol;
    private final InetSocketAddress serverAddress;
    private final byte[] payload;
    private final CountDownLatch handshaked = new CountDownLatch(1);

    public SslSender(String tlsProtocol, InetSocketAddress serverAddress, byte[] payload) {
        this.tlsProtocol = tlsProtocol;
        this.serverAddress = serverAddress;
        this.payload = payload;
        setDaemon(true);
        setName("SslSender - " + payload.length + " bytes @ " + serverAddress);
    }

    @Override
    public void run() {
        try {
            SSLContext sc = SSLContext.getInstance(tlsProtocol);
            sc.init(null, new TrustManager[]{new NaiveTrustManager()}, new java.security.SecureRandom());
            try (SSLSocket connection = (SSLSocket) sc.getSocketFactory().createSocket(serverAddress.getAddress(), serverAddress.getPort())) {
                OutputStream os = connection.getOutputStream();
                connection.startHandshake();
                handshaked.countDown();
                os.write(payload);
                os.flush();
            }
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }

    public boolean waitForHandshake(long timeoutMillis) throws InterruptedException {
        return handshaked.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * blindly trust any certificate presented to it
     */
    private static class NaiveTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] x509Certificates, String s) {
            //nop
        }

        @Override
        public void checkServerTrusted(X509Certificate[] x509Certificates, String s) {
            //nop
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
