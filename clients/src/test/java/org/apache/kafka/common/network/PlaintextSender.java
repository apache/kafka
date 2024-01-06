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
import java.net.Socket;

/**
 * test helper class that will connect to a given server address, write out the given payload and disconnect
 */
public class PlaintextSender extends Thread {

    @SuppressWarnings("this-escape")
    public PlaintextSender(final InetSocketAddress serverAddress, final byte[] payload) {
        super(() -> {
            try (Socket connection = new Socket(serverAddress.getAddress(), serverAddress.getPort());
                 OutputStream os = connection.getOutputStream()) {
                os.write(payload);
                os.flush();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        });
        setDaemon(true);
        setName("PlaintextSender - " + payload.length + " bytes @ " + serverAddress);
    }
}
