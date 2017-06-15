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

/**
 * States for KafkaChannel:
 * <ul>
 *   <li>NOT_CONNECTED: Connections are created in NOT_CONNECTED state. State is updated
 *       on {@link TransportLayer#finishConnect()} when socket connection is established.
 *       PLAINTEXT channels transition from NOT_CONNECTED to READY, others transition
 *       to AUTHENTICATE. Failures in NOT_CONNECTED state typically indicate that the
 *       remote endpoint is unavailable, which may be due to misconfigured endpoints.</li>
 *   <li>AUTHENTICATE: SSL, SASL_SSL and SASL_PLAINTEXT channels are in AUTHENTICATE state during SSL and
 *       SASL handshake. Disconnections in AUTHENTICATE state may indicate that SSL or SASL
 *       authentication failed. Channels transition to READY state when authentication completes
 *       successfully.</li>
 *   <li>READY: Connected, authenticated channels are in READY state. Channels may transition from
 *       READY to EXPIRED, FAILED_SEND or LOCAL_CLOSE.</li>
 *   <li>EXPIRED: Idle connections are moved to EXPIRED state on idle timeout and the channel is closed.</li>
 *   <li>FAILED_SEND: Channels transition from READY to FAILED_SEND state if the channel is closed due
 *       to a send failure.</li>
 *   <li>LOCAL_CLOSE: Channels are moved to LOCAL_CLOSE state if close() is initiated locally.</li>
 * </ul>
 * If the remote endpoint closes a channel, the state of the channel reflects the state the channel
 * was in at the time of disconnection. This state may be useful to identify the reason for disconnection.
 * <p>
 * Typical transitions:
 * <ul>
 *   <li>PLAINTEXT Good path: NOT_CONNECTED => READY => LOCAL_CLOSE</li>
 *   <li>SASL/SSL Good path: NOT_CONNECTED => AUTHENTICATE => READY => LOCAL_CLOSE</li>
 *   <li>Bootstrap server misconfiguration: NOT_CONNECTED, disconnected in NOT_CONNECTED state</li>
 *   <li>Security misconfiguration: NOT_CONNECTED => AUTHENTICATE, disconnected in AUTHENTICATE state</li>
 * </ul>
 */
public enum ChannelState {
    NOT_CONNECTED,
    AUTHENTICATE,
    READY,
    EXPIRED,
    FAILED_SEND,
    LOCAL_CLOSE
}