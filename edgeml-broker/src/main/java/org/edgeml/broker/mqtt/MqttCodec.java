package org.edgeml.broker.mqtt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqttCodec {
    private static final Logger log = LoggerFactory.getLogger(MqttCodec.class);

    public void decode(byte[] buffer, int length) {
        if (length > 0) {
            int packetType = (buffer[0] >> 4) & 0x0F;
            log.debug("Decoded MQTT Packet Type: {}", packetType);
            // 1 = CONNECT, 3 = PUBLISH, 8 = SUBSCRIBE, etc.
        }
    }

    public byte[] encodeConnack() {
        // Basic MQTT 3.1.1 CONNACK (0x20 0x02 0x00 0x00)
        return new byte[] { 0x20, 0x02, 0x00, 0x00 };
    }
}
