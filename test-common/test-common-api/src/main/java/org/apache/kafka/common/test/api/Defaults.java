package org.apache.kafka.common.test.api;

import org.apache.kafka.common.security.auth.SecurityProtocol;

public class Defaults {
    public static final int CONTROLLER_ID_OFFSET = 3000;
    public static final int BROKER_ID_OFFSET = 0;
    public static final SecurityProtocol DEFAULT_BROKER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_BROKER_LISTENER_NAME = "EXTERNAL";
    public static final SecurityProtocol DEFAULT_CONTROLLER_SECURITY_PROTOCOL = SecurityProtocol.PLAINTEXT;
    public static final String DEFAULT_CONTROLLER_LISTENER_NAME = "CONTROLLER";
}
