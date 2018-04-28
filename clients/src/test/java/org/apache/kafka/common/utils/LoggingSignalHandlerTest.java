package org.apache.kafka.common.utils;

import org.junit.Test;

public class LoggingSignalHandlerTest {

    @Test
    public void test() throws ReflectiveOperationException {
        new LoggingSignalHandler().register();
    }

}
