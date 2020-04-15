package org.apache.kafka.common.log.remote.storage;

import java.io.*;

public interface Transferer {

    void transfer(File from, File to) throws IOException;

}
