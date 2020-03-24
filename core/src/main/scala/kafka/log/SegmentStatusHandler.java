package kafka.log;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;

public class SegmentStatusHandler {

    public static SegmentStatus getStatus(File file) throws IOException {
        if(!file.exists()) {
            return SegmentStatus.UNKNOWN;
        }else{
            final byte[] data = Files.readAllBytes(file.toPath());
            return SegmentStatus.getStatus(ByteBuffer.wrap(data).getInt());
        }
    }

    public static void setStatus(File file, SegmentStatus status) throws IOException {
        Files.write(file.toPath(), ByteBuffer.allocate(Integer.BYTES).putInt(status.getStatus()).array());
    }

}
