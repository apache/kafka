package kafka.common.record;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(value = Parameterized.class)
public class RecordTest {

    private ByteBuffer key;
    private ByteBuffer value;
    private CompressionType compression;
    private Record record;

    public RecordTest(byte[] key, byte[] value, CompressionType compression) {
        this.key = key == null ? null : ByteBuffer.wrap(key);
        this.value = value == null ? null : ByteBuffer.wrap(value);
        this.compression = compression;
        this.record = new Record(key, value, compression);
    }

    @Test
    public void testFields() {
        assertEquals(compression, record.compressionType());
        assertEquals(key != null, record.hasKey());
        assertEquals(key, record.key());
        if (key != null)
            assertEquals(key.limit(), record.keySize());
        assertEquals(Record.CURRENT_MAGIC_VALUE, record.magic());
        assertEquals(value, record.value());
        if (value != null)
            assertEquals(value.limit(), record.valueSize());
    }

    @Test
    public void testChecksum() {
        assertEquals(record.checksum(), record.computeChecksum());
        assertTrue(record.isValid());
        for (int i = Record.CRC_OFFSET + Record.CRC_LENGTH; i < record.size(); i++) {
            Record copy = copyOf(record);
            copy.buffer().put(i, (byte) 69);
            assertFalse(copy.isValid());
            try {
                copy.ensureValid();
                fail("Should fail the above test.");
            } catch (InvalidRecordException e) {
                // this is good
            }
        }
    }

    private Record copyOf(Record record) {
        ByteBuffer buffer = ByteBuffer.allocate(record.size());
        record.buffer().put(buffer);
        buffer.rewind();
        record.buffer().rewind();
        return new Record(buffer);
    }

    @Test
    public void testEquality() {
        assertEquals(record, copyOf(record));
    }

    @Parameters
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<Object[]>();
        for (byte[] key : Arrays.asList(null, "".getBytes(), "key".getBytes()))
            for (byte[] value : Arrays.asList(null, "".getBytes(), "value".getBytes()))
                for (CompressionType compression : CompressionType.values())
                    values.add(new Object[] { key, value, compression });
        return values;
    }

}
