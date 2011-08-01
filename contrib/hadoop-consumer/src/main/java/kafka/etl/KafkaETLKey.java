package kafka.etl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import kafka.etl.KafkaETLKey;

public class KafkaETLKey implements WritableComparable<KafkaETLKey>{

    protected int _inputIndex;
    protected long _offset;
    protected long _checksum;
    
    /**
     * dummy empty constructor
     */
    public KafkaETLKey() {
        _inputIndex = 0;
        _offset = 0;
        _checksum = 0;
    }
    
    public KafkaETLKey (int index, long offset) {
        _inputIndex =  index;
        _offset = offset;
        _checksum = 0;
    }
    
    public KafkaETLKey (int index, long offset, long checksum) {
        _inputIndex =  index;
        _offset = offset;
        _checksum = checksum;
    }
    
    public void set(int index, long offset, long checksum) {
        _inputIndex = index;
        _offset = offset;
        _checksum = checksum;
    }
    
    public int getIndex() {
        return _inputIndex;
    }
    
    public long getOffset() {
        return _offset;
    }
    
    public long getChecksum() {
        return _checksum;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        _inputIndex = in.readInt(); 
        _offset = in.readLong();
        _checksum = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(_inputIndex);
        out.writeLong(_offset);
        out.writeLong(_checksum);
    }

    @Override
    public int compareTo(KafkaETLKey o) {
        if (_inputIndex != o._inputIndex)
            return _inputIndex = o._inputIndex;
        else {
            if  (_offset > o._offset) return 1;
            else if (_offset < o._offset) return -1;
            else {
                if  (_checksum > o._checksum) return 1;
                else if (_checksum < o._checksum) return -1;
                else return 0;
            }
        }
    }
    
    @Override
    public String toString() {
        return "index=" + _inputIndex + " offset=" + _offset + " checksum=" + _checksum;
    }

}
