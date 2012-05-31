/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.etl;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

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
