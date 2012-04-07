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


import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import kafka.api.FetchRequest;
import kafka.api.OffsetRequest;
import kafka.common.ErrorMapping;
import kafka.javaapi.MultiFetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

@SuppressWarnings({ "deprecation"})
public class KafkaETLContext {
    
    static protected int MAX_RETRY_TIME = 1;
    final static String CLIENT_BUFFER_SIZE = "client.buffer.size";
    final static String CLIENT_TIMEOUT = "client.so.timeout";

    final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;
    final static int DEFAULT_TIMEOUT = 60000; // one minute

    final static KafkaETLKey DUMMY_KEY = new KafkaETLKey();

    protected int _index; /*index of context*/
    protected String _input = null; /*input string*/
    protected KafkaETLRequest _request = null;
    protected SimpleConsumer _consumer = null; /*simple consumer*/

    protected long[] _offsetRange = {0, 0};  /*offset range*/
    protected long _offset = Long.MAX_VALUE; /*current offset*/
    protected long _count; /*current count*/

    protected MultiFetchResponse _response = null;  /*fetch response*/
    protected Iterator<MessageAndOffset> _messageIt = null; /*message iterator*/
    protected Iterator<ByteBufferMessageSet> _respIterator = null;
    protected int _retry = 0;
    protected long _requestTime = 0; /*accumulative request time*/
    protected long _startTime = -1;
    
    protected int _bufferSize;
    protected int _timeout;
    protected Reporter _reporter;
    
    protected MultipleOutputs _mos;
    protected OutputCollector<KafkaETLKey, BytesWritable> _offsetOut = null;
    
    public long getTotalBytes() {
        return (_offsetRange[1] > _offsetRange[0])? _offsetRange[1] - _offsetRange[0] : 0;
    }
    
    public long getReadBytes() {
        return _offset - _offsetRange[0];
    }
    
    public long getCount() {
        return _count;
    }
    
    /**
     * construct using input string
     */
    @SuppressWarnings("unchecked")
    public KafkaETLContext(JobConf job, Props props, Reporter reporter, 
                                    MultipleOutputs mos, int index, String input) 
    throws Exception {
        
        _bufferSize = getClientBufferSize(props);
        _timeout = getClientTimeout(props);
        System.out.println("bufferSize=" +_bufferSize);
        System.out.println("timeout=" + _timeout);
        _reporter = reporter;
        _mos = mos;
        
        // read topic and current offset from input
        _index= index; 
        _input = input;
        _request = new KafkaETLRequest(input.trim());
        
        // read data from queue
        URI uri = _request.getURI();
        _consumer = new SimpleConsumer(uri.getHost(), uri.getPort(), _timeout, _bufferSize);
        
        // get available offset range
        _offsetRange = getOffsetRange();
        System.out.println("Connected to node " + uri 
                + " beginning reading at offset " + _offsetRange[0]
                + " latest offset=" + _offsetRange[1]);

        _offset = _offsetRange[0];
        _count = 0;
        _requestTime = 0;
        _retry = 0;
        
        _startTime = System.currentTimeMillis();
    }
    
    public boolean hasMore () {
        return _messageIt != null && _messageIt.hasNext() 
                || _response != null && _respIterator.hasNext()
                || _offset < _offsetRange[1]; 
    }
    
    public boolean getNext(KafkaETLKey key, BytesWritable value) throws IOException {
        if ( !hasMore() ) return false;
        
        boolean gotNext = get(key, value);

        if(_response != null) {

            while ( !gotNext && _respIterator.hasNext()) {
                ByteBufferMessageSet msgSet = _respIterator.next();
                if ( hasError(msgSet)) return false;
                _messageIt = msgSet.iterator();
                gotNext = get(key, value);
            }
        }
        return gotNext;
    }
    
    public boolean fetchMore () throws IOException {
        if (!hasMore()) return false;
        
        FetchRequest fetchRequest = 
            new FetchRequest(_request.getTopic(), _request.getPartition(), _offset, _bufferSize);
        List<FetchRequest> array = new ArrayList<FetchRequest>();
        array.add(fetchRequest);

        long tempTime = System.currentTimeMillis();
        _response = _consumer.multifetch(array);
        if(_response != null)
            _respIterator = _response.iterator();
        _requestTime += (System.currentTimeMillis() - tempTime);
        
        return true;
    }
    
    @SuppressWarnings("unchecked")
    public void output(String fileprefix) throws IOException {
       String offsetString = _request.toString(_offset);

        if (_offsetOut == null)
            _offsetOut = (OutputCollector<KafkaETLKey, BytesWritable>)
                                    _mos.getCollector("offsets", fileprefix+_index, _reporter);
        _offsetOut.collect(DUMMY_KEY, new BytesWritable(offsetString.getBytes("UTF-8")));
        
    }
    
    public void close() throws IOException {
        if (_consumer != null) _consumer.close();
        
        String topic = _request.getTopic();
        long endTime = System.currentTimeMillis();
        _reporter.incrCounter(topic, "read-time(ms)", endTime - _startTime);
        _reporter.incrCounter(topic, "request-time(ms)", _requestTime);
        
        long bytesRead = _offset - _offsetRange[0];
        double megaRead = bytesRead / (1024.0*1024.0);
        _reporter.incrCounter(topic, "data-read(mb)", (long) megaRead);
        _reporter.incrCounter(topic, "event-count", _count);
    }
    
    protected boolean get(KafkaETLKey key, BytesWritable value) throws IOException {
        if (_messageIt != null && _messageIt.hasNext()) {
            MessageAndOffset messageAndOffset = _messageIt.next();
            
            ByteBuffer buf = messageAndOffset.message().payload();
            int origSize = buf.remaining();
            byte[] bytes = new byte[origSize];
          buf.get(bytes, buf.position(), origSize);
            value.set(bytes, 0, origSize);
            
            key.set(_index, _offset, messageAndOffset.message().checksum());
            
            _offset = messageAndOffset.offset();  //increase offset
            _count ++;  //increase count
            
            return true;
        }
        else return false;
    }
    
    /**
     * Get offset ranges
     */
    protected long[] getOffsetRange() throws IOException {

        /* get smallest and largest offsets*/
        long[] range = new long[2];

        long[] startOffsets = _consumer.getOffsetsBefore(_request.getTopic(), _request.getPartition(),
                OffsetRequest.EarliestTime(), 1);
        if (startOffsets.length != 1)
            throw new IOException("input:" + _input + " Expect one smallest offset but get "
                                            + startOffsets.length);
        range[0] = startOffsets[0];
        
        long[] endOffsets = _consumer.getOffsetsBefore(_request.getTopic(), _request.getPartition(),
                                        OffsetRequest.LatestTime(), 1);
        if (endOffsets.length != 1)
            throw new IOException("input:" + _input + " Expect one latest offset but get " 
                                            + endOffsets.length);
        range[1] = endOffsets[0];

        /*adjust range based on input offsets*/
        if ( _request.isValidOffset()) {
            long startOffset = _request.getOffset();
            if (startOffset > range[0]) {
                System.out.println("Update starting offset with " + startOffset);
                range[0] = startOffset;
            }
            else {
                System.out.println("WARNING: given starting offset " + startOffset 
                                            + " is smaller than the smallest one " + range[0] 
                                            + ". Will ignore it.");
            }
        }
        System.out.println("Using offset range [" + range[0] + ", " + range[1] + "]");
        return range;
    }
    
    /**
     * Called by the default implementation of {@link #map} to check error code
     * to determine whether to continue.
     */
    protected boolean hasError(ByteBufferMessageSet messages)
            throws IOException {
        int errorCode = messages.getErrorCode();
        if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
            /* offset cannot cross the maximum offset (guaranteed by Kafka protocol).
               Kafka server may delete old files from time to time */
            System.err.println("WARNING: current offset=" + _offset + ". It is out of range.");

            if (_retry >= MAX_RETRY_TIME)  return true;
            _retry++;
            // get the current offset range
            _offsetRange = getOffsetRange();
            _offset =  _offsetRange[0];
            return false;
        } else if (errorCode == ErrorMapping.InvalidMessageCode()) {
            throw new IOException(_input + " current offset=" + _offset
                    + " : invalid offset.");
        } else if (errorCode == ErrorMapping.WrongPartitionCode()) {
            throw new IOException(_input + " : wrong partition");
        } else if (errorCode != ErrorMapping.NoError()) {
            throw new IOException(_input + " current offset=" + _offset
                    + " error:" + errorCode);
        } else
            return false;
    }
    
    public static int getClientBufferSize(Props props) throws Exception {
        return props.getInt(CLIENT_BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
    }

    public static int getClientTimeout(Props props) throws Exception {
        return props.getInt(CLIENT_TIMEOUT, DEFAULT_TIMEOUT);
    }

}
