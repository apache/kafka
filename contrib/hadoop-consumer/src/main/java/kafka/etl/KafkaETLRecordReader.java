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
import java.util.ArrayList;
import java.util.List;

import kafka.common.KafkaException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

@SuppressWarnings({ "deprecation" })
public class KafkaETLRecordReader 
extends SequenceFileRecordReader<KafkaETLKey, BytesWritable> {

    /* max number of retries */
    protected Props _props;   /*properties*/
    protected JobConf _job;
    protected Reporter _reporter ;
    protected MultipleOutputs _mos;
    protected List<KafkaETLContext> _contextList;
    protected int _contextIndex ;
    
    protected long _totalBytes;
    protected long _readBytes;
    protected long _readCounts;
    
    protected String _attemptId = null;
    
    private static long _limit = 100; /*for testing only*/
    
    public KafkaETLRecordReader(InputSplit split, JobConf job, Reporter reporter) 
    throws IOException {
       super(job, (FileSplit) split);
       
       _props = KafkaETLUtils.getPropsFromJob(job);
       _contextList = new ArrayList<KafkaETLContext>();
       _job = job;
       _reporter = reporter;
       _contextIndex = -1;
       _mos = new MultipleOutputs(job);
       try {
           _limit = _props.getInt("kafka.request.limit", -1);
           
           /*get attemp id*/
           String taskId = _job.get("mapred.task.id");
           if (taskId == null) {
               throw new KafkaException("Configuration does not contain the property mapred.task.id");
           }
           String[] parts = taskId.split("_");
           if (    parts.length != 6 || !parts[0].equals("attempt") 
                || (!"m".equals(parts[3]) && !"r".equals(parts[3]))) {
                   throw new KafkaException("TaskAttemptId string : " + taskId + " is not properly formed");
           }
          _attemptId = parts[4]+parts[3];
       }catch (Exception e) {
           throw new IOException (e);
       }
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
        
        /* now record some stats */
        for (KafkaETLContext context: _contextList) {
            context.output(_attemptId);
            context.close();
        }
        
        _mos.close();
    }

    @Override
    public KafkaETLKey createKey() {
        return super.createKey();
    }

    @Override
    public BytesWritable createValue() {
        return super.createValue();
    }

    @Override
    public float getProgress() throws IOException {
        if (_totalBytes == 0) return 0f;
        
        if (_contextIndex >= _contextList.size()) return 1f;
        
        if (_limit < 0) {
            double p = ( _readBytes + getContext().getReadBytes() ) / ((double) _totalBytes);
            return (float)p;
        }
        else {
            double p = (_readCounts + getContext().getCount()) / ((double)_limit * _contextList.size());
            return (float)p;
        }
    }

    @Override
    public synchronized boolean next(KafkaETLKey key, BytesWritable value)
                                    throws IOException {
    try{
        if (_contextIndex < 0) { /* first call, get all requests */
            System.out.println("RecordReader.next init()");
            _totalBytes = 0;
            
            while ( super.next(key, value)) {
                String input = new String(value.getBytes(), "UTF-8");
                int index = _contextList.size();
                KafkaETLContext context = new KafkaETLContext(
                                              _job, _props, _reporter, _mos, index, input);
                _contextList.add(context);
                _totalBytes += context.getTotalBytes();
            }
            System.out.println("Number of requests=" + _contextList.size());
            
            _readBytes = 0;
            _readCounts = 0;
            _contextIndex = 0;
        }
        
        while (_contextIndex < _contextList.size()) {
            
            KafkaETLContext currContext = getContext();
            
            while (currContext.hasMore() && 
                       (_limit < 0 || currContext.getCount() < _limit)) {
                
                if (currContext.getNext(key, value)) {
                    //System.out.println("RecordReader.next get (key,value)");
                    return true;
                }
                else {
                    //System.out.println("RecordReader.next fetch more");
                    currContext.fetchMore();
                }
            }
            
            _readBytes += currContext.getReadBytes();
            _readCounts += currContext.getCount();
            _contextIndex ++;
            System.out.println("RecordReader.next will get from request " + _contextIndex);
       }
    }catch (Exception e) {
        throw new IOException (e);
    }
    return false;
    }
    
    protected KafkaETLContext getContext() throws IOException{
        if (_contextIndex >= _contextList.size()) 
            throw new IOException ("context index " + _contextIndex + " is out of bound " 
                                            + _contextList.size());
        return _contextList.get(_contextIndex);
    }

    

}
