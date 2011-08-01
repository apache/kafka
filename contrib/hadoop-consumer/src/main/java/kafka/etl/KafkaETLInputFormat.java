package kafka.etl;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import kafka.consumer.SimpleConsumer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;


@SuppressWarnings("deprecation")
public class KafkaETLInputFormat 
extends SequenceFileInputFormat<KafkaETLKey, BytesWritable> {

    protected Props _props;
    protected int _bufferSize;
    protected int _soTimeout;

    protected Map<Integer, URI> _nodes;
    protected int _partition;
    protected int _nodeId;
    protected String _topic;
    protected SimpleConsumer _consumer;

    protected MultipleOutputs _mos;
    protected OutputCollector<BytesWritable, BytesWritable> _offsetOut = null;

    protected long[] _offsetRange;
    protected long _startOffset;
    protected long _offset;
    protected boolean _toContinue = true;
    protected int _retry;
    protected long _timestamp;
    protected long _count;
    protected boolean _ignoreErrors = false;

    @Override
    public RecordReader<KafkaETLKey, BytesWritable> getRecordReader(InputSplit split,
                                    JobConf job, Reporter reporter)
                                    throws IOException {
        return new KafkaETLRecordReader(split, job, reporter);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return super.isSplitable(fs, file);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
        return super.getSplits(conf, numSplits);
    }
}
