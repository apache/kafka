Hadoop to Kafka Bridge
======================

What's new?
-----------

* Now supports Kafka's software load balancer (Kafka URIs are specified with
  kafka+zk as the scheme, as described below)
* Supports Kafka 0.7. Now uses the new Producer API, rather than the legacy
  SyncProducer.

What is it?
-----------

The Hadoop to Kafka bridge is a way to publish data from Hadoop to Kafka. There
are two possible mechanisms, varying from easy to difficult:  writing a Pig
script and writing messages in Avro format, or rolling your own job using the
Kafka `OutputFormat`. 

Note that there are no write-once semantics: any client of the data must handle
messages in an idempotent manner. That is, because of node failures and
Hadoop's failure recovery, it's possible that the same message is published
multiple times in the same push.

How do I use it?
----------------

With this bridge, Kafka topics are URIs and are specified in one of two
formats: `kafka+zk://<zk-path>#<kafka-topic>`, which uses the software load
balancer, or the legacy `kafka://<kafka-server>/<kafka-topic>` to connect to a
specific Kafka broker.

### Pig ###

Pig bridge writes data in binary Avro format with one message created per input
row. To push data via Kafka, store to the Kafka URI using `AvroKafkaStorage`
with the Avro schema as its first argument. You'll need to register the
appropriate Kafka JARs. Here is what an example Pig script looks like:

    REGISTER hadoop-producer_2.8.0-0.7.0.jar;
    REGISTER avro-1.4.0.jar;
    REGISTER piggybank.jar;
    REGISTER kafka-0.7.0.jar;
    REGISTER jackson-core-asl-1.5.5.jar;
    REGISTER jackson-mapper-asl-1.5.5.jar;
    REGISTER zkclient-20110412.jar;
    REGISTER zookeeper-3.3.4.jar;
    REGISTER scala-library.jar;

    member_info = LOAD 'member_info.tsv' as (member_id : int, name : chararray);
    names = FOREACH member_info GENERATE name;
    STORE member_info INTO 'kafka+zk://my-zookeeper:2181/kafka#member_info' USING kafka.bridge.AvroKafkaStorage('"string"');

That's it! The Pig StoreFunc makes use of AvroStorage in Piggybank to convert
from Pig's data model to the specified Avro schema.

Further, multi-store is possible with KafkaStorage, so you can easily write to
multiple topics and brokers in the same job:

    SPLIT member_info INTO early_adopters IF member_id < 1000, others IF member_id >= 1000;
    STORE early_adopters INTO 'kafka+zk://my-zookeeper:2181/kafka#early_adopters' USING AvroKafkaStorage('$schema');
    STORE others INTO 'kafka://my-broker:9092,my-broker2:9092/others' USING AvroKafkaStorage('$schema');

### KafkaOutputFormat ###

KafkaOutputFormat is a Hadoop OutputFormat for publishing data via Kafka. It
uses the newer 0.20 mapreduce APIs and simply pushes bytes (i.e.,
BytesWritable). This is a lower-level method of publishing data, as it allows
you to precisely control output.

Here is an example that publishes some input text. With KafkaOutputFormat, the
key is a NullWritable and is ignored; only values are published. Speculative
execution is turned off by the OutputFormat.

    import kafka.bridge.hadoop.KafkaOutputFormat;
    
    import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.BytesWritable;
    import org.apache.hadoop.io.NullWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.mapreduce.Job;
    import org.apache.hadoop.mapreduce.Mapper;
    import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
    import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
    
    import java.io.IOException;
    
    public class TextPublisher
    {
      public static void main(String[] args) throws Exception
      {
        if (args.length != 2) {
          System.err.println("usage: <input path> <kafka output url>");
          return;
        }
    
        Job job = new Job();
    
        job.setJarByClass(TextPublisher.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(KafkaOutputFormat.class);
    
        job.setMapperClass(TheMapper.class);
        job.setNumReduceTasks(0);
    
        FileInputFormat.addInputPath(job, new Path(args[0]));
        KafkaOutputFormat.setOutputPath(job, new Path(args[1]));
    
        if (!job.waitForCompletion(true)) {
          throw new RuntimeException("Job failed!");
        }
      }
    
      public static class TheMapper extends Mapper<Object, Object, NullWritable, BytesWritable>
      {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException
        {
          context.write(NullWritable.get(), new BytesWritable(((Text) value).getBytes()));
        }
      }
    }

What can I tune?
----------------

Normally, you needn't change any of these parameters:

* kafka.output.queue_size: Bytes to queue in memory before pushing to the Kafka
  producer (i.e., the batch size). Default is 10*1024*1024 (10MB).
* kafka.output.connect_timeout: Connection timeout in milliseconds (see Kafka
  producer docs). Default is 30*1000 (30s).
* kafka.output.reconnect_timeout: Milliseconds to wait until attempting
  reconnection (see Kafka producer docs). Default is 1000 (1s).
* kafka.output.bufsize: Producer buffer size in bytes (see Kafka producer
  docs). Default is 64*1024 (64KB). 
* kafka.output.max_msgsize: Maximum message size in bytes (see Kafka producer
  docs). Default is 1024*1024 (1MB).
* kafka.output.compression_codec: The compression codec to use (see Kafka producer
  docs). Default is 0 (no compression).

For easier debugging, the above values as well as the Kafka broker information
(either kafka.zk.connect or kafka.broker.list), the topic (kafka.output.topic),
and the schema (kafka.output.schema) are injected into the job's configuration.

