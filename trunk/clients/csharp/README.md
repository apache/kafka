# .NET Kafka Client

This is a .NET implementation of a client for Kafka using C#.  It provides for a basic implementation that covers most basic functionalities to include a simple Producer and Consumer.

The .NET client will wrap Kafka server error codes to the `KafkaException` class.  Exceptions are not trapped within the library and basically bubble up directly from the TcpClient and it's underlying Socket connection.  Clients using this library should look to do their own exception handling regarding these kinds of errors.

## Producer

The Producer can send one or more messages to Kafka in both a synchronous and asynchronous fashion.

### Producer Usage

    string payload1 = "kafka 1.";
    byte[] payloadData1 = Encoding.UTF8.GetBytes(payload1);
    Message msg1 = new Message(payloadData1);

    string payload2 = "kafka 2.";
    byte[] payloadData2 = Encoding.UTF8.GetBytes(payload2);
    Message msg2 = new Message(payloadData2);

    Producer producer = new Producer("localhost", 9092);
    producer.Send("test", 0, new List<Message> { msg1, msg2 });

### Asynchronous Producer Usage

    List<Message> messages = GetBunchOfMessages();

    Producer producer = new Producer("localhost", 9092);
    producer.SendAsync("test", 0, messages, (requestContext) => { // doing work });

### Multi-Producer Usage

    List<ProducerRequest> requests = new List<ProducerRequest>
    { 
        new ProducerRequest("test a", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("1: " + DateTime.UtcNow)) }),
        new ProducerRequest("test b", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("2: " + DateTime.UtcNow)) }),
        new ProducerRequest("test c", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("3: " + DateTime.UtcNow)) }),
        new ProducerRequest("test d", 0, new List<Message> { new Message(Encoding.UTF8.GetBytes("4: " + DateTime.UtcNow)) })
    };

    MultiProducerRequest request = new MultiProducerRequest(requests);
    Producer producer = new Producer("localhost", 9092);
    producer.Send(request);

## Consumer

The consumer has several functions of interest: `GetOffsetsBefore` and `Consume`.  `GetOffsetsBefore` will retrieve a list of offsets before a given time and `Consume` will attempt to get a list of messages from Kafka given a topic, partition and offset.  `Consume` allows for both a single and batched request function using the `MultiFetchRequest`.

### Consumer Usage

    Consumer consumer = new Consumer("localhost", 9092);
    int max = 10;
    long[] offsets = consumer.GetOffsetsBefore("test", 0, OffsetRequest.LatestTime, max);
    List<Message> messages = consumer.Consume("test", 0, offsets[0]);

### Consumer Multi-fetch

    Consumer consumer = new Consumer("localhost", 9092);
    MultiFetchRequest request = new MultiFetchRequest(new List<FetchRequest>
    {
        new FetchRequest("testa", 0, 0),
        new FetchRequest("testb", 0, 0),
        new FetchRequest("testc", 0, 0)
    });

    List<List<Message>> messages = consumer.Consume(request);