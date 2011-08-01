require File.dirname(__FILE__) + '/spec_helper'

describe Consumer do

  before(:each) do
    @mocked_socket = mock(TCPSocket)
    TCPSocket.stub!(:new).and_return(@mocked_socket) # don't use a real socket
    @consumer = Consumer.new
  end

  describe "Kafka Consumer" do

    it "should have a CONSUME_REQUEST_TYPE" do
      Consumer::CONSUME_REQUEST_TYPE.should eql(1)
      @consumer.should respond_to(:request_type)
    end

    it "should have a topic and a partition" do
      @consumer.should respond_to(:topic)
      @consumer.should respond_to(:partition)
    end

    it "should have a polling option, and a default value" do
      Consumer::DEFAULT_POLLING_INTERVAL.should eql(2)
      @consumer.should respond_to(:polling)
      @consumer.polling.should eql(2)
    end

    it "should set a topic and partition on initialize" do
      @consumer = Consumer.new({ :host => "localhost", :port => 9092, :topic => "testing" })
      @consumer.topic.should eql("testing")
      @consumer.partition.should eql(0)
      @consumer = Consumer.new({ :topic => "testing", :partition => 3 })
      @consumer.partition.should eql(3)
    end

    it "should set default host and port if none is specified" do
      @consumer = Consumer.new
      @consumer.host.should eql("localhost")
      @consumer.port.should eql(9092)
    end

    it "should have a default offset, and be able to set it" do
      @consumer.offset.should eql(0)
      @consumer = Consumer.new({ :offset => 1111 })
      @consumer.offset.should eql(1111)
    end

    it "should have a max size" do
      Consumer::MAX_SIZE.should eql(1048576)
      @consumer.max_size.should eql(1048576)
    end

    it "should return the size of the request" do
      @consumer.request_size.should eql(24)
      @consumer.topic = "someothertopicname"
      @consumer.request_size.should eql(38)
      @consumer.encode_request_size.should eql([@consumer.request_size].pack("N"))
    end

    it "should encode a request to consume" do
      bytes = [Kafka::Consumer::CONSUME_REQUEST_TYPE].pack("n") + ["test".length].pack("n") + "test" + [0].pack("N") + [0].pack("L_") + [Kafka::Consumer::MAX_SIZE].pack("N")
      @consumer.encode_request(Kafka::Consumer::CONSUME_REQUEST_TYPE, "test", 0, 0, Kafka::Consumer::MAX_SIZE).should eql(bytes)
    end

    it "should read the response data" do
      bytes = [12].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      @mocked_socket.should_receive(:read).exactly(:twice).and_return(bytes)
      @consumer.read_data_response.should eql(bytes[2, bytes.length])
    end

    it "should send a consumer request" do
      @consumer.stub!(:encode_request_size).and_return(666)
      @consumer.stub!(:encode_request).and_return("someencodedrequest")
      @consumer.should_receive(:write).with("someencodedrequest").exactly(:once).and_return(true)
      @consumer.should_receive(:write).with(666).exactly(:once).and_return(true)
      @consumer.send_consume_request.should eql(true)
    end

    it "should parse a message set from bytes" do
      bytes = [12].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      message = @consumer.parse_message_set_from(bytes).first
      message.payload.should eql("ale")
      message.checksum.should eql(1120192889)
      message.magic.should eql(0)
      message.valid?.should eql(true)
    end

    it "should consume messages" do
      @consumer.should_receive(:send_consume_request).and_return(true)
      @consumer.should_receive(:read_data_response).and_return("")
      @consumer.consume.should eql([])
    end

    it "should loop and execute a block with the consumed messages" do
      @consumer.stub!(:consume).and_return([mock(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:once).and_return([])
      @consumer.loop do |message|
        messages << message
        break # we don't wanna loop forever on the test
      end
    end

    it "should loop (every N seconds, configurable on polling attribute), and execute a block with the consumed messages" do
      @consumer = Consumer.new({ :polling => 1 })
      @consumer.stub!(:consume).and_return([mock(Kafka::Message)])
      messages = []
      messages.should_receive(:<<).exactly(:twice).and_return([])
      executed_times = 0
      @consumer.loop do |message|
        messages << message
        executed_times += 1
        break if executed_times >= 2 # we don't wanna loop forever on the test, only 2 seconds
      end

      executed_times.should eql(2)
    end
  end
end
