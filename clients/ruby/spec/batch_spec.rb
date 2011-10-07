require File.dirname(__FILE__) + '/spec_helper'

describe Batch do

  before(:each) do
    @batch = Batch.new
  end

  describe "batch messages" do
    it "holds all messages to be sent" do
      @batch.should respond_to(:messages)
      @batch.messages.class.should eql(Array)
    end

    it "supports queueing/adding messages to be send" do
      @batch.messages << mock(Kafka::Message.new("one"))
      @batch.messages << mock(Kafka::Message.new("two"))
      @batch.messages.length.should eql(2)
    end
  end
end