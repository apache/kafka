# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
require File.dirname(__FILE__) + '/spec_helper'

describe Message do

  before(:each) do
    @message = Message.new
  end

  describe "Kafka Message" do
    it "should have a default magic number" do
      Message::MAGIC_IDENTIFIER_DEFAULT.should eql(0)
    end

    it "should have a magic field, a checksum and a payload" do
      [:magic, :checksum, :payload].each do |field|
        @message.should respond_to(field.to_sym)
      end
    end

    it "should set a default value of zero" do
      @message.magic.should eql(Kafka::Message::MAGIC_IDENTIFIER_DEFAULT)
    end

    it "should allow to set a custom magic number" do
      @message = Message.new("ale", 1)
      @message.magic.should eql(1)
    end

    it "should calculate the checksum (crc32 of a given message)" do
      @message.payload = "ale"
      @message.calculate_checksum.should eql(1120192889)
      @message.payload = "alejandro"
      @message.calculate_checksum.should eql(2865078607)
    end

    it "should say if the message is valid using the crc32 signature" do
      @message.payload  = "alejandro"
      @message.checksum = 2865078607
      @message.valid?.should eql(true)
      @message.checksum = 0
      @message.valid?.should eql(false)
      @message = Message.new("alejandro", 0, 66666666) # 66666666 is a funny checksum
      @message.valid?.should eql(false)
    end

    it "should parse a message from bytes" do
      bytes = [12].pack("N") + [0].pack("C") + [1120192889].pack("N") + "ale"
      message = Kafka::Message.parse_from(bytes)
      message.valid?.should eql(true)
      message.magic.should eql(0)
      message.checksum.should eql(1120192889)
      message.payload.should eql("ale")
    end
  end
end
