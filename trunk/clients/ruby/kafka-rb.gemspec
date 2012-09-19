# -*- encoding: utf-8 -*-
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

Gem::Specification.new do |s|
  s.name = %q{kafka-rb}
  s.version = "0.0.5"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["Alejandro Crosa"]
  s.autorequire = %q{kafka-rb}
  s.date = %q{2011-01-13}
  s.description = %q{kafka-rb allows you to produce and consume messages using the Kafka distributed publish/subscribe messaging service.}
  s.email = %q{alejandrocrosa@gmail.com}
  s.extra_rdoc_files = ["LICENSE"]
  s.files = ["LICENSE", "README.md", "Rakefile", "lib/kafka", "lib/kafka/batch.rb", "lib/kafka/consumer.rb", "lib/kafka/io.rb", "lib/kafka/message.rb", "lib/kafka/producer.rb", "lib/kafka/request_type.rb", "lib/kafka/error_codes.rb", "lib/kafka.rb", "spec/batch_spec.rb", "spec/consumer_spec.rb", "spec/io_spec.rb", "spec/kafka_spec.rb", "spec/message_spec.rb", "spec/producer_spec.rb", "spec/spec_helper.rb"]
  s.homepage = %q{http://github.com/acrosa/kafka-rb}
  s.require_paths = ["lib"]
  s.rubygems_version = %q{1.3.7}
  s.summary = %q{A Ruby client for the Kafka distributed publish/subscribe messaging service}

  if s.respond_to? :specification_version then
    current_version = Gem::Specification::CURRENT_SPECIFICATION_VERSION
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<rspec>, [">= 0"])
    else
      s.add_dependency(%q<rspec>, [">= 0"])
    end
  else
    s.add_dependency(%q<rspec>, [">= 0"])
  end
end
