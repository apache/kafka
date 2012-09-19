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
/*
 * encoder_helper.hpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_ENCODER_HELPER_HPP_
#define KAFKA_ENCODER_HELPER_HPP_

#include <ostream>
#include <string>

#include <arpa/inet.h>
#include <boost/crc.hpp>

#include <stdint.h>

namespace kafkaconnect {
namespace test { class encoder_helper; }

const uint16_t kafka_format_version = 0;

const uint8_t message_format_magic_number = 0;
const uint8_t message_format_extra_data_size = 1 + 4;
const uint8_t message_format_header_size = message_format_extra_data_size + 4;

class encoder_helper
{
private:
	friend class test::encoder_helper;
	template <typename T> friend void encode(std::ostream&, const std::string&, const uint32_t, const T&);

	static std::ostream& message(std::ostream& stream, const std::string message)
	{
		// Message format is ... message & data size (4 bytes)
		raw(stream, htonl(message_format_extra_data_size + message.length()));

		// ... magic number (1 byte)
		stream << message_format_magic_number;

		// ... string crc32 (4 bytes)
		boost::crc_32_type result;
		result.process_bytes(message.c_str(), message.length());
		raw(stream, htonl(result.checksum()));

		// ... message string bytes
		stream << message;

		return stream;
	}

	template <typename Data>
	static std::ostream& raw(std::ostream& stream, const Data& data)
	{
		stream.write(reinterpret_cast<const char*>(&data), sizeof(Data));
		return stream;
	}
};

}

#endif /* KAFKA_ENCODER_HELPER_HPP_ */
