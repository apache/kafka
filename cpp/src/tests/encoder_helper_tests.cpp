/*
 * encoder_helper_tests.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE kafkaconnect
#include <boost/test/unit_test.hpp>

#include <arpa/inet.h>

#include "../encoder_helper.hpp"

// test wrapper
namespace kafkaconnect { namespace test {
class encoder_helper {
public:
	static std::ostream& message(std::ostream& stream, const std::string message) { return kafkaconnect::encoder_helper::message(stream, message); }
	template <typename T> static std::ostream& raw(std::ostream& stream, const T& t) { return kafkaconnect::encoder_helper::raw(stream, t); }
};
} }

using namespace kafkaconnect::test;

BOOST_AUTO_TEST_SUITE(kafka_encoder_helper)

BOOST_AUTO_TEST_CASE(encode_raw_char)
{
	std::ostringstream stream;
	char value = 0x1;

	encoder_helper::raw(stream, value);

	BOOST_CHECK_EQUAL(stream.str().length(), 1);
	BOOST_CHECK_EQUAL(stream.str().at(0), value);
}

BOOST_AUTO_TEST_CASE(encode_raw_integer)
{
	std::ostringstream stream;
	int value = 0x10203;

	encoder_helper::raw(stream, htonl(value));

	BOOST_CHECK_EQUAL(stream.str().length(), 4);
	BOOST_CHECK_EQUAL(stream.str().at(0), 0);
	BOOST_CHECK_EQUAL(stream.str().at(1), 0x1);
	BOOST_CHECK_EQUAL(stream.str().at(2), 0x2);
	BOOST_CHECK_EQUAL(stream.str().at(3), 0x3);
}

BOOST_AUTO_TEST_CASE(encode_message)
{
	std::string message = "a simple test";
	std::ostringstream stream;

	encoder_helper::message(stream, message);

	BOOST_CHECK_EQUAL(stream.str().length(), kafkaconnect::message_format_header_size + message.length());
	BOOST_CHECK_EQUAL(stream.str().at(3), 5 + message.length());
	BOOST_CHECK_EQUAL(stream.str().at(4), kafkaconnect::message_format_magic_number);

	for(size_t i = 0; i < message.length(); ++i)
	{
		BOOST_CHECK_EQUAL(stream.str().at(9 + i), message.at(i));
	}
}

BOOST_AUTO_TEST_SUITE_END()
