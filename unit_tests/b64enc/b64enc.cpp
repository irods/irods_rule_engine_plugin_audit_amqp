#define CATCH_CONFIG_MAIN
#include <catch2/catch.hpp>

#include "irods/private/audit_amqp.hpp"
#include "irods/private/audit_b64enc.hpp"

#include <string>

#include <nlohmann/json.hpp>

TEST_CASE("as-needed base64 encoding")
{
	nlohmann::json json_obj;
	std::string key;
	std::string val;

	SECTION("valid strings")
	{
		auto check_valid_str = [&]() {
			// insert value
			const std::uint64_t time_ms = irods::plugin::rule_engine::audit_amqp::ts_clock::now().time_since_epoch() /
			                              std::chrono::milliseconds(1);
			irods::plugin::rule_engine::audit_amqp::insert_as_string_or_base64(json_obj, key, val, time_ms);

			// verify b64 key does not exist
			const auto b64val_container = json_obj.find(key + "_b64");
			CHECK(b64val_container == json_obj.end());

			// verify key exists
			const auto val_container = json_obj.find(key);
			REQUIRE(val_container != json_obj.end());

			// verify value has not changed
			const auto& val_out = val_container->get_ref<const std::string&>();
			CHECK(val_out == val);
		};

		SECTION("valid ascii")
		{
			key = "valid_ascii";
			val = "asdf1234";

			check_valid_str();
		}

		SECTION("string with nulls")
		{
			key = "valid_nulls";
			val = "aaa\0bbb\0ccc";

			check_valid_str();
		}

		SECTION("valid 2-octet sequence")
		{
			key = "valid_2oct";
			val = "\xc3\xb1";

			check_valid_str();
		}

		SECTION("valid 3-octet sequence")
		{
			key = "valid_3oct";
			val = "\xe2\x82\xa1";

			check_valid_str();
		}

		SECTION("valid 4-octet sequence")
		{
			key = "valid_4oct";
			val = "\xf0\x90\x8c\xbc";

			check_valid_str();
		}
	}

	SECTION("invalid strings")
	{
		std::string b64val;

		auto check_invalid_str = [&]() {
			// insert value
			const std::uint64_t time_ms = irods::plugin::rule_engine::audit_amqp::ts_clock::now().time_since_epoch() /
			                              std::chrono::milliseconds(1);
			irods::plugin::rule_engine::audit_amqp::insert_as_string_or_base64(json_obj, key, val, time_ms);

			// verify non-b64 key does not exist
			const auto val_container = json_obj.find(key);
			CHECK(val_container == json_obj.end());

			// verify b64 key exists
			const auto b64val_container = json_obj.find(key + "_b64");
			REQUIRE(b64val_container != json_obj.end());

			// verify encoded value
			const auto& b64val_out = b64val_container->get_ref<const std::string&>();
			CHECK(b64val_out == b64val);
		};

		SECTION("string with quotes")
		{
			key = "has_quot";
			val = "\"normal\" and \"boring\"";
			b64val = "Im5vcm1hbCIgYW5kICJib3Jpbmci";

			check_invalid_str();
		}

		SECTION("string with unix newlines")
		{
			key = "has_endl_unix";
			val = "we\nare\nmultiline";
			b64val = "d2UKYXJlCm11bHRpbGluZQ==";

			check_invalid_str();
		}

		SECTION("string with windows newlines")
		{
			key = "has_endl_win32";
			val = "we\r\nare\r\nmultiline";
			b64val = "d2UNCmFyZQ0KbXVsdGlsaW5l";

			check_invalid_str();
		}

		SECTION("string with tabs")
		{
			key = "valid_nulls";
			val = "tab\tstops\there";
			b64val = "dGFiCXN0b3BzCWhlcmU=";

			check_invalid_str();
		}

		SECTION("invalid 2-octet sequence")
		{
			key = "invalid_2oct";
			val = "\xc3\x28";
			b64val = "wyg=";

			check_invalid_str();
		}

		SECTION("invalid sequence identifier")
		{
			key = "invalid_seqid";
			val = "\xa0\xa1";
			b64val = "oKE=";

			check_invalid_str();
		}

		SECTION("invalid 3-octet sequence in octet 2")
		{
			key = "invalid_3oct2";
			val = "\xe2\x28\xa1";
			b64val = "4iih";

			check_invalid_str();
		}

		SECTION("invalid 3-octet sequence in octet 3")
		{
			key = "invalid_3oct3";
			val = "\xe2\x82\x28";
			b64val = "4oIo";

			check_invalid_str();
		}

		SECTION("invalid 4-octet sequence in octet 2")
		{
			key = "invalid_4oct2";
			val = "\xf0\x28\x8c\xbc";
			b64val = "8CiMvA==";

			check_invalid_str();
		}

		SECTION("invalid 4-octet sequence in octet 3")
		{
			key = "invalid_4oct3";
			val = "\xf0\x90\x28\xbc";
			b64val = "8JAovA==";

			check_invalid_str();
		}

		SECTION("invalid 4-octet sequence in octet 4")
		{
			key = "invalid_4oct4";
			val = "\xf0\x90\x8c\x28";
			b64val = "8JCMKA==";

			check_invalid_str();
		}

		SECTION("semi-valid 5-octet sequence")
		{
			key = "semivalid_5oct";
			val = "\xf8\xa1\xa1\xa1\xa1";
			b64val = "+KGhoaE=";

			check_invalid_str();
		}

		SECTION("semi-valid 6-octet sequence")
		{
			key = "semivalid_6oct";
			val = "\xfc\xa1\xa1\xa1\xa1\xa1";
			b64val = "/KGhoaGh";

			check_invalid_str();
		}
	}
}
