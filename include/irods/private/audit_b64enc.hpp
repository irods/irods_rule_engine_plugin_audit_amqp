#ifndef IRODS_AUDIT_AMQP_B64ENC_HPP
#define IRODS_AUDIT_AMQP_B64ENC_HPP

#include "irods/private/audit_amqp.hpp"

#include <string>

#include <boost/config.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <nlohmann/json.hpp>

namespace irods::plugin::rule_engine::audit_amqp
{
	static BOOST_FORCEINLINE void insert_as_string_or_base64(
		nlohmann::json& json_obj,
		const std::string& key,
		const std::string& val,
		const std::uint64_t& time_ms)
	{
		try {
			json_obj[key] = nlohmann::json::parse("\"" + val + "\"");
		}
		catch (const nlohmann::json::exception&) {
			using namespace boost::archive::iterators;
			using b64enc = base64_from_binary<transform_width<std::string::const_iterator, 6, 8>>;

			// encode into base64 string
			std::string val_b64(b64enc(std::begin(val)), b64enc(std::end(val)));
			val_b64.append((3 - val.length() % 3) % 3, '='); // add padding ='s

			// new key for encoded value
			const std::string key_b64 = key + "_b64";

			json_obj[key_b64] = val_b64;

			// clang-format off
			log_re::debug({
				{"rule_engine_plugin", rule_engine_name},
				{"log_message", "Invalid UTF-8 encountered when adding element to message; added as base64"},
				{"element_original_key", key},
				{"element_key", key_b64},
				{"message_timestamp", std::to_string(time_ms)},
			});
			// clang-format on
		}
	}
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_B64ENC_HPP
