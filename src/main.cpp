// irods includes
#include "irods/private/audit_amqp.hpp"
#include "irods/private/audit_b64enc.hpp"
#include "irods/private/amqp_sender.hpp"
#include <irods/irods_logger.hpp>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_serialization.hpp>
#include <irods/irods_server_properties.hpp>

// LIST is #defined in irods/reconstants.hpp
// and is an enum entry in proton/type_id.hpp
#ifdef LIST
#  undef LIST
#endif

// boost includes
#include <boost/any.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <boost/config.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>

// proton-cpp includes
#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/timestamp.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>
#include <proton/sender.hpp>
#include <proton/session.hpp>

// misc includes
#include <nlohmann/json.hpp>
#include <fmt/core.h>
#include <fmt/compile.h>

// stl includes
#include <cstdint>
#include <version>
#include <iostream>
#include <vector>
#include <string>
#include <string_view>
#include <chrono>
#include <map>
#include <fstream>
#include <mutex>
#include <regex>

// filesystem
// clang-format off
#ifdef __cpp_lib_filesystem
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;
#endif
// clang-format on

namespace irods::plugin::rule_engine::audit_amqp
{
	namespace
	{
		const auto pep_regex_flavor = std::regex::ECMAScript;

		// NOLINTBEGIN(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)
		const std::string_view default_pep_regex_to_match{"audit_.*"};
		const std::string_view default_amqp_url{"localhost:5672/irods_audit_messages"};

		const fs::path default_log_path_prefix{fs::temp_directory_path()};
		const bool default_test_mode = false;

		std::string audit_pep_regex_to_match;
		std::string audit_amqp_url;

		fs::path log_path_prefix;
		bool test_mode;

		bool warned_amqp_options = false;

		fs::path log_file_path;
		std::ofstream log_file_ofstream;

		// audit_pep_regex is initially populated with an unoptimized default, as optimization
		// makes construction slower, and we don't expect it to be used before configuration is read.
		std::regex audit_pep_regex{audit_pep_regex_to_match, pep_regex_flavor};

		std::mutex audit_plugin_mutex;
		// NOLINTEND(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)
	} // namespace

	static BOOST_FORCEINLINE void set_default_configs()
	{
		audit_pep_regex_to_match = default_pep_regex_to_match;
		audit_amqp_url = default_amqp_url;
		test_mode = default_test_mode;
		log_path_prefix = default_log_path_prefix;

		audit_pep_regex = std::regex(audit_pep_regex_to_match, pep_regex_flavor | std::regex::optimize);
	}

	static auto get_re_configs(const std::string& _instance_name) -> irods::error
	{
		try {
			const auto& rule_engines = irods::get_server_property<const nlohmann::json&>(
				std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE});
			for (const auto& rule_engine : rule_engines) {
				const auto& inst_name = rule_engine.at(irods::KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
				if (inst_name != _instance_name) {
					continue;
				}

				if (rule_engine.count(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION) <= 0) {
					set_default_configs();
					// clang-format off
					log_re::debug({
						{"rule_engine_plugin", rule_engine_name},
						{"log_message", "Using default plugin configuration"},
						{"instance_name", _instance_name},
					});
					// clang-format on

					return SUCCESS();
				}

				const auto& plugin_spec_cfg = rule_engine.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);

				audit_pep_regex_to_match = plugin_spec_cfg.at("pep_regex_to_match").get<std::string>();

				const auto& amqp_topic = plugin_spec_cfg.at("amqp_topic").get_ref<const std::string&>();
				const auto& amqp_location = plugin_spec_cfg.at("amqp_location").get_ref<const std::string&>();
				audit_amqp_url = fmt::format(FMT_STRING("{0:s}/{1:s}"), amqp_location, amqp_topic);

				// test_mode is optional
				const auto test_mode_cfg = plugin_spec_cfg.find("test_mode");
				if (test_mode_cfg == plugin_spec_cfg.end()) {
					test_mode = default_test_mode;
				}
				else {
					const auto& test_mode_str = test_mode_cfg->get_ref<const std::string&>();
					test_mode = boost::iequals(test_mode_str, "true");
				}

				// log_path_prefix is optional
				const auto log_path_prefix_cfg = plugin_spec_cfg.find("log_path_prefix");
				if (log_path_prefix_cfg == plugin_spec_cfg.end()) {
					log_path_prefix = default_log_path_prefix;
				}
				else {
					log_path_prefix = log_path_prefix_cfg->get<std::string>();
				}

				// look for amqp_options and log a warning if it is present
				const auto amqp_options_cfg = plugin_spec_cfg.find("amqp_options");
				if (amqp_options_cfg != plugin_spec_cfg.end() && !warned_amqp_options) {
					// clang-format off
					log_re::warn({
						{"rule_engine_plugin", rule_engine_name},
						{"log_message", "Found amqp_options configuration setting. This setting is no longer used and "
						                "should be removed from the plugin configuration."},
						{"instance_name", _instance_name},
					});
					// clang-format on
					warned_amqp_options = true;
				}

				audit_pep_regex = std::regex(audit_pep_regex_to_match, pep_regex_flavor | std::regex::optimize);

				return SUCCESS();
			}
		}
		catch (const std::out_of_range& e) {
			return ERROR(KEY_NOT_FOUND, e.what());
		}
		catch (const nlohmann::json::exception& e) {
			return ERROR(SYS_LIBRARY_ERROR, e.what());
		}
		catch (const std::exception& e) {
			return ERROR(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "an unknown error occurred");
		}

		return ERROR(SYS_INVALID_INPUT_PARAM, "failed to find plugin configuration");
	}

	static auto start([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name)
		-> irods::error
	{
		std::lock_guard<std::mutex> lock(audit_plugin_mutex);

		irods::error ret = get_re_configs(_instance_name);
		if (!ret.ok()) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{"log_message", "Error loading plugin configuration"},
				{"instance_name", _instance_name},
				{"error_result", ret.result()},
			});
			// clang-format on
		}

		nlohmann::json json_obj;

		std::string msg_str;

		try {
			std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["@timestamp"] = time_ms;
			json_obj["hostname"] = boost::asio::ip::host_name();

			pid_t pid = getpid();
			json_obj["pid"] = pid;

			json_obj["action"] = "START";

			if (test_mode) {
				log_file_path = log_path_prefix / fmt::format(FMT_STRING("{0:08d}.txt"), pid);
				json_obj["log_file"] = log_file_path;
			}

			msg_str = json_obj.dump();

			proton::message msg(msg_str);
			msg.content_type("application/json");
			msg.creation_time(proton::timestamp(static_cast<proton::timestamp::numeric_type>(time_ms)));
			send_handler handler(msg, audit_amqp_url);
			proton::container(handler).run();
		}
		catch (const irods::exception& e) {
			log_exception(e, "Caught iRODS exception", {"instance_name", _instance_name});
			return ERROR(e.code(), e.what());
		}
		catch (const nlohmann::json::exception& e) {
			log_exception(e, "Caught nlohmann-json exception", {"instance_name", _instance_name});
			return ERROR(SYS_LIBRARY_ERROR, e.what());
		}
		catch (const std::exception& e) {
			log_exception(e, "Caught exception", {"instance_name", _instance_name});
			return ERROR(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "an unknown error occurred");
		}

		if (test_mode) {
			if (!log_file_ofstream.is_open()) {
				log_file_ofstream.open(log_file_path);
			}
			log_file_ofstream << msg_str << std::endl;
		}

		return SUCCESS();
	}

	static auto stop([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name) -> irods::error
	{
		std::lock_guard<std::mutex> lock(audit_plugin_mutex);

		nlohmann::json json_obj;

		std::string msg_str;
		std::string log_file;

		try {
			std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["@timestamp"] = time_ms;

			json_obj["hostname"] = boost::asio::ip::host_name();
			json_obj["pid"] = getpid();
			json_obj["action"] = "STOP";

			if (test_mode) {
				json_obj["log_file"] = log_file_path;
			}

			msg_str = json_obj.dump();

			proton::message msg(msg_str);
			msg.content_type("application/json");
			msg.creation_time(proton::timestamp(static_cast<proton::timestamp::numeric_type>(time_ms)));
			send_handler handler(msg, audit_amqp_url);
			proton::container(handler).run();
		}
		catch (const irods::exception& e) {
			log_exception(e, "Caught iRODS exception", {"instance_name", _instance_name});
			return ERROR(e.code(), e.what());
		}
		catch (const nlohmann::json::exception& e) {
			log_exception(e, "Caught nlohmann-json exception", {"instance_name", _instance_name});
			return ERROR(SYS_LIBRARY_ERROR, e.what());
		}
		catch (const std::exception& e) {
			log_exception(e, "Caught exception", {"instance_name", _instance_name});
			return ERROR(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "an unknown error occurred");
		}

		if (test_mode) {
			if (!log_file_ofstream.is_open()) {
				log_file_ofstream.open(log_file_path);
			}
			log_file_ofstream << msg_str << std::endl;
			log_file_ofstream.close();
		}

		return SUCCESS();
	}

	static auto rule_exists([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _rn, bool& _ret)
		-> irods::error
	{
		try {
			std::smatch matches;
			_ret = std::regex_match(_rn, matches, audit_pep_regex);
		}
		catch (const std::exception& _e) {
			return ERROR(SYS_INTERNAL_ERR, _e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "an unknown error occurred");
		}

		return SUCCESS();
	}

	static auto list_rules(
		[[maybe_unused]] irods::default_re_ctx& _re_ctx,
		[[maybe_unused]] std::vector<std::string>& _rules) -> irods::error
	{
		return SUCCESS();
	}

	static auto exec_rule(
		[[maybe_unused]] irods::default_re_ctx& _re_ctx,
		const std::string& _rn,
		std::list<boost::any>& _ps,
		irods::callback _eff_hdlr) -> irods::error
	{
		std::lock_guard<std::mutex> lock(audit_plugin_mutex);

		// stores a counter of unique arg types
		std::map<std::string, std::size_t> arg_type_map;

		ruleExecInfo_t* rei = nullptr;
		irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
		if (!err.ok()) {
			return err;
		}

		nlohmann::json json_obj;

		std::string msg_str;
		std::string log_file;

		try {
			std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["@timestamp"] = time_ms;
			json_obj["hostname"] = boost::asio::ip::host_name();
			json_obj["pid"] = getpid();
			json_obj["rule_name"] = _rn;

			for (const auto& itr : _ps) {
				// The BytesBuf parameter should not be serialized because this commonly contains
				// the entirety of the contents of files. These could be very big and cause the
				// message broker to explode.
				if (std::type_index(typeid(BytesBuf*)) == std::type_index(itr.type())) {
					// clang-format off
					log_re::trace({
						{"rule_engine_plugin", rule_engine_name},
						{"log_message", "skipping serialization of BytesBuf parameter"},
						{"rule_name", _rn},
					});
					// clang-format on
					continue;
				}

				// serialize the parameter to a map
				irods::re_serialization::serialized_parameter_t param;
				irods::error ret = irods::re_serialization::serialize_parameter(itr, param);
				if (!ret.ok()) {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{"log_message", "failed to serialize argument"},
						{"rule_name", _rn},
						{"error_result", ret.result()},
					});
					// clang-format on
					continue;
				}

				for (const auto& elem : param) {
					const std::string& arg = elem.first;

					std::size_t ctr;
					const auto iter = arg_type_map.find(arg);
					if (iter == arg_type_map.end()) {
						arg_type_map.insert(std::make_pair(arg, static_cast<std::size_t>(1)));
						ctr = 1;
					}
					else {
						ctr = iter->second + 1;
						iter->second = ctr;
					}

					if (ctr > 1) {
						const std::string key = fmt::format(FMT_COMPILE("{0:s}__{1:d}"), arg, ctr);
						insert_as_string_or_base64(json_obj, key, elem.second, time_ms);
					}
					else {
						insert_as_string_or_base64(json_obj, arg, elem.second, time_ms);
					}
				}
			}

			msg_str = json_obj.dump();

			proton::message msg(msg_str);
			msg.content_type("application/json");
			msg.creation_time(proton::timestamp(static_cast<proton::timestamp::numeric_type>(time_ms)));
			send_handler handler(msg, audit_amqp_url);
			proton::container(handler).run();
		}
		catch (const irods::exception& e) {
			log_exception(e, "Caught iRODS exception", {"rule_name", _rn});
			return ERROR(e.code(), e.what());
		}
		catch (const nlohmann::json::exception& e) {
			log_exception(e, "Caught nlohmann-json exception", {"rule_name", _rn});
			return ERROR(SYS_LIBRARY_ERROR, e.what());
		}
		catch (const std::exception& e) {
			log_exception(e, "Caught exception", {"rule_name", _rn});
			return ERROR(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "an unknown error occurred");
		}

		if (test_mode) {
			if (!log_file_ofstream.is_open()) {
				log_file_ofstream.open(log_file_path);
			}
			log_file_ofstream << msg_str << std::endl;
		}

		return err;
	}
} // namespace irods::plugin::rule_engine::audit_amqp

//
// Plugin Factory
//

using pluggable_rule_engine = irods::pluggable_rule_engine<irods::default_re_ctx>;

extern "C" auto plugin_factory(const std::string& _inst_name, const std::string& _context) -> pluggable_rule_engine*
{
	using namespace irods::plugin::rule_engine::audit_amqp;

	set_default_configs();

	const auto not_supported = [](auto&&...) { return ERROR(SYS_NOT_SUPPORTED, "Not supported."); };

	auto* rule_engine = new irods::pluggable_rule_engine<irods::default_re_ctx>(_inst_name, _context);

	rule_engine->add_operation("start", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(start));

	rule_engine->add_operation("stop", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(stop));

	rule_engine->add_operation(
		"rule_exists", std::function<irods::error(irods::default_re_ctx&, const std::string&, bool&)>(rule_exists));

	rule_engine->add_operation(
		"list_rules", std::function<irods::error(irods::default_re_ctx&, std::vector<std::string>&)>(list_rules));

	rule_engine->add_operation(
		"exec_rule",
		std::function<irods::error(
			irods::default_re_ctx&, const std::string&, std::list<boost::any>&, irods::callback)>(exec_rule));

	rule_engine->add_operation(
		"exec_rule_text",
		std::function<irods::error(
			irods::default_re_ctx&, const std::string&, msParamArray_t*, const std::string&, irods::callback)>(
			not_supported));

	rule_engine->add_operation(
		"exec_rule_expression",
		std::function<irods::error(irods::default_re_ctx&, const std::string&, msParamArray_t*, irods::callback)>(
			not_supported));

	return rule_engine;
}
