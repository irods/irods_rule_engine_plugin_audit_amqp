// irods includes
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
#include <boost/config.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/base64_from_binary.hpp>
#include <boost/archive/iterators/transform_width.hpp>
#include <boost/format.hpp>

// proton-cpp includes
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/tracker.hpp>
#include <proton/sender.hpp>

// nlohmann includes
#include <nlohmann/json.hpp>

// fmt includes
#include <fmt/core.h>
#include <fmt/compile.h>

// stl includes
#include <cstdint>
#include <version>
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <map>
#include <fstream>
#include <mutex>

namespace
{

	// NOLINTBEGIN(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)
	std::string audit_pep_regex_to_match{"audit_.*"};
	std::string audit_amqp_topic{"irods_audit_messages"};
	std::string audit_amqp_location{"localhost:5672"};
	std::string audit_amqp_options;
	std::string log_path_prefix{"/tmp"};
	bool test_mode = false;
	std::ofstream log_file_ofstream;

	std::mutex audit_plugin_mutex;
	// NOLINTEND(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)

	const char* const rule_engine_name = "audit_amqp";

	using log_re = irods::experimental::log::rule_engine;

#if __cpp_lib_chrono >= 201907
	// we use millisecond precision in our timestamps, so we want to use a clock
	// that does not implement leap seconds as repeated non-leap seconds, if we can.
	using ts_clock = std::chrono::utc_clock;
#else
	// fallback to system_clock
	using ts_clock = std::chrono::system_clock;
#endif

	// See qpid-cpp docs
	// https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/simple_send_8cpp-example.html
	class send_handler : public proton::messaging_handler
	{
	  public:
		// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
		send_handler(const std::string& _message_body, const std::string& _location, const std::string& _topic)
			: _amqp_location(_location)
			, _amqp_topic(_topic)
			, _message(_message_body)
			, _message_sent(false)
		{
		}

		void on_container_start(proton::container& container) override
		{
			proton::connection_options conn_opts;
			container.open_sender(
				fmt::format(FMT_COMPILE("{0:s}/{1:s}"), _amqp_location, _amqp_topic),
				conn_opts);
		}

		void on_sendable(proton::sender& _sender) override
		{
			if (_sender.credit() && !_message_sent) {
				_sender.send(_message);
				_message_sent = true;
			}
		}

		void on_tracker_accept(proton::tracker& tracker) override
		{
			// we're only sending one message
			// so we don't care about the credit system
			// or tracking confirmed messages
			if (_message_sent) {
				tracker.connection().close();
			}
		}

	  private:
		const std::string& _amqp_location;
		const std::string& _amqp_topic;
		proton::message _message;
		bool _message_sent;
	}; // class send_handler

	template <class T>
	BOOST_FORCEINLINE void log_exception(
		const T& exception,
		const std::string& log_message,
		const irods::experimental::log::key_value& context_info)
	{
		// clang-format off
		log_re::info({
			{"rule_engine_plugin", rule_engine_name},
			{"log_message", log_message},
			context_info,
			{"exception", exception.what()},
		});
		// clang-format on
	}

	BOOST_FORCEINLINE void insert_as_string_or_base64(
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

	// NOLINTNEXTLINE(readability-function-cognitive-complexity)
	auto get_re_configs(const std::string& _instance_name) -> irods::error
	{
		try {
			const auto& rule_engines = irods::get_server_property<const nlohmann::json&>(
				std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE});
			for (const auto& rule_engine : rule_engines) {
				const auto& inst_name = rule_engine.at(irods::KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
				if (inst_name == _instance_name) {
					if (rule_engine.count(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION) > 0) {
						const auto& plugin_spec_cfg = rule_engine.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);

						audit_pep_regex_to_match = plugin_spec_cfg.at("pep_regex_to_match").get<std::string>();
						audit_amqp_topic = plugin_spec_cfg.at("amqp_topic").get<std::string>();
						audit_amqp_location = plugin_spec_cfg.at("amqp_location").get<std::string>();
						audit_amqp_options = plugin_spec_cfg.at("amqp_options").get<std::string>();

						// look for a test mode setting.  if it doesn't exist just keep test_mode at false.
						// if test_mode = true and log_path_prefix isn't set just leave the default
						const auto test_mode_cfg = plugin_spec_cfg.find("test_mode");
						if (test_mode_cfg != plugin_spec_cfg.end()) {
							const auto& test_mode_str = test_mode_cfg->get_ref<const std::string&>();
							test_mode = boost::iequals(test_mode_str, "true");
							if (test_mode) {
								const auto log_path_prefix_cfg = plugin_spec_cfg.find("log_path_prefix");
								if (log_path_prefix_cfg != plugin_spec_cfg.end()) {
									log_path_prefix = log_path_prefix_cfg->get<std::string>();
								}
							}
						}
					}
					else {
						// clang-format off
						log_re::debug({
							{"rule_engine_plugin", rule_engine_name},
							{"log_message", "Using default plugin configuration"},
							{"instance_name", _instance_name},
						});
						// clang-format on
					}

					return SUCCESS();
				}
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

	auto start([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name) -> irods::error
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

		char host_name[MAX_NAME_LEN];
		gethostname(host_name, MAX_NAME_LEN);

		std::string msg_str;
		std::string log_file;

		try {
			std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["@timestamp"] = time_ms;
			json_obj["hostname"] = host_name;

			pid_t pid = getpid();
			json_obj["pid"] = pid;

			json_obj["action"] = "START";

			if (test_mode) {
				log_file = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
				json_obj["log_file"] = log_file;
			}
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

		msg_str = json_obj.dump();
		send_handler s(msg_str, audit_amqp_location, audit_amqp_topic); // NOLINT(readability-identifier-length)
		proton::container(s).run();

		if (test_mode) {
			log_file_ofstream.open(log_file);
			log_file_ofstream << msg_str << std::endl;
		}

		return SUCCESS();
	}

	auto stop([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name) -> irods::error
	{
		std::lock_guard<std::mutex> lock(audit_plugin_mutex);

		nlohmann::json json_obj;

		std::string msg_str;
		std::string log_file;

		try {
			std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["@timestamp"] = time_ms;

			char host_name[MAX_NAME_LEN];
			gethostname(host_name, MAX_NAME_LEN);
			json_obj["hostname"] = host_name;

			pid_t pid = getpid();
			json_obj["pid"] = pid;

			json_obj["action"] = "STOP";

			if (test_mode) {
				json_obj["log_file"] = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
			}
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

		msg_str = json_obj.dump();
		send_handler s(msg_str, audit_amqp_location, audit_amqp_topic); // NOLINT(readability-identifier-length)
		proton::container(s).run();

		if (test_mode) {
			log_file_ofstream << msg_str << std::endl;
			log_file_ofstream.close();
		}

		return SUCCESS();
	}

	auto rule_exists([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _rn, bool& _ret)
		-> irods::error
	{
		try {
			boost::smatch matches;
			boost::regex expr(audit_pep_regex_to_match);
			_ret = boost::regex_match(_rn, matches, expr);
		}
		catch (const boost::exception& _e) {
			std::string what = boost::diagnostic_information(_e);
			return ERROR(SYS_INTERNAL_ERR, what);
		}

		return SUCCESS();
	}

	auto list_rules([[maybe_unused]] irods::default_re_ctx& _re_ctx, [[maybe_unused]] std::vector<std::string>& _rules)
		-> irods::error
	{
		return SUCCESS();
	}

	auto exec_rule(
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

			char host_name[MAX_NAME_LEN];
			gethostname(host_name, MAX_NAME_LEN);
			json_obj["hostname"] = host_name;

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

		msg_str = json_obj.dump();
		send_handler s(msg_str, audit_amqp_location, audit_amqp_topic); // NOLINT(readability-identifier-length)
		proton::container(s).run();

		if (test_mode) {
			log_file_ofstream << msg_str << std::endl;
		}

		return err;
	}

} // namespace

//
// Plugin Factory
//

using pluggable_rule_engine = irods::pluggable_rule_engine<irods::default_re_ctx>;

extern "C" auto plugin_factory(const std::string& _inst_name, const std::string& _context) -> pluggable_rule_engine*
{
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
