#include "irods/private/audit_amqp.hpp"
#include "irods/private/audit_b64enc.hpp"
#include "irods/private/audit_config.hpp"
#include "irods/private/amqp_sender.hpp"

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_error.hpp>
#include <irods/irods_exception.hpp>
#include <irods/irods_logger.hpp>
#include <irods/irods_state_table.h>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_serialization.hpp>
#include <irods/irods_re_structs.hpp>
#include <irods/msParam.h>
#include <irods/rodsDef.h>
#include <irods/rodsErrorTable.h>

#include <boost/any.hpp>

#include <fmt/format.h>
#include <fmt/compile.h>

#include <nlohmann/json.hpp>

#include <proton/error.hpp>

#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <exception>
#include <iostream>
#include <list>
#include <map>
#include <regex>
#include <string>
#include <vector>
#include <version>
#include <utility>

// filesystem
// clang-format off
#ifdef __cpp_lib_filesystem
#include <filesystem>
#include <system_error>
namespace fs = std::filesystem;
using error_code_type = std::error_code;
#else
#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
namespace fs = boost::filesystem;
using error_code_type = boost::system::error_code;
#endif
// clang-format on

namespace irods::plugin::rule_engine::audit_amqp
{
	namespace
	{
		// NOLINTBEGIN(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)
		plugin_config audit_config;
		irods::error error_state;

		amqp_sender audit_amqp_sender;

		fs::path log_file_path;
		std::ofstream log_file_ofstream;
		// NOLINTEND(cert-err58-cpp, cppcoreguidelines-avoid-non-const-global-variables)

		template <class Logger>
		void log_test_mode_diag(const Logger& _logger,
		                        const std::string& _log_message,
		                        const std::string& _instance_name,
		                        const std::string& _test_mode_log_path)
		{
			// clang-format off
			_logger({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_file_path", _test_mode_log_path},
				{"log_message", _log_message},
			});
			// clang-format on
		}

		template <class Logger>
		void log_test_mode_diag(const Logger& _logger,
		                        const std::string& _log_message,
		                        const std::string& _instance_name)
		{
			// clang-format off
			_logger({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", _log_message},
			});
			// clang-format on
		}

	} // namespace

	static irods::error setup([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::debug({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", "setup called"},
		});
		// clang-format on
#endif
		try {
			// test log should never throw exceptions
			log_file_ofstream.exceptions(static_cast<std::ios_base::iostate>(0));

			irods::error ret = audit_config.initialize(_instance_name);
			if (!ret.ok()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _instance_name},
					{"log_message", "Error loading plugin configuration"},
					{"error_result", ret.result()},
				});
				// clang-format on

				error_state = PASSMSG("Error loading plugin configuration", ret);
				return error_state;
			}

			ret = audit_amqp_sender.configure(_instance_name, audit_config.amqp_config());
			if (!ret.ok()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _instance_name},
					{"log_message", "Error establishing AMQP connection"},
					{"error_result", ret.result()},
				});
				// clang-format on

				error_state = PASSMSG("Error configuring amqp_sender", ret);
				return error_state;
			}
		}
		catch (const irods::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught iRODS exception", e_what, _instance_name);
			error_state =
				ERROR(e.code(), fmt::format(FMT_COMPILE("Unhandled exception during plugin setup: {}"), e_what));
			return error_state;
		}
		catch (const std::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught exception", e_what, _instance_name);
			error_state = ERROR(
				SYS_INTERNAL_ERR, fmt::format(FMT_COMPILE("Unhandled exception during plugin setup: {}"), e_what));
			return error_state;
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "Caught unknown exception"}
			});
			// clang-format on
			error_state = ERROR(SYS_UNKNOWN_ERROR, "Unknown error during plugin setup.");
			return error_state;
		}

		error_state = SUCCESS();
		return error_state;
	} // setup

	static irods::error teardown([[maybe_unused]] irods::default_re_ctx& _re_ctx,
	                             [[maybe_unused]] const std::string& _instance_name)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::debug({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", "teardown called"},
		});
		// clang-format on
#endif
		// reset error state
		error_state = SUCCESS();
		return error_state;
	} // teardown

	static irods::error start([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::debug({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", "start called"},
		});
		// clang-format on
#endif

		if (!error_state.ok()) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "start called with plugin in error state"},
				{"error_state::result", error_state.result()},
			});
			// clang-format on

			return error_state;
		}

		nlohmann::json json_obj;
		const pid_t pid = getpid();

		try {
			const std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);

			irods::error ret = audit_amqp_sender.open();
			if (!ret.ok()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _instance_name},
					{"log_message", "Error establishing AMQP connection"},
					{"error_result", ret.result()},
				});
				// clang-format on

				error_state = PASSMSG("Error establishing AMQP connection", ret);
				return error_state;
			}

			json_obj["action"] = "START";

			if (audit_config.test_mode_enabled()) {
				if (audit_config.test_mode_log_path_prefix().empty()) {
					log_test_mode_diag(
						log_re::trace, "log_path_prefix is empty. cannot open test log.", _instance_name);
					log_file_path.clear();
					// ensure log_file_ofstream is closed
					if (log_file_ofstream.is_open()) {
						log_test_mode_diag(log_re::trace, "log_file_ofstream open. Closing.", _instance_name);
						log_file_ofstream.close();
						if (!log_file_ofstream.good()) {
							log_test_mode_diag(log_re::error, "Error closing log_file_ofstream.", _instance_name);
						}
					}
				}
				else {
					log_file_path =
						audit_config.test_mode_log_path_prefix() / fmt::format(FMT_COMPILE("{0:08d}.txt"), pid);
					json_obj["log_file"] = log_file_path;

					const std::string log_file_path_str = log_file_path.string();

					if (log_file_ofstream.is_open()) {
						log_test_mode_diag(log_re::trace,
						                   "log_file_ofstream already open. Closing.",
						                   _instance_name,
						                   log_file_path_str);
						log_file_ofstream.close();
						if (!log_file_ofstream.good()) {
							log_test_mode_diag(log_re::error, "Error closing log_file_ofstream.", _instance_name);
						}
					}

					error_code_type mkdirs_ec;
					fs::create_directories(log_file_path.parent_path(), mkdirs_ec);

					log_test_mode_diag(log_re::trace, "opening log_file_ofstream.", _instance_name, log_file_path_str);
					log_file_ofstream.open(log_file_path, std::ios_base::out | std::ios_base::ate);
					if (!log_file_ofstream.good()) {
						log_test_mode_diag(
							log_re::error, "Error opening log_file_ofstream.", _instance_name, log_file_path_str);
					}
				}
			}
			else {
				// ensure log_file_ofstream is closed
				if (log_file_ofstream.is_open()) {
					log_test_mode_diag(
						log_re::error, "Test mode disabled but log_file_ofstream open. Closing.", _instance_name);
					log_file_ofstream.close();
					if (!log_file_ofstream.good()) {
						log_test_mode_diag(log_re::error, "Error closing log_file_ofstream.", _instance_name);
					}
				}
			}

			const auto err = audit_amqp_sender.send_message(json_obj, time_ms, pid, log_file_ofstream);
			if (!err.ok()) {
				error_state = SUCCESS();
				return err;
			}
		}
		catch (const irods::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught iRODS exception", e_what, _instance_name);
			error_state =
				ERROR(e.code(), fmt::format(FMT_COMPILE("Unhandled iRODS exception during plugin start: {}"), e_what));
			return error_state;
		}
		catch (const nlohmann::json::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught nlohmann-json exception", e_what, _instance_name);
			error_state =
				ERROR(SYS_LIBRARY_ERROR,
				      fmt::format(FMT_COMPILE("Unhandled nlohmann-json exception during plugin start: {}"), e_what));
			return error_state;
		}
		catch (const proton::error& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught qpid-proton exception", e_what, _instance_name);
			error_state =
				ERROR(SYS_LIBRARY_ERROR,
				      fmt::format(FMT_COMPILE("Unhandled qpid-proton exception during plugin start: {}"), e_what));
			return error_state;
		}
		catch (const std::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught exception", e_what, _instance_name);
			error_state = ERROR(
				SYS_INTERNAL_ERR, fmt::format(FMT_COMPILE("Unhandled exception during plugin start: {}"), e_what));
			return error_state;
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "Caught unknown exception"}
			});
			// clang-format on
			error_state = ERROR(SYS_UNKNOWN_ERROR, "Unknown error during plugin start.");
			return error_state;
		}

		error_state = SUCCESS();
		return error_state;
	}

	static auto stop([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _instance_name) -> irods::error
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::debug({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", "stop called"},
		});
		// clang-format on
#endif

		if (!error_state.ok()) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "stop called with plugin in error state"},
				{"error_state::result", error_state.result()},
			});
			// clang-format on

			return error_state;
		}

		nlohmann::json json_obj;
		irods::error ret = SUCCESS();

		try {
			const std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);

			json_obj["action"] = "STOP";
			if (audit_config.test_mode_enabled() && !log_file_path.empty()) {
				json_obj["log_file"] = log_file_path;
			}

			const auto err = audit_amqp_sender.send_message(json_obj, time_ms, getpid(), log_file_ofstream);
			audit_amqp_sender.close();
			if (!err.ok()) {
				ret = err;
			}
		}
		catch (const irods::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught iRODS exception", e_what, _instance_name);
			ret = ERROR(e.code(), fmt::format(FMT_COMPILE("Unhandled iRODS exception during plugin stop: {}"), e_what));
		}
		catch (const nlohmann::json::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught nlohmann-json exception", e_what, _instance_name);
			ret = ERROR(SYS_LIBRARY_ERROR,
			            fmt::format(FMT_COMPILE("Unhandled nlohmann-json exception during plugin stop: {}"), e_what));
		}
		catch (const proton::error& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught qpid-proton exception", e_what, _instance_name);
			ret = ERROR(SYS_LIBRARY_ERROR,
			            fmt::format(FMT_COMPILE("Unhandled qpid-proton exception during plugin stop: {}"), e_what));
		}
		catch (const std::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught exception", e_what, _instance_name);
			ret =
				ERROR(SYS_INTERNAL_ERR, fmt::format(FMT_COMPILE("Unhandled exception during plugin stop: {}"), e_what));
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "Caught unknown exception"}
			});
			// clang-format on
			ret = ERROR(SYS_UNKNOWN_ERROR, "Unknown error during plugin stop.");
		}

		log_file_ofstream.close();
		if (!log_file_ofstream.good()) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"log_message", "Error closing log_file_ofstream."}
			});
			// clang-format on
		}

		return ret;
	}

	static auto rule_exists([[maybe_unused]] irods::default_re_ctx& _re_ctx, const std::string& _rn, bool& _ret)
		-> irods::error
	{
		try {
			if (audit_config.pep_regex().has_value()) {
				std::smatch matches;
				_ret = std::regex_match(_rn, matches, audit_config.pep_regex().value());
				if ((audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) &&
				    !error_state.ok())
				{
					// TODO: should we be doing this?
					return error_state;
				}
			}
			else if (!error_state.ok()) {
				return error_state;
			}
			else {
				// if we wind up here, something terrible has happened.
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{"log_message", "No pep_regex, but no error_state."}
				});
				// clang-format on
				return ERROR(RE_RUNTIME_ERROR, "No pep_regex, but no error_state.");
			}
		}
		catch (const std::exception& _e) {
			return ERROR(SYS_INTERNAL_ERR, _e.what());
		}
		catch (...) {
			return ERROR(SYS_UNKNOWN_ERROR, "An unknown error occurred");
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
		const std::string& _instance_name = audit_amqp_sender.re_instance_name();

#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::debug({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", "exec_rule called"},
		});
		// clang-format on
#endif

		if (!error_state.ok()) {
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::ALLOW_OPERATION) {
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _instance_name},
					{"rule_name", _rn},
					{"log_message", "Plugin is in error state. Skipping audit."}
				});
				// clang-format on
				return CODE(RULE_ENGINE_CONTINUE);
			}
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"rule_name", _rn},
				{"log_message", "Plugin is in error state. Returning previous error."}
			});
			// clang-format on
			return error_state;
		}

		// stores a counter of unique arg types
		std::map<std::string, std::size_t> arg_type_map;

		ruleExecInfo_t* rei = nullptr;
		if (const auto err = _eff_hdlr("unsafe_ms_ctx", &rei); !err.ok()) {
			// clang-format off
			log_re::trace({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"rule_name", _rn},
				{"log_message", "Could not get rule execution context (REI)"},
				{"error_result", err.result()}
			});
			// clang-format on

			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::ALLOW_OPERATION) {
				return CODE(RULE_ENGINE_CONTINUE);
			}
			return err;
		}

		nlohmann::json json_obj;

		try {
			const std::uint64_t time_ms = ts_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
			json_obj["rule_name"] = _rn;

			for (const auto& itr : _ps) {
				// The BytesBuf parameter should not be serialized because this commonly contains
				// the entirety of the contents of files. These could be very big and cause the
				// message broker to explode.
				if (std::type_index(typeid(BytesBuf*)) == std::type_index(itr.type())) {
					// clang-format off
					log_re::trace({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _instance_name},
						{"rule_name", _rn},
						{"log_message", "Skipping serialization of BytesBuf parameter"}
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
						{irods::KW_CFG_INSTANCE_NAME, _instance_name},
						{"rule_name", _rn},
						{"log_message", "Failed to serialize argument"},
						{"error_result", ret.result()}
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

			auto err = audit_amqp_sender.send_message(json_obj, time_ms, getpid(), log_file_ofstream);
			if (!err.ok() && (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION)) {
				return err;
			}
		}
		catch (const irods::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught iRODS exception", e_what, _instance_name, _rn);
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) {
				return ERROR(
					e.code(), fmt::format(FMT_COMPILE("Unhandled iRODS exception during exec_rule: {}"), e_what));
			}
		}
		catch (const nlohmann::json::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught nlohmann-json exception", e_what, _instance_name, _rn);
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) {
				return ERROR(
					SYS_LIBRARY_ERROR,
					fmt::format(FMT_COMPILE("Unhandled nlohmann-json exception during exec_rule: {}"), e_what));
			}
		}
		catch (const proton::error& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught qpid-proton exception", e_what, _instance_name, _rn);
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) {
				return ERROR(SYS_LIBRARY_ERROR,
				             fmt::format(FMT_COMPILE("Unhandled qpid-proton exception during exec_rule: {}"), e_what));
			}
		}
		catch (const std::exception& e) {
			const std::string e_what = e.what();
			log_exception(log_re::error, "Caught exception", e_what, _instance_name, _rn);
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) {
				return ERROR(
					SYS_INTERNAL_ERR, fmt::format(FMT_COMPILE("Unhandled exception during exec_rule: {}"), e_what));
			}
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _instance_name},
				{"rule_name", _rn},
				{"log_message", "Caught unknown exception"}
			});
			// clang-format on
			if (audit_config.failsafe_mode() == plugin_config::failsafe_mode::BLOCK_OPERATION) {
				return ERROR(SYS_UNKNOWN_ERROR, "Unknown error during exec_rule.");
			}
		}

		return CODE(RULE_ENGINE_CONTINUE);
	}
} // namespace irods::plugin::rule_engine::audit_amqp

//
// Plugin Factory
//

using pluggable_rule_engine = irods::pluggable_rule_engine<irods::default_re_ctx>;

extern "C" auto plugin_factory(const std::string& _inst_name, const std::string& _context) -> pluggable_rule_engine*
{
	using namespace irods::plugin::rule_engine::audit_amqp;

	const auto not_supported = [](auto&&...) { return ERROR(SYS_NOT_SUPPORTED, "Not supported."); };

	auto* rule_engine = new irods::pluggable_rule_engine<irods::default_re_ctx>(_inst_name, _context);

	rule_engine->add_operation("setup", std::function{setup});
	rule_engine->add_operation("teardown", std::function{teardown});

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
