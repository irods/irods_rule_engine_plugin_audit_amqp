#include "irods/private/audit_config.hpp"
#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_config.hpp"

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_error.hpp>
#include <irods/irods_server_properties.hpp>
#include <irods/rodsErrorTable.h>

#include <boost/algorithm/string/predicate.hpp>

#include <nlohmann/json.hpp>

#include <exception>
#include <optional>
#include <regex>
#include <stdexcept>
#include <string>
#include <vector>

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
	irods::error plugin_config::load_configuration(const nlohmann::json& _plugin_specific_configuration,
	                                               const std::string& _re_instance_name)
	{
		// This is a bit weird, as we want to attempt loading failsafe mode and pep regex regardless of whether or not
		// the rest of the configuration loads successfully, but for all other configuration options, we want to
		// preserve the previous values if *any* part of the configuration fails to load.

		irods::error ret = SUCCESS();
		std::exception_ptr rethrow_e;

		try {
			const auto failsafe_mode_cfg = _plugin_specific_configuration.find(KW_FAILSAFE_MODE);
			if (failsafe_mode_cfg == _plugin_specific_configuration.end()) {
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("{} absent from configuration. This will be a required setting in future "
					                         "versions of the plugin"),
								 KW_FAILSAFE_MODE)},
				});
				// clang-format on
				failsafe_mode_ = defaults::failsafe_mode;
			}
			else {
				const std::string& failsafe_mode = failsafe_mode_cfg->get_ref<const std::string&>();
				if (boost::iequals(failsafe_mode, "BLOCK_OPERATION")) {
					failsafe_mode_ = plugin_config::failsafe_mode::BLOCK_OPERATION;
				}
				else if (boost::iequals(failsafe_mode, "ALLOW_OPERATION")) {
					failsafe_mode_ = plugin_config::failsafe_mode::ALLOW_OPERATION;
				}
				else {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("{} must be one of [BLOCK_OPERATION, ALLOW_OPERATION]."),
						             KW_FAILSAFE_MODE)},
						{KW_FAILSAFE_MODE, failsafe_mode},
					});
					// clang-format on
					failsafe_mode_ = defaults::failsafe_mode;
					ret = ERROR(
						SYS_CONFIG_FILE_ERR, fmt::format(FMT_COMPILE("Unrecognized {} value."), KW_FAILSAFE_MODE));
				}
			}
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message",
				 fmt::format(FMT_COMPILE("Error while parsing {}."),
				             KW_FAILSAFE_MODE)},
			});
			// clang-format on
			failsafe_mode_ = defaults::failsafe_mode;
			rethrow_e = std::current_exception();
		}

		std::string new_pep_regex_str;
		try {
			new_pep_regex_str = _plugin_specific_configuration.at(KW_PEP_REGEX).get_ref<const std::string&>();
			std::regex new_pep_regex;
			new_pep_regex = std::regex(new_pep_regex_str, pep_regex_flags_);
			pep_regex_ = new_pep_regex;
		}
		catch (const std::regex_error& e) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message", "Failed to compile pep regex"},
				{"std::regex_error.what()", e.what()},
				{"std::regex_error.code()", std::to_string(e.code())},
				{KW_PEP_REGEX, new_pep_regex_str}
			});
			if (failsafe_mode_ == failsafe_mode::BLOCK_OPERATION) {
				// discard previous value if we are to block operations
				pep_regex_ = defaults::pep_regex;
			}
			// clang-format on
			if (ret.ok() && !rethrow_e) {
				ret = ERROR(INVALID_REGEXP, "Failed to compile pep regex");
			}
		}
		catch (...) {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message", fmt::format(FMT_COMPILE("Error while parsing {}."), KW_PEP_REGEX)},
			});
			// clang-format on
			if (failsafe_mode_ == failsafe_mode::BLOCK_OPERATION) {
				// discard previous value if we are to block operations
				pep_regex_ = defaults::pep_regex;
			}
			if (ret.ok() && !rethrow_e) {
				// favor error from failsafe mode
				rethrow_e = std::current_exception();
			}
		}

		if (rethrow_e) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			std::rethrow_exception(rethrow_e);
		}

		if (!ret.ok()) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			return ret;
		}

		class amqp_config new_amqp_config;
		irods::error res = new_amqp_config.initialize(_plugin_specific_configuration, _re_instance_name);
		if (!res.ok()) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			return PASS(res);
		}

		// test_mode is optional
		bool new_test_mode_enabled;
		const auto test_mode_cfg = _plugin_specific_configuration.find(KW_TEST_MODE);
		if (test_mode_cfg == _plugin_specific_configuration.end()) {
			new_test_mode_enabled = defaults::test_mode_enabled;
		}
		else if (test_mode_cfg->is_string()) {
			const auto& test_mode_str = test_mode_cfg->get_ref<const std::string&>();
			new_test_mode_enabled = boost::iequals(test_mode_str, "true");
		}
		else {
			new_test_mode_enabled = test_mode_cfg->get<bool>();
		}

		// log_path_prefix is optional
		fs::path new_test_mode_log_path_prefix;
		const auto log_path_prefix_cfg = _plugin_specific_configuration.find(KW_TEST_MODE_LOG_PATH_PREFIX);
		if (log_path_prefix_cfg == _plugin_specific_configuration.end()) {
			new_test_mode_log_path_prefix = fs::temp_directory_path();
		}
		else {
			new_test_mode_log_path_prefix = log_path_prefix_cfg->get_ref<const std::string&>();
		}

		test_mode_enabled_ = new_test_mode_enabled;
		test_mode_log_path_prefix_ = new_test_mode_log_path_prefix;
		amqp_config_ = new_amqp_config;

		is_configured_ = true;
		is_old_config_ = false;

		return SUCCESS();
	}

	irods::error plugin_config::initialize(const nlohmann::json& _plugin_specific_configuration,
	                                       const std::string& _re_instance_name)
	{
		if (!is_configured_) {
			initialize_from_defaults();
		}

		return load_configuration(_plugin_specific_configuration, _re_instance_name);
	}

	irods::error plugin_config::initialize(const std::string& _re_instance_name)
	{
		if (!is_configured_) {
			initialize_from_defaults();
		}

		try {
			const auto rule_engines = irods::get_server_property<nlohmann::json>(
				std::vector<std::string>{irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE});
			for (const auto& rule_engine : rule_engines) {
				const auto& inst_name = rule_engine.at(irods::KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
				if (inst_name != _re_instance_name) {
					continue;
				}

				const auto plugin_spec_cfg = rule_engine.find(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
				if (plugin_spec_cfg == rule_engine.end()) {
					if (is_configured_) {
						is_old_config_ = true;
					}
					return ERROR(KEY_NOT_FOUND, "Failed to find plugin-specific configuration");
				}
				if (!plugin_spec_cfg->is_object()) {
					if (is_configured_) {
						is_old_config_ = true;
					}
					return ERROR(KEY_TYPE_MISMATCH, "Invalid plugin-specific configuration type");
				}

				return initialize(*plugin_spec_cfg, _re_instance_name);
			}
		}
		catch (const std::out_of_range& e) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			return ERROR(KEY_NOT_FOUND, e.what());
		}
		catch (const nlohmann::json::exception& e) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			IRODS_ERROR_ENUM errcode = SYS_LIBRARY_ERROR;
			// For what these values mean, see https://json.nlohmann.me/home/exceptions/
			if (((302 <= e.id) && (e.id <= 312)) || (e.id == 314)) {
				errcode = KEY_TYPE_MISMATCH;
			}
			else if ((e.id == 403) || (e.id == 404)) {
				errcode = KEY_NOT_FOUND;
			}
			else if (e.id == 406) {
				errcode = SYS_CONFIG_FILE_ERR;
			}
			return ERROR(errcode, e.what());
		}
		catch (const std::exception& e) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			return ERROR(SYS_INTERNAL_ERR, e.what());
		}
		catch (...) {
			if (is_configured_) {
				is_old_config_ = true;
			}
			return ERROR(SYS_UNKNOWN_ERROR, "An unknown error occurred");
		}

		if (is_configured_) {
			is_old_config_ = true;
		}
		return ERROR(SYS_CONFIG_FILE_ERR, "Failed to find plugin configuration");
	}
} //namespace irods::plugin::rule_engine::audit_amqp
