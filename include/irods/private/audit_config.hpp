#ifndef IRODS_AUDIT_AUDIT_CONFIG_HPP
#define IRODS_AUDIT_AUDIT_CONFIG_HPP

#include "irods/private/amqp_config.hpp"

#include <irods/irods_error.hpp>

#include <fmt/format.h>
#include <fmt/compile.h>

#include <nlohmann/json.hpp>

#include <optional>
#include <regex>
#include <string>
#include <type_traits>

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
	/// \brief Class for plugin configuration
	///
	/// See README.md for an explanation of each configuration option.
	class plugin_config
	{
	  public:
		enum class failsafe_mode
		{
			BLOCK_OPERATION,
			ALLOW_OPERATION
		};

		/// \brief Class containing configuration defaults and fallbacks.
		///
		/// \note
		/// For required configuration options, the value here is the fallback value.
		class defaults
		{
		  public:
			defaults() = delete;

			static constexpr const enum failsafe_mode failsafe_mode = failsafe_mode::BLOCK_OPERATION;

			static constexpr const auto pep_regex = std::nullopt;
			static constexpr const std::regex::flag_type pep_regex_flags =
				std::regex::ECMAScript | std::regex::optimize;

			static constexpr const bool test_mode_enabled = false;
		};

		[[nodiscard]] irods::error initialize(const nlohmann::json& _plugin_specific_configuration,
		                                      const std::string& _re_instance_name);
		[[nodiscard]] irods::error initialize(const std::string& _re_instance_name);

		void initialize_from_defaults()
		{
			is_configured_ = false;
			is_old_config_ = false;

			failsafe_mode_ = defaults::failsafe_mode;

			pep_regex_ = defaults::pep_regex;
			pep_regex_flags_ = defaults::pep_regex_flags;

			test_mode_enabled_ = defaults::test_mode_enabled;
			test_mode_log_path_prefix_ = fs::temp_directory_path();

			amqp_config_.initialize_from_defaults();
		}

		[[nodiscard]] constexpr bool is_configured() const { return is_configured_; }
		[[nodiscard]] constexpr bool is_old_config() const { return is_old_config_; }

		// NOLINTBEGIN(readability-const-return-type)
		[[nodiscard]] constexpr enum failsafe_mode failsafe_mode() const { return failsafe_mode_; }
		[[nodiscard]] constexpr const std::optional<std::regex>& pep_regex() const { return pep_regex_; }
		[[nodiscard]] constexpr bool test_mode_enabled() const { return test_mode_enabled_; }
		[[nodiscard]] constexpr const fs::path& test_mode_log_path_prefix() const { return test_mode_log_path_prefix_; }
		[[nodiscard]] constexpr const amqp_config& amqp_config() const { return amqp_config_; }
		// NOLINTEND(readability-const-return-type)

		[[nodiscard]] static const plugin_config& default_config()
		{
			if (!default_instance_.has_value()) {
				plugin_config config;
				config.initialize_from_defaults();
				default_instance_ = config;
			}

			return *default_instance_;
		}

		static constexpr const char* const KW_FAILSAFE_MODE = "failsafe_mode";
		static constexpr const char* const KW_PEP_REGEX = "pep_regex_to_match";
		static constexpr const char* const KW_TEST_MODE = "test_mode";
		static constexpr const char* const KW_TEST_MODE_LOG_PATH_PREFIX = "log_path_prefix";

	  private:
		[[nodiscard]] irods::error load_configuration(const nlohmann::json& _plugin_specific_configuration,
		                                              const std::string& _re_instance_name);

		bool is_configured_{false};
		bool is_old_config_{false};

		enum failsafe_mode failsafe_mode_;

		std::regex::flag_type pep_regex_flags_;
		std::optional<std::regex> pep_regex_;

		bool test_mode_enabled_;
		fs::path test_mode_log_path_prefix_;

		class amqp_config amqp_config_;

		static std::optional<plugin_config> default_instance_;
	};
} //namespace irods::plugin::rule_engine::audit_amqp

// NOLINTBEGIN(clazy-function-args-by-value, readability-convert-member-functions-to-static)
template <>
struct fmt::formatter<enum irods::plugin::rule_engine::audit_amqp::plugin_config::failsafe_mode>
	: fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum irods::plugin::rule_engine::audit_amqp::plugin_config::failsafe_mode& _mode,
	                      FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_mode) {
			case irods::plugin::rule_engine::audit_amqp::plugin_config::failsafe_mode::BLOCK_OPERATION:
				valstr = "BLOCK_OPERATION";
				break;
			case irods::plugin::rule_engine::audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION:
				valstr = "ALLOW_OPERATION";
				break;
			default:
				return format_to(
					_ctx.out(),
					FMT_COMPILE("UNKNOWN_{}"),
					static_cast<std::underlying_type_t<
						enum irods::plugin::rule_engine::audit_amqp::plugin_config::failsafe_mode>>(_mode));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};
// NOLINTEND(clazy-function-args-by-value, readability-convert-member-functions-to-static)

#endif // IRODS_AUDIT_AUDIT_CONFIG_HPP
