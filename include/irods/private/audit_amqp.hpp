#ifndef IRODS_AUDIT_AMQP_MAIN_HPP
#define IRODS_AUDIT_AMQP_MAIN_HPP

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_logger.hpp>

#include <chrono>
#include <string>

namespace irods::plugin::rule_engine::audit_amqp
{
	static inline constexpr const char* const rule_engine_name = "audit_amqp";

	using log_re = irods::experimental::log::rule_engine;

#if __cpp_lib_chrono >= 201907
	// we use millisecond precision in our timestamps, so we want to use a clock
	// that does not implement leap seconds as repeated non-leap seconds, if we can.
	using ts_clock = std::chrono::utc_clock;
#else
	// fallback to system_clock
	using ts_clock = std::chrono::system_clock;
#endif

	template <class Logger>
	void log_exception(const Logger& _logger,
	                   const std::string& _log_message,
	                   const std::string& _e_what,
	                   const std::string& _instance_name,
	                   const std::string& _rule_name)
	{
		// clang-format off
		_logger({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"rule_name", _rule_name},
			{"log_message", _log_message},
			{"exception", _e_what},
		});
		// clang-format on
	}

	template <class Logger>
	void log_exception(const Logger& _logger,
	                   const std::string& _log_message,
	                   const std::string& _e_what,
	                   const std::string& _instance_name)
	{
		// clang-format off
		_logger({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _instance_name},
			{"log_message", _log_message},
			{"exception", _e_what},
		});
		// clang-format on
	}
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_MAIN_HPP
