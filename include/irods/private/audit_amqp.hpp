#ifndef IRODS_AUDIT_AMQP_MAIN_HPP
#define IRODS_AUDIT_AMQP_MAIN_HPP

#include <irods/irods_logger.hpp>

#include <chrono>
#include <string>

#include <boost/config.hpp>

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

	template <class T>
	static BOOST_FORCEINLINE void log_exception(
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
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_MAIN_HPP
