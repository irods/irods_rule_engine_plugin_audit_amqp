#ifndef IRODS_AUDIT_AMQP_PROTON_ERROR_EVENT_HPP
#define IRODS_AUDIT_AMQP_PROTON_ERROR_EVENT_HPP

#include "irods/private/proton_formatters.hpp"
#include "irods/private/proton_object_type.hpp"

#include <proton/binary.hpp>
#include <proton/connection.hpp>
#include <proton/sender.hpp>
#include <proton/session.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>

#include <chrono>
#include <variant>

namespace irods::plugin::rule_engine::audit_amqp
{
	class proton_error_event
	{
	  public:
		using clock = std::chrono::steady_clock;
		using time_point = clock::time_point;
		using event_source_variant = std::variant<proton::error_condition,
		                                          proton::tracker,
		                                          proton::transport,
		                                          proton::connection,
		                                          proton::session,
		                                          proton::sender>;

		proton_error_event(const time_point& _event_time_point,
		                   const proton_object_type _event_source_type,
		                   const event_source_variant& _event_source)
			: event_time_point_(_event_time_point)
			, event_source_type_(_event_source_type)
			, event_source_(_event_source)
		{}

		proton_error_event(const proton_object_type _event_source_type,
		                   const event_source_variant& _event_source)
			: proton_error_event(clock::now(), _event_source_type, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::error_condition& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::UNKNOWN, _event_source)
		{}

		proton_error_event(const proton::error_condition& _event_source)
			: proton_error_event(proton_object_type::UNKNOWN, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::tracker& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::TRACKER, _event_source)
		{}

		proton_error_event(const proton::tracker& _event_source)
			: proton_error_event(proton_object_type::TRACKER, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::transport& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::TRANSPORT, _event_source)
		{}

		proton_error_event(const proton::transport& _event_source)
			: proton_error_event(proton_object_type::TRANSPORT, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::connection& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::CONNECTION, _event_source)
		{}

		proton_error_event(const proton::connection& _event_source)
			: proton_error_event(proton_object_type::CONNECTION, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::session& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::SESSION, _event_source)
		{}

		proton_error_event(const proton::session& _event_source)
			: proton_error_event(proton_object_type::SESSION, _event_source)
		{}

		proton_error_event(const time_point& _event_time_point,
		                   const proton::sender& _event_source)
			: proton_error_event(_event_time_point, proton_object_type::SENDER, _event_source)
		{}

		proton_error_event(const proton::sender& _event_source)
			: proton_error_event(proton_object_type::SENDER, _event_source)
		{}

		[[nodiscard]] const constexpr time_point& event_time_point() const { return event_time_point_; }
		[[nodiscard]] const constexpr proton_object_type& event_source_type() const { return event_source_type_; }
		[[nodiscard]] const constexpr event_source_variant& event_source() const { return event_source_; }

	  private:
		time_point event_time_point_;
		proton_object_type event_source_type_;
		event_source_variant event_source_;
	};
}

#endif // IRODS_AUDIT_AMQP_PROTON_ERROR_EVENT_HPP
