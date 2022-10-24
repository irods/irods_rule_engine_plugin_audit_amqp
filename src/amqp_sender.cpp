#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_sender.hpp"

#include <boost/config.hpp>

namespace irods::plugin::rule_engine::audit_amqp
{
	namespace
	{
		BOOST_FORCEINLINE void log_proton_error(const proton::error_condition& err_cond, const std::string& log_message)
		{
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{"log_message", log_message},
				{"error_condition::name", err_cond.name()},
				{"error_condition::description", err_cond.description()},
				{"error_condition::what", err_cond.what()}
			});
			// clang-format on
		}
	} // namespace

	send_handler::send_handler(const proton::message& _message, const std::string& _url)
		: _amqp_url(_url)
		, _message(_message)
		, _message_sent(false)
	{
	}

	void send_handler::on_container_start(proton::container& _container)
	{
		proton::connection_options conn_opts;
		_container.open_sender(_amqp_url, conn_opts);
	}

	void send_handler::on_sendable(proton::sender& _sender)
	{
		if (_sender.credit() && !_message_sent) {
			_sender.send(_message);
			_message_sent = true;
		}
	}

	void send_handler::on_tracker_accept(proton::tracker& _tracker)
	{
		// we're only sending one message
		// so we don't care about the credit system
		// or tracking confirmed messages
		if (_message_sent) {
			_tracker.connection().close();
		}
	}

	void send_handler::on_tracker_reject([[maybe_unused]] proton::tracker& _tracker)
	{
		// clang-format off
		log_re::error({
			{"rule_engine_plugin", rule_engine_name},
			{"log_message", "AMQP server unexpectedly rejected message"}
		});
		// clang-format on
	}

	void send_handler::on_transport_error(proton::transport& _transport)
	{
		log_proton_error(_transport.error(), "Transport error in proton messaging handler");
	}

	void send_handler::on_connection_error(proton::connection& _connection)
	{
		log_proton_error(_connection.error(), "Connection error in proton messaging handler");
	}

	void send_handler::on_session_error(proton::session& _session)
	{
		log_proton_error(_session.error(), "Session error in proton messaging handler");
	}

	void send_handler::on_receiver_error(proton::receiver& _receiver)
	{
		log_proton_error(_receiver.error(), "Receiver error in proton messaging handler");
	}

	void send_handler::on_sender_error(proton::sender& _sender)
	{
		log_proton_error(_sender.error(), "Sender error in proton messaging handler");
	}

	void send_handler::on_error(const proton::error_condition& _err_cond)
	{
		log_proton_error(_err_cond, "Unknown error in proton messaging handler");
	}
} //namespace irods::plugin::rule_engine::audit_amqp
