#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_sender.hpp"

#include <boost/config.hpp>
#include <fmt/core.h>
#include <fmt/compile.h>

namespace irods::plugin::rule_engine::audit_amqp
{
	namespace
	{
		BOOST_FORCEINLINE std::string build_amqp_url(
			const std::string_view& _scheme,
			const std::string_view& _host,
			const std::uint_fast16_t& _port,
			const std::string_view& _topic)
		{
			// TODO: use Boost.URL (if available at compile-time) to validate URLs
			return fmt::format(FMT_COMPILE("{0:s}://{1:s}:{2:d}/{3:s}"), _scheme, _host, _port, _topic);
		}

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

	amqp_endpoint::amqp_endpoint(
		const std::string_view& _scheme,
		const std::string_view& _host,
		const std::uint_fast16_t& _port,
		const std::string_view& _topic,
		const std::string_view& _user,
		const std::string_view& _password)
		: url(build_amqp_url(_scheme, _host, _port, _topic))
		, user(_user)
		, password(_password)
	{
	}

	amqp_sender::amqp_sender(const std::vector<amqp_endpoint>& _endpoints)
		: _endpoints(_endpoints), _endpoint_idx(static_cast<std::vector<amqp_endpoint>::size_type>(0))
	{
	}

	amqp_sender::~amqp_sender()
	{
		if (_log_file_ofstream.is_open()) {
			_log_file_ofstream.close();
		}
	}

	void amqp_sender::enable_test_mode(const fs::path _test_mode_log_path)
	{
		if (_log_file_ofstream.is_open()) {
			_log_file_ofstream.close();
		}
		_log_file_path.assign(_test_mode_log_path);
	}

	void amqp_sender::disable_test_mode()
	{
		_log_file_path.clear();
		if (_log_file_ofstream.is_open()) {
			_log_file_ofstream.close();
		}
	}

	void amqp_sender::send_message(const nlohmann::json& _message_body, const std::uint64_t _timestamp_ms)
	{
		const std::string msg_str = _message_body.dump();

		proton::message msg(msg_str);
		msg.content_type("application/json");
		msg.creation_time(proton::timestamp(static_cast<proton::timestamp::numeric_type>(_timestamp_ms)));
		send_handler handler(msg, _endpoints);
		proton::container(handler).run();

		if (!_log_file_path.empty()) {
			if (!_log_file_ofstream.is_open()) {
				_log_file_ofstream.open(_log_file_path);
			}
			_log_file_ofstream << msg_str << std::endl;
		}
	}

	send_handler::send_handler(const proton::message& _message, const std::vector<amqp_endpoint>& _endpoints)
		: _endpoints(_endpoints)
		, _endpoint_idx(static_cast<std::vector<amqp_endpoint>::size_type>(0))
		, _message(_message)
		, _message_sent(false)
	{
	}

	void send_handler::on_container_start(proton::container& _container)
	{
		const amqp_endpoint& endpoint = _endpoints[_endpoint_idx];
		proton::connection_options conn_opts;
		if (!endpoint.user.empty()) {
			conn_opts.user(endpoint.user);
		}
		if (!endpoint.password.empty()) {
			conn_opts.password(endpoint.password);
		}
		_container.open_sender(endpoint.url, conn_opts);
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
