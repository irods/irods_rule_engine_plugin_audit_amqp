#ifndef IRODS_AUDIT_AMQP_SENDER_HPP
#define IRODS_AUDIT_AMQP_SENDER_HPP

#include "irods/private/audit_amqp.hpp"

#include <string>

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

namespace irods::plugin::rule_engine::audit_amqp
{
	class send_handler : public proton::messaging_handler
	{
	  public:
		send_handler(const proton::message& _message, const std::string& _url);
		void on_container_start(proton::container& _container) override;
		void on_sendable(proton::sender& _sender) override;
		void on_tracker_accept(proton::tracker& _tracker) override;
		void on_tracker_reject(proton::tracker& _tracker) override;
		void on_transport_error(proton::transport& _transport) override;
		void on_connection_error(proton::connection& _connection) override;
		void on_session_error(proton::session& _session) override;
		void on_receiver_error(proton::receiver& _receiver) override;
		void on_sender_error(proton::sender& _sender) override;
		void on_error(const proton::error_condition& _err_cond) override;

	  private:
		const std::string& _amqp_url;
		const proton::message& _message;
		bool _message_sent;
	};
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_SENDER_HPP
