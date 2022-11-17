#ifndef IRODS_AUDIT_AMQP_SENDER_HPP
#define IRODS_AUDIT_AMQP_SENDER_HPP

#include "irods/private/audit_amqp.hpp"

#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>

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

#include <fmt/core.h>
#include <fmt/compile.h>
#include <nlohmann/json.hpp>

// filesystem
// clang-format off
#ifdef __cpp_lib_filesystem
#include <filesystem>
namespace fs = std::filesystem;
#else
#include <boost/filesystem.hpp>
namespace fs = boost::filesystem;
#endif
// clang-format on

namespace irods::plugin::rule_engine::audit_amqp
{
	class amqp_endpoint
	{
	  public:
		amqp_endpoint(
			const std::string_view& _scheme,
			const std::string_view& _host,
			const std::uint_fast16_t& _port,
			const std::string_view& _topic,
			const std::string_view& _user,
			const std::string_view& _password);

		const std::string url;
		const std::string user;
		const std::string password;

		static constexpr const std::string_view default_scheme{"amqp"};
		static constexpr const std::uint_fast16_t default_amqp_port = 5672;
		static constexpr const std::uint_fast16_t default_amqps_port = 5671;
		static constexpr const std::string_view default_user{""};
		static constexpr const std::string_view default_password{""};
	};

	class send_handler : public proton::messaging_handler
	{
	  public:
		send_handler(const proton::message& _message, const std::vector<amqp_endpoint>& _endpoints);
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
		const std::vector<amqp_endpoint>& _endpoints;
		std::vector<amqp_endpoint>::size_type _endpoint_idx;
		const proton::message& _message;
		bool _message_sent;
	};

	class amqp_sender
	{
	  public:
		amqp_sender(const std::vector<amqp_endpoint>& _endpoints);
		~amqp_sender();
		void enable_test_mode(const fs::path _test_mode_log_path);
		void disable_test_mode();
		void send_message(const nlohmann::json& _message_body, const std::uint64_t _timestamp_ms);
	  private:
		const std::vector<amqp_endpoint>& _endpoints;
		std::vector<amqp_endpoint>::size_type _endpoint_idx;
		fs::path _log_file_path;
		std::ofstream _log_file_ofstream;
	};
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_SENDER_HPP
