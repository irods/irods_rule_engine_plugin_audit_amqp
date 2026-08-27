#ifndef IRODS_AUDIT_AMQP_SENDER_HPP
#define IRODS_AUDIT_AMQP_SENDER_HPP

#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_config.hpp"
#include "irods/private/proton_error_event.hpp"

#include <irods/irods_error.hpp>

#include <nlohmann/json.hpp>

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/error_condition.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/sender.hpp>
#include <proton/session.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>

#include <sys/types.h>

#include <cstdint>
#include <fstream>
#include <mutex>
#include <optional>
#include <semaphore>
#include <string>
#include <thread>
#include <vector>

namespace irods::plugin::rule_engine::audit_amqp
{
	class amqp_sender : public proton::messaging_handler
	{
	  public:
		amqp_sender();
		~amqp_sender() override;

		[[nodiscard]] irods::error configure(const std::string& _re_instance_name, const amqp_config& _amqp_config);
		[[nodiscard]] irods::error unconfigure();

		[[nodiscard]] irods::error open();
		void close();

		[[nodiscard]] constexpr bool is_configured() const { return amqp_config_.is_initialized(); }
		[[nodiscard]] constexpr bool is_open() const { return is_open_; }
		[[nodiscard]] constexpr const std::string& re_instance_name() const { return re_instance_name_; }

		[[nodiscard]] irods::error send_message(nlohmann::json& _message_body,
		                                        const std::uint64_t _timestamp_ms,
		                                        const pid_t _pid,
		                                        std::ofstream& _test_log_ofstream);

		void on_container_start(proton::container& _container) override;
		void on_connection_open(proton::connection& _connection) override;
		void on_session_open(proton::session& _session) override;
		void on_sender_open(proton::sender& _sender) override;
		void on_transport_error(proton::transport& _transport) override;
		void on_connection_error(proton::connection& _connection) override;
		void on_session_error(proton::session& _session) override;
		void on_sender_error(proton::sender& _sender) override;
		void on_tracker_reject(proton::tracker& _tracker) override;
		void on_error(const proton::error_condition& _err_cond) override;

#ifdef IRODS_AUDIT_EXTRA_TRACE
		void on_container_stop(proton::container& _container) override;
		void on_sendable(proton::sender& _sender) override;
		void on_transport_open(proton::transport& _transport) override;
		void on_transport_close(proton::transport& _transport) override;
		void on_connection_close(proton::connection& _connection) override;
		void on_session_close(proton::session& _session) override;
		void on_sender_detach(proton::sender& _sender) override;
		void on_sender_close(proton::sender& _sender) override;
		void on_tracker_accept(proton::tracker& _tracker) override;
		void on_tracker_release(proton::tracker& _tracker) override;
		void on_tracker_settle(proton::tracker& _tracker) override;
		void on_sender_drain_start(proton::sender& _sender) override;
		void on_connection_wake(proton::connection& _connection) override;
#endif

	  private:
		bool is_open_;

		std::string re_instance_name_;

		amqp_config amqp_config_;

		std::optional<std::thread> proton_thread_;
		std::mutex amqp_send_mutex_;
		std::binary_semaphore connection_sem_;
		std::binary_semaphore session_sem_;
		std::binary_semaphore sender_sem_;

		std::optional<proton::container> container_;
		std::optional<proton::connection> connection_;
		std::optional<proton::sender> sender_;

		std::vector<proton_error_event> error_queue_;
	};
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_SENDER_HPP
