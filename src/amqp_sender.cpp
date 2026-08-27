#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_config.hpp"
#include "irods/private/amqp_sender.hpp"
#include "irods/private/proton_formatters.hpp"

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_error.hpp>
#include <irods/irods_exception.hpp>
#include <irods/irods_logger.hpp>
#include <irods/irods_server_properties.hpp>
#include <irods/rodsErrorTable.h>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/compile.h>
#if FMT_VERSION >= 90000
#  include <fmt/std.h>
#endif

#include <nlohmann/json.hpp>

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/error.hpp>
#include <proton/error_condition.hpp>
#include <proton/message.hpp>
#include <proton/sender.hpp>
#include <proton/sender_options.hpp>
#include <proton/session.hpp>
#include <proton/session_options.hpp>
#include <proton/timestamp.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>
#include <proton/work_queue.hpp>
#include <proton/uuid.hpp>
#include <proton/value.hpp>

#include <sys/types.h>

#include <chrono>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <semaphore>
#include <string>
#include <thread>
#include <vector>

namespace irods::plugin::rule_engine::audit_amqp
{
	amqp_sender::amqp_sender() : is_open_(false), connection_sem_(0), session_sem_(0), sender_sem_(0) {}

	amqp_sender::~amqp_sender()
	{
		close();
	}

	irods::error amqp_sender::configure(const std::string& _re_instance_name, const amqp_config& _amqp_config)
	{
		if (is_open_) {
			return ERROR(SYS_ALREADY_INITIALIZED, "amqp_sender::configure called on open amqp_sender");
		}
		if (!_amqp_config.is_initialized()) {
			return ERROR(SYS_UNINITIALIZED, "amqp_sender::configure called with uninitialized amqp_config argument");
		}

#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
			{"call", __PRETTY_FUNCTION__},
			{"primary_endpoint", _amqp_config.primary_endpoint()},
		});
		// clang-format on
		std::uint32_t ep_ctr = 0;
		for (const std::string& endpoint : _amqp_config.failover_endpoints()) {
			log_kvs.emplace_back(fmt::format(FMT_COMPILE("failover_endpoint_{0:02d}"), ep_ctr++), endpoint);
		}
		log_kvs.emplace_back("path", _amqp_config.path());
		log_re::trace(log_kvs);
#endif

		re_instance_name_ = _re_instance_name;
		amqp_config_ = _amqp_config;

		return SUCCESS();
	}

	irods::error amqp_sender::unconfigure()
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__}
		});
		// clang-format on
#endif

		if (is_open_) {
			return ERROR(RE_RUNTIME_ERROR, "amqp_sender::unconfigure called on open amqp_sender");
		}

		amqp_config_.deinitialize();

		return SUCCESS();
	}

	irods::error amqp_sender::open()
	{
		if (!amqp_config_.is_initialized()) {
			return ERROR(SYS_UNINITIALIZED, "amqp_sender::open called on unconfigured amqp_sender");
		}

		close();

		// This is after the call to close to avoid the potentially confusing situation of having the log entry for the
		// close call immediately following the log entry for the open call.
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
			{"primary_endpoint", amqp_config_.primary_endpoint()},
		});
		// clang-format on
		std::uint32_t ep_ctr = 0;
		for (const std::string& endpoint : amqp_config_.failover_endpoints()) {
			log_kvs.emplace_back(fmt::format(FMT_COMPILE("failover_endpoint_{0:02d}"), ep_ctr++), endpoint);
		}
		log_kvs.emplace_back("path", amqp_config_.path());
		log_re::trace(log_kvs);
#endif

		proton::connection_options conn_opts;
		conn_opts.handler(*this);
		amqp_config_.configure_connection(conn_opts, re_instance_name_);

		proton::sender_options sender_opts;
		sender_opts.handler(*this);
		amqp_config_.configure_sender(sender_opts);

		const std::string container_id = proton::uuid::random().str();
		container_.emplace(*this, container_id);
		proton::container& container = *container_;

		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Container constructed."},
			{"proton_container::id", container_id},
		});
		// clang-format on

		container.client_connection_options(conn_opts);
		container.sender_options(sender_opts);

		const std::string& re_instance_name = re_instance_name_;
		auto& error_queue = error_queue_;
		const auto run_e = std::make_shared<std::exception_ptr>();
		const std::weak_ptr<std::exception_ptr> run_e_wk = run_e;
		bool did_timeout = false;
		proton_thread_.emplace([&container, &re_instance_name, &error_queue, run_e_wk]() {
			try {
				container.run();
			}
			catch (const std::exception& e) {
				if (const auto run_e = run_e_wk.lock(); run_e) {
					*run_e = std::current_exception();
				}
				error_queue.emplace_back(std::current_exception());
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name},
					{"log_message", "container.run() threw an exception."},
					{"exception", e.what()}
				});
				// clang-format on
			}
		});

		container.connect(amqp_config_.primary_endpoint(), conn_opts);
		if (amqp_config_.connection_open_timeout() > std::chrono::milliseconds::zero()) {
			if (!connection_sem_.try_acquire_for(amqp_config_.connection_open_timeout())) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Reached timeout while establishing AMQP connection."}
				});
				// clang-format on
				did_timeout = true;
			}
		}
		else {
			connection_sem_.acquire();
		}
		if (*run_e) {
			try {
				std::rethrow_exception(*run_e);
			}
			catch (const std::exception& e) {
				// TODO: inspect exception?
				return ERROR(RE_RUNTIME_ERROR,
				             fmt::format("Unhandled exception while establishing AMQP connection: {}", e.what()));
			}
		}
		if (did_timeout) {
			// TODO: inspect error queue
			return ERROR(RE_RUNTIME_ERROR, "Reached timeout while establishing AMQP connection.");
		}

		if (amqp_config_.session_open_timeout() > std::chrono::milliseconds::zero()) {
			if (!session_sem_.try_acquire_for(amqp_config_.session_open_timeout())) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Reached timeout while opening AMQP session."}
				});
				// clang-format on
				did_timeout = true;
				return ERROR(RE_RUNTIME_ERROR, "Reached timeout while opening AMQP session.");
			}
		}
		else {
			session_sem_.acquire();
		}
		if (*run_e) {
			try {
				std::rethrow_exception(*run_e);
			}
			catch (const std::exception& e) {
				// TODO: inspect exception?
				return ERROR(RE_RUNTIME_ERROR,
				             fmt::format("Unhandled exception while opening AMQP session: {}", e.what()));
			}
		}
		if (did_timeout) {
			// TODO: inspect error queue
			return ERROR(RE_RUNTIME_ERROR, "Reached timeout while opening AMQP session.");
		}

		if (amqp_config_.sender_open_timeout() > std::chrono::milliseconds::zero()) {
			if (!sender_sem_.try_acquire_for(amqp_config_.sender_open_timeout())) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Reached timeout while opening AMQP sender."}
				});
				// clang-format on
				did_timeout = true;
				return ERROR(RE_RUNTIME_ERROR, "Reached timeout while opening AMQP sender.");
			}
		}
		else {
			sender_sem_.acquire();
		}
		if (*run_e) {
			try {
				std::rethrow_exception(*run_e);
			}
			catch (const std::exception& e) {
				// TODO: inspect exception?
				return ERROR(RE_RUNTIME_ERROR,
				             fmt::format("Unhandled exception while opening AMQP sender: {}", e.what()));
			}
		}
		if (did_timeout) {
			// TODO: inspect error queue
			return ERROR(RE_RUNTIME_ERROR, "Reached timeout while opening AMQP sender.");
		}

		// TODO: verify open connection and inspect error queue

		return SUCCESS();
	}

	void amqp_sender::close()
	{
		const std::scoped_lock<std::mutex> send_lock(amqp_send_mutex_);

#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
#endif

		if (sender_.has_value()) {
			const auto sender_disconn_sem = std::make_shared<std::binary_semaphore>(0);
			const std::weak_ptr<std::binary_semaphore> sender_disconn_sem_wk = sender_disconn_sem;
			proton::sender& sender = *sender_;
			bool wq_res = sender.work_queue().add([&sender, sender_disconn_sem_wk]() {
				if (!sender.closed()) {
					sender.close();
				}
				if (const auto sender_disconn_sem = sender_disconn_sem_wk.lock(); sender_disconn_sem) {
					sender_disconn_sem->release();
				}
			});
			if (wq_res) {
				if (amqp_config_.sender_close_timeout() > std::chrono::milliseconds::zero()) {
					if (!sender_disconn_sem->try_acquire_for(amqp_config_.sender_close_timeout())) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
							{"log_message", "Reached timeout while closing AMQP sender."}
						});
						// clang-format on
					}
				}
				else {
					sender_disconn_sem->acquire();
				}
#ifdef IRODS_AUDIT_EXTRA_TRACE
				// clang-format off
				log_re::trace({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "AMQP sender closed"},
				});
				// clang-format on
#endif
			}
			else {
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Could not queue AMQP sender close call"},
				});
				// clang-format on
			}

			const auto session_disconn_sem = std::make_shared<std::binary_semaphore>(0);
			const std::weak_ptr<std::binary_semaphore> session_disconn_sem_wk = session_disconn_sem;
			proton::session session = sender.session();
			wq_res = session.work_queue().add([&session, session_disconn_sem_wk]() {
				if (!session.closed()) {
					session.close();
				}
				if (const auto session_disconn_sem = session_disconn_sem_wk.lock(); session_disconn_sem) {
					session_disconn_sem->release();
				}
			});
			if (wq_res) {
				if (amqp_config_.session_close_timeout() > std::chrono::milliseconds::zero()) {
					if (!session_disconn_sem->try_acquire_for(amqp_config_.session_close_timeout())) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
							{"log_message", "Reached timeout while closing AMQP session."}
						});
						// clang-format on
					}
				}
				else {
					session_disconn_sem->acquire();
				}
#ifdef IRODS_AUDIT_EXTRA_TRACE
				// clang-format off
				log_re::trace({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "AMQP session closed"},
				});
				// clang-format on
#endif
			}
			else {
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Could not queue AMQP session close call"},
				});
				// clang-format on
			}
		}

		is_open_ = false;

		bool did_close_conn = false;
		if (connection_.has_value()) {
			const auto connection_disconn_sem = std::make_shared<std::binary_semaphore>(0);
			const std::weak_ptr<std::binary_semaphore> connection_disconn_sem_wk = connection_disconn_sem;
			proton::connection& connection = *connection_;
			const bool wq_res = connection.work_queue().add([&connection, connection_disconn_sem_wk]() {
				if (!connection.closed()) {
					connection.close();
				}
				if (const auto connection_disconn_sem = connection_disconn_sem_wk.lock(); connection_disconn_sem) {
					connection_disconn_sem->release();
				}
			});
			if (wq_res) {
				if (amqp_config_.connection_close_timeout() > std::chrono::milliseconds::zero()) {
					if (!connection_disconn_sem->try_acquire_for(amqp_config_.connection_close_timeout())) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
							{"log_message", "Reached timeout while closing AMQP connection."}
						});
						// clang-format on
					}
				}
				else {
					connection_disconn_sem->acquire();
				}
				did_close_conn = true;
#ifdef IRODS_AUDIT_EXTRA_TRACE
				// clang-format off
				log_re::trace({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "AMQP connection closed"},
				});
#endif
				// clang-format on
			}
			else {
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Could not queue AMQP connection close call"},
				});
				// clang-format on
			}
		}

		if (container_.has_value() && !did_close_conn) {
			container_->stop();
#ifdef IRODS_AUDIT_EXTRA_TRACE
			// clang-format off
			log_re::trace({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
				{"log_message", "AMQP container stopped"},
			});
#endif
			// clang-format on
		}

		if (proton_thread_.has_value()) {
			if (proton_thread_->joinable()) {
				proton_thread_->join();
#ifdef IRODS_AUDIT_EXTRA_TRACE
				// clang-format off
				log_re::trace({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Proton thread joined"},
				});
#endif
				// clang-format on
			}
			else {
				// clang-format off
				log_re::debug({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Proton thread not joinable"},
				});
				// clang-format on
			}
		}

		sender_.reset();
		connection_.reset();
		container_.reset();
		proton_thread_.reset();
	}

	irods::error amqp_sender::send_message(nlohmann::json& _message_body,
	                                       const std::uint64_t _timestamp_ms,
	                                       const pid_t _pid,
	                                       std::ofstream& _test_log_ofstream)
	{
		const std::scoped_lock<std::mutex> send_lock(amqp_send_mutex_);

		irods::error ret = SUCCESS();

		if (!(is_open_ && sender_.has_value())) {
			THROW(SYS_UNINITIALIZED, "send_message called on closed amqp_sender");
		}

#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
#endif

		_message_body["@timestamp"] = _timestamp_ms;
		_message_body["hostname"] = irods::get_server_property<std::string>(irods::KW_CFG_HOST);
		_message_body["pid"] = _pid;

		const std::string msg_str = _message_body.dump();

		const auto msg = std::make_shared<proton::message>(msg_str);
		msg->content_type("application/json");
		msg->creation_time(proton::timestamp(static_cast<proton::timestamp::numeric_type>(_timestamp_ms)));
		if (amqp_config_.durable_messages().has_value()) {
			msg->durable(*amqp_config_.durable_messages());
		}

		const auto send_sem = std::make_shared<std::binary_semaphore>(0);
		const std::weak_ptr<std::binary_semaphore> send_sem_wk = send_sem;
		proton::sender& sender = *sender_;
		const std::string& re_instance_name = re_instance_name_;
		auto& error_queue = error_queue_;
		const auto send_e = std::make_shared<std::exception_ptr>();
		const std::weak_ptr<std::exception_ptr> send_e_wk = send_e;
		bool did_timeout = false;
		const bool is_sending = sender.work_queue().add([&sender, msg, &re_instance_name, &error_queue, send_sem_wk, send_e_wk]() {
			try {
				const proton::tracker t = sender.send(*msg);
#ifdef IRODS_AUDIT_EXTRA_TRACE
				// clang-format off
				log_list log_kvs({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name},
					{"log_message", "Returned from sender.send()."},
				});
				// clang-format on
				dump_proton_object(log_kvs, t);
				log_re::debug(log_kvs);
#endif
			}
			catch (const std::exception& e) {
				if (const auto send_e = send_e_wk.lock(); send_e) {
					*send_e = std::current_exception();
				}
				error_queue.emplace_back(std::current_exception());
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name},
					{"log_message", "Exception thrown while sending AMQP message."},
					{"exception", e.what()}
				});
				// clang-format on
			}
			if (const auto send_sem = send_sem_wk.lock(); send_sem) {
				send_sem->release();
			}
		});
		if (is_sending) {
			if (amqp_config_.message_send_timeout() > std::chrono::milliseconds::zero()) {
				if (!send_sem->try_acquire_for(amqp_config_.message_send_timeout())) {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
						{"log_message", "Reached timeout while sending AMQP message."}
					});
					// clang-format on
					did_timeout = true;
				}
			}
			else {
				send_sem->acquire();
			}
			if (*send_e) {
				try {
					std::rethrow_exception(*send_e);
				}
				catch (const std::exception& e) {
					// TODO: inspect exception?
					ret = ERROR(RE_RUNTIME_ERROR,
					             fmt::format("Unhandled exception while sending AMQP message: {}", e.what()));
				}
			}
			else if (did_timeout) {
				// TODO: inspect error queue
				ret = ERROR(RE_RUNTIME_ERROR, "Reached timeout while sending AMQP message.");
			}
			// TODO: verify message sent (inspect tracker?) and inspect error queue
		}
		else {
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
				{"log_message", "Could not add message to work queue."}
			});
			// clang-format on
		}

		if (_test_log_ofstream.is_open()) {
#ifdef IRODS_AUDIT_EXTRA_TRACE
			// clang-format off
			log_re::trace({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
				{"log_message", "Writing amqp message to test log."}
			});
			// clang-format on
#endif
			_test_log_ofstream << msg_str << '\n' << std::flush;
			if (!_test_log_ofstream.good()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
					{"log_message", "Error while writing to test log."}
				});
				// clang-format on
			}
		}

		return ret;
	}

	// TODO: add container start (and stop?) timeouts
	// TODO: continue breaking up event handler into open
	void amqp_sender::on_container_start([[maybe_unused]] proton::container& _container)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
			{"proton_container::id", _container.id()},
		});
		// clang-format on
#endif
	}

	void amqp_sender::on_connection_open([[maybe_unused]] proton::connection& _connection)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _connection);
		log_re::trace(log_kvs);
#endif
		connection_ = _connection;
		connection_sem_.release();

		auto session = connection_->open_session(proton::session_options().handler(*this));
		session_sem_.release();

		proton::sender_options sender_opts;
		sender_opts.handler(*this);
		amqp_config_.configure_sender(sender_opts);

		sender_ = session.open_sender(amqp_config_.path(), sender_opts);
		is_open_ = true;
		sender_sem_.release();
	}

	void amqp_sender::on_session_open([[maybe_unused]] proton::session& _session)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _session);
		log_re::trace(log_kvs);
#endif
	}

	void amqp_sender::on_sender_open([[maybe_unused]] proton::sender& _sender)
	{
#ifdef IRODS_AUDIT_EXTRA_TRACE
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _sender);
		log_re::trace(log_kvs);
#endif
	}

	void amqp_sender::on_tracker_reject([[maybe_unused]] proton::tracker& _tracker)
	{
		error_queue_.emplace_back(_tracker);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "AMQP server unexpectedly rejected message"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _tracker);
		log_re::error(log_kvs);
	}

	void amqp_sender::on_transport_error(proton::transport& _transport)
	{
		error_queue_.emplace_back(_transport);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Transport error in proton messaging handler"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _transport);
		log_re::error(log_kvs);
	}

	void amqp_sender::on_connection_error(proton::connection& _connection)
	{
		error_queue_.emplace_back(_connection);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Connection error in proton messaging handler"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _connection);
		log_re::error(log_kvs);
	}

	void amqp_sender::on_session_error(proton::session& _session)
	{
		error_queue_.emplace_back(_session);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Session error in proton messaging handler"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _session);
		log_re::error(log_kvs);
	}

	void amqp_sender::on_sender_error(proton::sender& _sender)
	{
		error_queue_.emplace_back(_sender);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Sender error in proton messaging handler"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _sender);
		log_re::error(log_kvs);
	}

	void amqp_sender::on_error(const proton::error_condition& _err_cond)
	{
		error_queue_.emplace_back(_err_cond);
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"log_message", "Unknown error in proton messaging handler"},
		});
		// clang-format on
		dump_proton_object(log_kvs, _err_cond);
		log_re::error(log_kvs);
	}

#ifdef IRODS_AUDIT_EXTRA_TRACE
	void amqp_sender::on_container_stop([[maybe_unused]] proton::container& _container)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
			{"proton_container::id", _container.id()},
		});
		// clang-format on
	}

	void amqp_sender::on_sendable([[maybe_unused]] proton::sender& _sender)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
	}

	void amqp_sender::on_transport_open([[maybe_unused]] proton::transport& _transport)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _transport);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_transport_close([[maybe_unused]] proton::transport& _transport)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _transport);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_connection_close([[maybe_unused]] proton::connection& _connection)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _connection);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_session_close([[maybe_unused]] proton::session& _session)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _session);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_sender_detach([[maybe_unused]] proton::sender& _sender)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _sender);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_sender_close([[maybe_unused]] proton::sender& _sender)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _sender);
		log_re::trace(log_kvs);
	}

	void amqp_sender::on_tracker_accept([[maybe_unused]] proton::tracker& _tracker)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
	}

	void amqp_sender::on_tracker_release([[maybe_unused]] proton::tracker& _tracker)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
	}

	void amqp_sender::on_tracker_settle([[maybe_unused]] proton::tracker& _tracker)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
	}

	void amqp_sender::on_sender_drain_start([[maybe_unused]] proton::sender& _sender)
	{
		// clang-format off
		log_re::trace({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
	}

	void amqp_sender::on_connection_wake([[maybe_unused]] proton::connection& _connection)
	{
		// clang-format off
		log_list log_kvs({
			{"rule_engine_plugin", rule_engine_name},
			{irods::KW_CFG_INSTANCE_NAME, re_instance_name_},
			{"call", __PRETTY_FUNCTION__},
		});
		// clang-format on
		dump_proton_object(log_kvs, _connection);
		log_re::trace(log_kvs);
	}
#endif // IRODS_AUDIT_EXTRA_TRACE
} //namespace irods::plugin::rule_engine::audit_amqp
