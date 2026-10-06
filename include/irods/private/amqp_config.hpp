#ifndef IRODS_AUDIT_AMQP_CONFIG_HPP
#define IRODS_AUDIT_AMQP_CONFIG_HPP

#include "irods/private/audit_amqp.hpp"

#include <irods/irods_error.hpp>

#include <nlohmann/json.hpp>

#include <proton/connection_options.hpp>
#include <proton/delivery_mode.hpp>
#include <proton/duration.hpp>
#include <proton/sender_options.hpp>
#include <proton/ssl.hpp>
#include <proton/source.hpp>
#include <proton/target.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace irods::plugin::rule_engine::audit_amqp
{
	/// \brief Class for AMQP configuration
	///
	/// See README.md for an explanation of each configuration option.
	///
	/// \note
	/// A value of ``std::nullopt`` indicates an unset option. In these cases, the behavior is defined by Qpid Proton.
	class amqp_config
	{
	  public:
		/// \brief Class containing configuration defaults and fallbacks.
		///
		/// \note
		/// For required configuration options, the value here is the fallback value. Fallback values will only be used
		/// if something really terrible happens.
		class defaults
		{
		  public:
			defaults() = delete;

			static constexpr const std::string_view primary_endpoint{"amqp://localhost:5672"};

			static constexpr const std::string_view path{"/queues/irods_audit_messages"};

			static constexpr const auto user = std::nullopt;
			static constexpr const auto password = std::nullopt;

			static constexpr const std::optional<std::uint32_t> connection_max_frame_size = std::nullopt;
			static constexpr const std::optional<std::uint16_t> connection_max_sessions = std::nullopt;
			static constexpr const auto connection_idle_timeout = std::nullopt;
			static constexpr const auto connection_virtual_host = std::nullopt;
			static constexpr const std::chrono::milliseconds connection_open_timeout{3000};
			static constexpr const std::chrono::milliseconds connection_close_timeout{3000};
			static constexpr const auto reconnect_delay = std::nullopt;
			static constexpr const std::optional<float> reconnect_delay_multiplier = std::nullopt;
			static constexpr const auto reconnect_max_delay = std::nullopt;
			static constexpr const std::optional<int> reconnect_max_attempts = std::nullopt;

			static constexpr const std::optional<enum proton::ssl::verify_mode> ssl_verify_mode = std::nullopt;
			static constexpr const auto ssl_trust_db = std::nullopt;
			static constexpr const auto ssl_certdb_main = std::nullopt;
			static constexpr const auto ssl_certdb_extra = std::nullopt;
			static constexpr const auto ssl_cert_password = std::nullopt;

			static constexpr const std::optional<bool> sasl_enabled = std::nullopt;
			static constexpr const auto sasl_mechanisms = std::nullopt;
			static constexpr const std::optional<bool> sasl_allow_insecure = std::nullopt;

			static constexpr const std::optional<enum proton::delivery_mode::modes> sender_delivery_mode = std::nullopt;
			static constexpr const std::optional<bool> sender_auto_settle = std::nullopt;
			static constexpr const std::chrono::milliseconds sender_open_timeout{3000};
			static constexpr const std::chrono::milliseconds sender_close_timeout{3000};

			static constexpr const auto sender_source_address = std::nullopt;
			static constexpr const std::optional<bool> sender_source_dynamic = std::nullopt;
			static constexpr const std::optional<bool> sender_source_anonymous = std::nullopt;
			static constexpr const std::optional<enum proton::source::distribution_mode> sender_source_distribution_mode = std::nullopt;
			static constexpr const std::optional<enum proton::source::durability_mode> sender_source_durability_mode = std::nullopt;
			static constexpr const auto sender_source_timeout = std::nullopt;
			static constexpr const std::optional<enum proton::source::expiry_policy> sender_source_expiry_policy = std::nullopt;

			static constexpr const auto sender_target_address = std::nullopt;
			static constexpr const std::optional<bool> sender_target_dynamic = std::nullopt;
			static constexpr const std::optional<bool> sender_target_anonymous = std::nullopt;
			static constexpr const std::optional<enum proton::target::durability_mode> sender_target_durability_mode =
				proton::target::durability_mode::UNSETTLED_STATE;
			static constexpr const auto sender_target_timeout = std::nullopt;
			static constexpr const std::optional<enum proton::target::expiry_policy> sender_target_expiry_policy = std::nullopt;

			static constexpr const std::optional<bool> durable_messages = true;
			static constexpr const std::chrono::milliseconds message_send_timeout{5000};

			static constexpr const std::chrono::milliseconds session_close_timeout{3000};
			static constexpr const std::chrono::milliseconds session_open_timeout{3000};
		};

		[[nodiscard]] irods::error initialize(const nlohmann::json& _plugin_specific_configuration,
		                                      const std::string& _re_instance_name);

		void configure_connection(proton::connection_options& _conn_opts, const std::string& _re_instance_name);
		void configure_sender(proton::sender_options& _sender_opts);

		void deinitialize()
		{
			is_initialized_ = false;

			primary_endpoint_.clear();
			failover_endpoints_.clear();

			path_.clear();

			user_ = std::nullopt;
			password_ = std::nullopt;

			connection_max_frame_size_ = std::nullopt;
			connection_max_sessions_ = std::nullopt;
			connection_idle_timeout_ = std::nullopt;
			connection_virtual_host_ = std::nullopt;
			connection_open_timeout_ = std::chrono::milliseconds::zero();
			connection_close_timeout_ = std::chrono::milliseconds::zero();
			reconnect_delay_ = std::nullopt;
			reconnect_delay_multiplier_ = std::nullopt;
			reconnect_max_delay_ = std::nullopt;
			reconnect_max_attempts_ = std::nullopt;

			ssl_verify_mode_ = std::nullopt;
			ssl_trust_db_ = std::nullopt;
			ssl_certdb_main_ = std::nullopt;
			ssl_certdb_extra_ = std::nullopt;
			ssl_cert_password_ = std::nullopt;

			sasl_enabled_ = std::nullopt;
			sasl_mechanisms_ = std::nullopt;
			sasl_allow_insecure_ = std::nullopt;

			sender_delivery_mode_ = std::nullopt;
			sender_auto_settle_ = std::nullopt;
			sender_open_timeout_ = std::chrono::milliseconds::zero();
			sender_close_timeout_ = std::chrono::milliseconds::zero();

			sender_source_address_ = std::nullopt;
			sender_source_dynamic_ = std::nullopt;
			sender_source_anonymous_ = std::nullopt;
			sender_source_distribution_mode_ = std::nullopt;
			sender_source_durability_mode_ = std::nullopt;
			sender_source_timeout_ = std::nullopt;
			sender_source_expiry_policy_ = std::nullopt;

			sender_target_address_ = std::nullopt;
			sender_target_dynamic_ = std::nullopt;
			sender_target_anonymous_ = std::nullopt;
			sender_target_durability_mode_ = std::nullopt;
			sender_target_timeout_ = std::nullopt;
			sender_target_expiry_policy_ = std::nullopt;

			durable_messages_ = std::nullopt;
			message_send_timeout_ = std::chrono::milliseconds::zero();

			session_open_timeout_ = std::chrono::milliseconds::zero();
			session_close_timeout_ = std::chrono::milliseconds::zero();
		}

		void initialize_from_defaults()
		{
			primary_endpoint_ = defaults::primary_endpoint;
			failover_endpoints_.clear();
			failover_endpoints_.shrink_to_fit();

			path_ = defaults::path;

			user_ = defaults::user;
			password_ = defaults::password;

			connection_max_frame_size_ = defaults::connection_max_frame_size;
			connection_max_sessions_ = defaults::connection_max_sessions;
			connection_idle_timeout_ = defaults::connection_idle_timeout;
			connection_virtual_host_ = defaults::connection_virtual_host;
			connection_open_timeout_ = defaults::connection_open_timeout;
			connection_close_timeout_ = defaults::connection_close_timeout;
			reconnect_delay_ = defaults::reconnect_delay;
			reconnect_delay_multiplier_ = defaults::reconnect_delay_multiplier;
			reconnect_max_delay_ = defaults::reconnect_max_delay;
			reconnect_max_attempts_ = defaults::reconnect_max_attempts;

			ssl_verify_mode_ = defaults::ssl_verify_mode;
			ssl_trust_db_ = defaults::ssl_trust_db;
			ssl_certdb_main_ = defaults::ssl_certdb_main;
			ssl_certdb_extra_ = defaults::ssl_certdb_extra;
			ssl_cert_password_ = defaults::ssl_cert_password;

			sasl_enabled_ = defaults::sasl_enabled;
			sasl_mechanisms_ = defaults::sasl_mechanisms;
			sasl_allow_insecure_ = defaults::sasl_allow_insecure;

			sender_delivery_mode_ = defaults::sender_delivery_mode;
			sender_auto_settle_ = defaults::sender_auto_settle;
			sender_open_timeout_ = defaults::sender_open_timeout;
			sender_close_timeout_ = defaults::sender_close_timeout;

			sender_source_address_ = defaults::sender_source_address;
			sender_source_dynamic_ = defaults::sender_source_dynamic;
			sender_source_anonymous_ = defaults::sender_source_anonymous;
			sender_source_distribution_mode_ = defaults::sender_source_distribution_mode;
			sender_source_durability_mode_ = defaults::sender_source_durability_mode;
			sender_source_timeout_ = defaults::sender_source_timeout;
			sender_source_expiry_policy_ = defaults::sender_source_expiry_policy;

			sender_target_address_ = defaults::sender_target_address;
			sender_target_dynamic_ = defaults::sender_target_dynamic;
			sender_target_anonymous_ = defaults::sender_target_anonymous;
			sender_target_durability_mode_ = defaults::sender_target_durability_mode;
			sender_target_timeout_ = defaults::sender_target_timeout;
			sender_target_expiry_policy_ = defaults::sender_target_expiry_policy;

			durable_messages_ = defaults::durable_messages;
			message_send_timeout_ = defaults::message_send_timeout;

			session_open_timeout_ = defaults::session_open_timeout;
			session_close_timeout_ = defaults::session_close_timeout;
		}

		[[nodiscard]] constexpr bool is_initialized() const { return is_initialized_; }

		// NOLINTBEGIN(readability-const-return-type)
		[[nodiscard]] constexpr const std::string& primary_endpoint() const { return primary_endpoint_; }
		[[nodiscard]] constexpr const std::vector<std::string>& failover_endpoints() const { return failover_endpoints_; }
		[[nodiscard]] constexpr const std::string& path() const { return path_; }
		[[nodiscard]] constexpr const std::optional<std::string>& user() const { return user_; }
		[[nodiscard]] constexpr const std::optional<std::string>& password() const { return password_; }
		[[nodiscard]] constexpr const std::optional<std::uint32_t>& connection_max_frame_size() const { return connection_max_frame_size_; }
		[[nodiscard]] constexpr const std::optional<std::uint16_t>& connection_max_sessions() const { return connection_max_sessions_; }
		[[nodiscard]] constexpr const std::optional<proton::duration>& connection_idle_timeout() const { return connection_idle_timeout_; }
		[[nodiscard]] constexpr const std::optional<std::string>& connection_virtual_host() const { return connection_virtual_host_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds connection_open_timeout() const { return connection_open_timeout_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds connection_close_timeout() const { return connection_close_timeout_; }
		[[nodiscard]] constexpr const std::optional<proton::duration>& reconnect_delay() const { return reconnect_delay_; }
		[[nodiscard]] constexpr const std::optional<float>& reconnect_delay_multiplier() const { return reconnect_delay_multiplier_; }
		[[nodiscard]] constexpr const std::optional<proton::duration>& reconnect_max_delay() const { return reconnect_max_delay_; }
		[[nodiscard]] constexpr const std::optional<int>& reconnect_max_attempts() const { return reconnect_max_attempts_; }
		[[nodiscard]] constexpr const std::optional<enum proton::ssl::verify_mode>& ssl_verify_mode() const { return ssl_verify_mode_; }
		[[nodiscard]] constexpr const std::optional<std::string>& ssl_trust_db() const { return ssl_trust_db_; }
		[[nodiscard]] constexpr const std::optional<std::string>& ssl_certdb_main() const { return ssl_certdb_main_; }
		[[nodiscard]] constexpr const std::optional<std::string>& ssl_certdb_extra() const { return ssl_certdb_extra_; }
		[[nodiscard]] constexpr const std::optional<std::string>& ssl_cert_password() const { return ssl_cert_password_; }
		[[nodiscard]] constexpr const std::optional<bool>& sasl_enabled() const { return sasl_enabled_; }
		[[nodiscard]] constexpr const std::optional<std::string>& sasl_mechanisms() const { return sasl_mechanisms_; }
		[[nodiscard]] constexpr const std::optional<bool>& sasl_allow_insecure() const { return sasl_allow_insecure_; }
		[[nodiscard]] constexpr const std::optional<enum proton::delivery_mode::modes>& sender_delivery_mode() const { return sender_delivery_mode_; }
		[[nodiscard]] constexpr const std::optional<bool>& sender_auto_settle() const { return sender_auto_settle_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds sender_open_timeout() const { return sender_open_timeout_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds sender_close_timeout() const { return sender_close_timeout_; }
		[[nodiscard]] constexpr const std::optional<std::string>& sender_source_address() const { return sender_source_address_; }
		[[nodiscard]] constexpr const std::optional<bool>& sender_source_dynamic() const { return sender_source_dynamic_; }
		[[nodiscard]] constexpr const std::optional<bool>& sender_source_anonymous() const { return sender_source_anonymous_; }
		[[nodiscard]] constexpr const std::optional<enum proton::source::distribution_mode>& sender_source_distribution_mode() const { return sender_source_distribution_mode_; }
		[[nodiscard]] constexpr const std::optional<enum proton::source::durability_mode>& sender_source_durability_mode() const { return sender_source_durability_mode_; }
		[[nodiscard]] constexpr const std::optional<proton::duration>& sender_source_timeout() const { return sender_source_timeout_; }
		[[nodiscard]] constexpr const std::optional<enum proton::source::expiry_policy>& sender_source_expiry_policy() const { return sender_source_expiry_policy_; }
		[[nodiscard]] constexpr const std::optional<std::string>& sender_target_address() const { return sender_target_address_; }
		[[nodiscard]] constexpr const std::optional<bool>& sender_target_dynamic() const { return sender_target_dynamic_; }
		[[nodiscard]] constexpr const std::optional<bool>& sender_target_anonymous() const { return sender_target_anonymous_; }
		[[nodiscard]] constexpr const std::optional<enum proton::target::durability_mode>& sender_target_durability_mode() const { return sender_target_durability_mode_; }
		[[nodiscard]] constexpr const std::optional<proton::duration>& sender_target_timeout() const { return sender_target_timeout_; }
		[[nodiscard]] constexpr const std::optional<enum proton::target::expiry_policy>& sender_target_expiry_policy() const { return sender_target_expiry_policy_; }
		[[nodiscard]] constexpr const std::optional<bool>& durable_messages() const { return durable_messages_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds message_send_timeout() const { return message_send_timeout_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds session_open_timeout() const { return session_open_timeout_; }
		[[nodiscard]] constexpr const std::chrono::milliseconds session_close_timeout() const { return session_close_timeout_; }
		// NOLINTEND(readability-const-return-type)

		[[nodiscard]] static const amqp_config& default_config()
		{
			if (!default_instance_.has_value()) {
				amqp_config config;
				config.initialize_from_defaults();
				default_instance_ = config;
			}

			return *default_instance_;
		}

		static constexpr const char* const KW_ENDPOINTS = "amqp_endpoints";
		static constexpr const char* const KW_ENDPOINT_SCHEME = "scheme";
		static constexpr const char* const KW_ENDPOINT_HOST = "host";
		static constexpr const char* const KW_ENDPOINT_PORT = "port";
		static constexpr const char* const KW_ENDPOINT_PARAMETERS = "parameters";
		static constexpr const char* const KW_ENDPOINT_FRAGMENT = "fragment";

		static constexpr const char* const KW_USER = "amqp_user";
		static constexpr const char* const KW_PASSWORD = "amqp_password";

		static constexpr const char* const KW_PATH = "amqp_path";
		static constexpr const char* const KW_PATH_PARAMETERS = "amqp_path_parameters";
		static constexpr const char* const KW_PATH_FRAGMENT = "amqp_path_fragment";

		static constexpr const char* const KW_DEPRECATED_LOCATION = "amqp_location";
		static constexpr const char* const KW_DEPRECATED_TOPIC = "amqp_topic";

		static constexpr const char* const KW_SSL = "amqp_ssl";
		static constexpr const char* const KW_SSL_VERIFY_MODE = "verify_mode";
		static constexpr const char* const KW_SSL_TRUST_DB = "trust_db";
		static constexpr const char* const KW_SSL_CERTDB_MAIN = "certdb_main";
		static constexpr const char* const KW_SSL_CERTDB_EXTRA = "certdb_extra";
		static constexpr const char* const KW_SSL_CERT_PASSWORD = "cert_password";

		static constexpr const char* const KW_SASL = "amqp_sasl";
		static constexpr const char* const KW_SASL_ENABLE = "enable";
		static constexpr const char* const KW_SASL_MECHANISMS = "mechanisms";
		static constexpr const char* const KW_SASL_ALLOW_INSECURE = "allow_insecure";

		static constexpr const char* const KW_CONNECTION_MAX_FRAME_SIZE = "amqp_connection_max_frame_size";
		static constexpr const char* const KW_CONNECTION_MAX_SESSIONS = "amqp_connection_max_sessions";
		static constexpr const char* const KW_CONNECTION_IDLE_TIMEOUT = "amqp_connection_idle_timeout";
		static constexpr const char* const KW_CONNECTION_VIRTUAL_HOST = "amqp_connection_virtual_host";
		static constexpr const char* const KW_CONNECTION_OPEN_TIMEOUT = "amqp_connection_open_timeout";
		static constexpr const char* const KW_CONNECTION_CLOSE_TIMEOUT = "amqp_connection_close_timeout";
		static constexpr const char* const KW_RECONNECT_BASE_DELAY = "amqp_reconnect_base_delay";
		static constexpr const char* const KW_RECONNECT_DELAY_MULTIPLIER = "amqp_reconnect_delay_multiplier";
		static constexpr const char* const KW_RECONNECT_MAX_DELAY = "amqp_reconnect_max_delay";
		static constexpr const char* const KW_RECONNECT_MAX_ATTEMPTS = "amqp_reconnect_max_attempts";

		static constexpr const char* const KW_SENDER = "amqp_sender";
		static constexpr const char* const KW_LINK_DELIVERY_MODE = "delivery_mode";
		static constexpr const char* const KW_LINK_AUTO_SETTLE = "auto_settle";
		static constexpr const char* const KW_LINK_OPEN_TIMEOUT = "open_timeout";
		static constexpr const char* const KW_LINK_CLOSE_TIMEOUT = "close_timeout";

		static constexpr const char* const KW_LINK_SOURCE = "source";
		static constexpr const char* const KW_LINK_TARGET = "target";
		static constexpr const char* const KW_TERMINUS_ADDRESS = "address";
		static constexpr const char* const KW_TERMINUS_DYNAMIC = "dynamic";
		static constexpr const char* const KW_TERMINUS_ANONYMOUS = "anonymous";
		static constexpr const char* const KW_TERMINUS_DURABILITY_MODE = "durability_mode";
		static constexpr const char* const KW_TERMINUS_TIMEOUT = "timeout";
		static constexpr const char* const KW_TERMINUS_EXPIRY_POLICY = "expiry_policy";
		static constexpr const char* const KW_SOURCE_DISTRIBUTION_MODE = "distribution_mode";

		static constexpr const char* const KW_DURABLE_MESSAGES = "amqp_durable_messages";
		static constexpr const char* const KW_MESSAGE_SEND_TIMEOUT = "amqp_message_send_timeout";

		static constexpr const char* const KW_SESSION_OPEN_TIMEOUT = "amqp_session_open_timeout";
		static constexpr const char* const KW_SESSION_CLOSE_TIMEOUT = "amqp_session_close_timeout";

		static constexpr const char* const KW_DEPRECATED_OPTIONS = "amqp_options";

	  private:
		bool is_initialized_{false};

		std::string primary_endpoint_;
		std::vector<std::string> failover_endpoints_;

		std::string path_;

		std::optional<std::string> user_;
		std::optional<std::string> password_;

		std::optional<std::uint32_t> connection_max_frame_size_;
		std::optional<std::uint16_t> connection_max_sessions_;
		std::optional<proton::duration> connection_idle_timeout_;
		std::optional<std::string> connection_virtual_host_;
		std::chrono::milliseconds connection_open_timeout_;
		std::chrono::milliseconds connection_close_timeout_;
		std::optional<proton::duration> reconnect_delay_;
		std::optional<float> reconnect_delay_multiplier_;
		std::optional<proton::duration> reconnect_max_delay_;
		std::optional<int> reconnect_max_attempts_;

		std::optional<enum proton::ssl::verify_mode> ssl_verify_mode_;
		std::optional<std::string> ssl_trust_db_;
		std::optional<std::string> ssl_certdb_main_;
		std::optional<std::string> ssl_certdb_extra_;
		std::optional<std::string> ssl_cert_password_;

		std::optional<bool> sasl_enabled_;
		std::optional<std::string> sasl_mechanisms_;
		std::optional<bool> sasl_allow_insecure_;

		std::optional<enum proton::delivery_mode::modes> sender_delivery_mode_;
		std::optional<bool> sender_auto_settle_;
		std::chrono::milliseconds sender_open_timeout_;
		std::chrono::milliseconds sender_close_timeout_;

		std::optional<std::string> sender_source_address_;
		std::optional<bool> sender_source_dynamic_;
		std::optional<bool> sender_source_anonymous_;
		std::optional<enum proton::source::distribution_mode> sender_source_distribution_mode_;
		std::optional<enum proton::source::durability_mode> sender_source_durability_mode_;
		std::optional<proton::duration> sender_source_timeout_;
		std::optional<enum proton::source::expiry_policy> sender_source_expiry_policy_;

		std::optional<std::string> sender_target_address_;
		std::optional<bool> sender_target_dynamic_;
		std::optional<bool> sender_target_anonymous_;
		std::optional<enum proton::target::durability_mode> sender_target_durability_mode_;
		std::optional<proton::duration> sender_target_timeout_;
		std::optional<enum proton::target::expiry_policy> sender_target_expiry_policy_;

		std::optional<bool> durable_messages_;
		std::chrono::milliseconds message_send_timeout_;

		std::chrono::milliseconds session_open_timeout_;
		std::chrono::milliseconds session_close_timeout_;

		static std::optional<amqp_config> default_instance_;
	};
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_CONFIG_HPP
