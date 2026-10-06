#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_config.hpp"
#include "irods/private/audit_amqp_version.hpp"

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_error.hpp>
#include <irods/irods_version.h>
#include <irods/rodsErrorTable.h>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/preprocessor/stringize.hpp>
#include <boost/url/url_view.hpp>

#include <fmt/format.h>
#include <fmt/compile.h>

#include <nlohmann/json.hpp>

#include <proton/connection_options.hpp>
#include <proton/duration.hpp>
#include <proton/delivery_mode.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/source.hpp>
#include <proton/source_options.hpp>
#include <proton/ssl.hpp>
#include <proton/symbol.hpp>
#include <proton/target.hpp>
#include <proton/target_options.hpp>
#include <proton/value.hpp>

#include <chrono>
#include <cstdint>
#include <limits>
#include <map>
#include <sstream>
#include <string>

namespace irods::plugin::rule_engine::audit_amqp
{
	irods::error amqp_config::initialize(const nlohmann::json& _plugin_specific_configuration,
	                                     const std::string& _re_instance_name)
	{
		// start fresh
		deinitialize();

		bool found_endpoint = false;
		const auto amqp_endpoints_cfg = _plugin_specific_configuration.find(KW_ENDPOINTS);
		if (amqp_endpoints_cfg != _plugin_specific_configuration.end()) {
			const auto& endpoints_cfg = amqp_endpoints_cfg->get_ref<const nlohmann::json::array_t&>();
			for (const auto& endpoint_cfg : endpoints_cfg) {
				if (!endpoint_cfg.is_object()) {
					const std::string& errmsg =
						fmt::format(FMT_COMPILE("One or more {} entries is invalid type."), KW_ENDPOINTS);
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message", errmsg},
					});
					// clang-format on
					return ERROR(KEY_TYPE_MISMATCH, errmsg);
				}

				std::stringstream endpoint_ss;

				const auto scheme_cfg = endpoint_cfg.find(KW_ENDPOINT_SCHEME);
				if ((scheme_cfg != endpoint_cfg.end()) && !scheme_cfg->is_null()) {
					endpoint_ss << scheme_cfg->get_ref<const std::string&>() << "://";
				}

				const auto& host = endpoint_cfg.at(KW_ENDPOINT_HOST).get_ref<const std::string&>();
				endpoint_ss << host;

				const auto port_cfg = endpoint_cfg.find(KW_ENDPOINT_PORT);
				if ((port_cfg != endpoint_cfg.end()) && !port_cfg->is_null()) {
					const auto port = port_cfg->get<nlohmann::json::number_unsigned_t>();
					if (port > std::numeric_limits<std::uint16_t>::max()) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("AMQP endpoint port must not exceed {}."),
							             std::numeric_limits<std::uint16_t>::max())},
							{KW_ENDPOINT_PORT, std::to_string(port)}
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("AMQP endpoint port greater than {}."),
						                         std::numeric_limits<std::uint16_t>::max()));
					}
					endpoint_ss << ':' << std::to_string(port);
				}

				bool found_endpoint_params = false;
				const auto endpoint_params_cfg = endpoint_cfg.find(KW_ENDPOINT_PARAMETERS);
				if ((endpoint_params_cfg != endpoint_cfg.end()) && !endpoint_params_cfg->is_null()) {
					const auto& endpoint_params = endpoint_params_cfg->get_ref<const nlohmann::json::object_t&>();
					for (const auto& [ep_key, ep_val] : endpoint_params) {
						if (found_endpoint_params) {
							endpoint_ss << '&' << ep_key;
						}
						else {
							endpoint_ss << "/?" << ep_key;
						}

						found_endpoint_params = true;

						if (ep_val.is_null()) {
							continue;
						}

						endpoint_ss << '=' << ep_val.get_ref<const std::string&>();
					}
				}

				const auto endpoint_frag_cfg = endpoint_cfg.find(KW_ENDPOINT_FRAGMENT);
				if ((endpoint_frag_cfg != endpoint_cfg.end()) && !endpoint_frag_cfg->is_null()) {
					endpoint_ss << '#' << endpoint_frag_cfg->get_ref<const std::string&>();
				}

				if (found_endpoint) {
					failover_endpoints_.push_back(endpoint_ss.str());
				}
				else {
					primary_endpoint_ = endpoint_ss.str();
					found_endpoint = true;
				}
			}
		}

		bool found_user = false;
		const auto user_cfg = _plugin_specific_configuration.find(KW_USER);
		if (user_cfg != _plugin_specific_configuration.end()) {
			found_user = true;
			if (!user_cfg->is_null()) {
				user_ = user_cfg->get_ref<const std::string&>();
			}
		}

		bool found_password = false;
		const auto password_cfg = _plugin_specific_configuration.find(KW_PASSWORD);
		if (password_cfg != _plugin_specific_configuration.end()) {
			found_password = true;
			if (!password_cfg->is_null()) {
				password_ = password_cfg->get_ref<const std::string&>();
			}
		}

		// check amqp_location
		const auto amqp_location_cfg = _plugin_specific_configuration.find(KW_DEPRECATED_LOCATION);
		if (amqp_location_cfg == _plugin_specific_configuration.end()) {
			if (!found_endpoint) {
				const std::string& errmsg =
					fmt::format(FMT_COMPILE("{} empty or not present in rule engine configuration."), KW_ENDPOINTS);
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message", errmsg}
				});
				// clang-format on
				return ERROR(KEY_NOT_FOUND, errmsg);
			}
		}
		else {
			// clang-format off
			log_re::warn({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message",
				 fmt::format(FMT_COMPILE("Found {} configuration setting. This setting has been deprecated in favor "
				                         "of {}, {}, and {} and will be ignored in future versions of the plugin."),
				             KW_DEPRECATED_LOCATION, KW_ENDPOINTS, KW_USER, KW_PASSWORD)}
			});
			// clang-format on

			const auto& amqp_location = amqp_location_cfg->get_ref<const std::string&>();
			const boost::urls::url_view proton_url(amqp_location);

			if (found_endpoint) {
				// clang-format off
				log_re::info({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Ignoring location from {} in favor of {}."),
					             KW_DEPRECATED_LOCATION, KW_ENDPOINTS)},
				});
				// clang-format on
			}
			else {
				if (!proton_url.has_authority()) {
					const std::string& errmsg =
						fmt::format(FMT_COMPILE("Cannot derive AMQP endpoint host from {}."), KW_DEPRECATED_LOCATION);
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message", errmsg},
						{KW_DEPRECATED_LOCATION, amqp_location},
					});
					// clang-format on
					return ERROR(SYS_CONFIG_FILE_ERR, errmsg);
				}

				std::stringstream endpoint_ss;
				if (proton_url.has_scheme()) {
					endpoint_ss << proton_url.scheme() << "://";
				}
				endpoint_ss << proton_url.encoded_host_and_port();

				if (proton_url.has_query() || proton_url.has_fragment()) {
					endpoint_ss << '/';
					if (proton_url.has_query()) {
						endpoint_ss << '?' << proton_url.encoded_query();
					}
					if (proton_url.has_fragment()) {
						endpoint_ss << '#' << proton_url.encoded_fragment();
					}
				}

				primary_endpoint_ = endpoint_ss.str();
			}

			if (proton_url.has_userinfo()) {
				if (found_user) {
					// clang-format off
					log_re::info({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("Ignoring credentials from {} in favor of {} and {}."),
						             KW_DEPRECATED_LOCATION, KW_USER, KW_PASSWORD)},
					});
					// clang-format on
				}
				else {
					user_ = proton_url.user();
					found_user = true;
					if (proton_url.has_password()) {
						if (found_password) {
							// clang-format off
							log_re::info({
								{"rule_engine_plugin", rule_engine_name},
								{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
								{"log_message",
								 fmt::format(FMT_COMPILE("Ignoring password from {} in favor of {}."),
								             KW_DEPRECATED_LOCATION, KW_USER)},
							});
							// clang-format on
						}
						else {
							password_ = proton_url.password();
							found_password = true;
						}
					}
				}
			}
		}

		failover_endpoints_.shrink_to_fit();

		if (!found_user) {
			user_ = defaults::user;
		}
		if (!found_password) {
			password_ = defaults::password;
		}

		bool found_path = false;
		std::stringstream path_ss;
		const auto amqp_path_cfg = _plugin_specific_configuration.find(KW_PATH);
		if ((amqp_path_cfg != _plugin_specific_configuration.end()) && !amqp_path_cfg->is_null()) {
			const auto& amqp_path_str = amqp_path_cfg->get_ref<const std::string&>();
			if (!amqp_path_str.empty()) {
				path_ss << '/' << amqp_path_str;
			}
			found_path = true;
		}

		// check amqp_topic
		const auto amqp_topic_cfg = _plugin_specific_configuration.find(KW_DEPRECATED_TOPIC);
		if (amqp_topic_cfg == _plugin_specific_configuration.end()) {
			if (!found_path) {
				const std::string& errmsg =
					fmt::format(FMT_COMPILE("{} not present in rule engine configuration."), KW_PATH);
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message", errmsg},
				});
				// clang-format on
				return ERROR(KEY_NOT_FOUND, errmsg);
			}
		}
		else {
			// clang-format off
			log_re::warn({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message",
				 fmt::format(FMT_COMPILE("Found {} configuration setting. This setting has been deprecated in favor "
				                         "of {} and will be ignored in future versions of the plugin."),
				             KW_DEPRECATED_TOPIC, KW_PATH)},
			});
			// clang-format on

			if (found_path) {
				// clang-format off
				log_re::info({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Ignoring {} in favor of {}."), KW_DEPRECATED_TOPIC, KW_PATH)},
				});
				// clang-format on
			}
			else {
				const auto& amqp_topic_str = amqp_topic_cfg->get_ref<const std::string&>();
				if (!amqp_topic_str.empty()) {
					path_ss << '/' << amqp_topic_str;
				}
			}
		}

		bool found_path_params = false;
		const auto path_params_cfg = _plugin_specific_configuration.find(KW_PATH_PARAMETERS);
		if ((path_params_cfg != _plugin_specific_configuration.end()) && !path_params_cfg->is_null()) {
			const auto& path_params = path_params_cfg->get_ref<const nlohmann::json::object_t&>();
			for (const auto& [pp_key, pp_val] : path_params) {
				path_ss << (found_path_params ? '&' : '?') << pp_key;

				found_path_params = true;

				if (pp_val.is_null()) {
					continue;
				}

				path_ss << '=' << pp_val.get_ref<const std::string&>();
			}
		}

		const auto path_frag_cfg = _plugin_specific_configuration.find(KW_PATH_FRAGMENT);
		if ((path_frag_cfg != _plugin_specific_configuration.end()) && !path_frag_cfg->is_null()) {
			path_ss << '#' << path_frag_cfg->get_ref<const std::string&>();
		}

		path_ = path_ss.str();

		const auto amqp_ssl_cfg = _plugin_specific_configuration.find(KW_SSL);
		if ((amqp_ssl_cfg == _plugin_specific_configuration.end()) || amqp_ssl_cfg->is_null()) {
			ssl_verify_mode_ = defaults::ssl_verify_mode;
			ssl_trust_db_ = defaults::ssl_trust_db;
			ssl_certdb_main_ = defaults::ssl_certdb_main;
			ssl_certdb_extra_ = defaults::ssl_certdb_extra;
			ssl_cert_password_ = defaults::ssl_cert_password;
		}
		else if (!amqp_ssl_cfg->is_object()) {
			const std::string& errmsg = fmt::format(FMT_COMPILE("{} is invalid type."), KW_SSL);
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message", errmsg},
			});
			// clang-format on
			return ERROR(KEY_TYPE_MISMATCH, errmsg);
		}
		else {
			const auto& ssl_cfg = *amqp_ssl_cfg;

			const auto ssl_verify_mode_cfg = ssl_cfg.find(KW_SSL_VERIFY_MODE);
			if (ssl_verify_mode_cfg == ssl_cfg.end()) {
				ssl_verify_mode_ = defaults::ssl_verify_mode;
			}
			else if (!ssl_verify_mode_cfg->is_null()) {
				const std::string& ssl_verify_mode = ssl_verify_mode_cfg->get_ref<const std::string&>();

				if (boost::iequals(ssl_verify_mode, "VERIFY_PEER")) {
					ssl_verify_mode_ = proton::ssl::verify_mode::VERIFY_PEER;
				}
				else if (boost::iequals(ssl_verify_mode, "ANONYMOUS_PEER")) {
					ssl_verify_mode_ = proton::ssl::verify_mode::ANONYMOUS_PEER;
				}
				else if (boost::iequals(ssl_verify_mode, "VERIFY_PEER_NAME")) {
					ssl_verify_mode_ = proton::ssl::verify_mode::VERIFY_PEER_NAME;
				}
				else {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("{} must be one of [VERIFY_PEER, ANONYMOUS_PEER, VERIFY_PEER_NAME]."),
						             KW_SSL_VERIFY_MODE)},
						{fmt::format(FMT_COMPILE("{}:{}"), KW_SSL, KW_SSL_VERIFY_MODE), ssl_verify_mode},
					});
					// clang-format on
					return ERROR(SYS_CONFIG_FILE_ERR,
					             fmt::format(FMT_COMPILE("Unrecognized {}:{} value."), KW_SSL, KW_SSL_VERIFY_MODE));
				}
			}

			bool found_trust_db = false;
			const auto ssl_trust_db_cfg = ssl_cfg.find(KW_SSL_TRUST_DB);
			if (ssl_trust_db_cfg == ssl_cfg.end()) {
				ssl_trust_db_ = defaults::ssl_trust_db;
			}
			else if (!ssl_trust_db_cfg->is_null()) {
				ssl_trust_db_ = ssl_trust_db_cfg->get_ref<const std::string&>();
				found_trust_db = true;
			}

			bool found_certdb_main = false;
			const auto ssl_certdb_main_cfg = ssl_cfg.find(KW_SSL_CERTDB_MAIN);
			if (ssl_certdb_main_cfg == ssl_cfg.end()) {
				ssl_certdb_main_ = defaults::ssl_certdb_main;
			}
			else if (!found_trust_db) {
				ssl_certdb_main_ = defaults::ssl_certdb_main;
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Found {} but no {}. Ignoring {}."),
					             KW_SSL_CERTDB_MAIN, KW_SSL_TRUST_DB, KW_SSL_CERTDB_MAIN)},
				});
				// clang-format on
			}
			else if (!ssl_certdb_main_cfg->is_null()) {
				ssl_certdb_main_ = ssl_certdb_main_cfg->get_ref<const std::string&>();
				found_certdb_main = true;
			}

			bool found_certdb_extra = false;
			const auto ssl_certdb_extra_cfg = ssl_cfg.find(KW_SSL_CERTDB_EXTRA);
			if (ssl_certdb_extra_cfg == ssl_cfg.end()) {
				ssl_certdb_extra_ = defaults::ssl_certdb_extra;
			}
			else if (!found_certdb_main) {
				ssl_certdb_extra_ = defaults::ssl_certdb_extra;
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Found {} but no {}. Ignoring {}."),
					             KW_SSL_CERTDB_EXTRA, KW_SSL_CERTDB_MAIN, KW_SSL_CERTDB_EXTRA)},
				});
				// clang-format on
			}
			else if (!ssl_certdb_extra_cfg->is_null()) {
				ssl_certdb_extra_ = ssl_certdb_extra_cfg->get_ref<const std::string&>();
				found_certdb_extra = true;
			}

			const auto ssl_cert_password_cfg = ssl_cfg.find(KW_SSL_CERT_PASSWORD);
			if (ssl_cert_password_cfg == ssl_cfg.end()) {
				ssl_cert_password_ = defaults::ssl_cert_password;
			}
			else if (!found_certdb_extra) {
				ssl_cert_password_ = defaults::ssl_cert_password;
				// clang-format off
				log_re::warn({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Found {} but no {}. Ignoring {}."),
					             KW_SSL_CERT_PASSWORD, KW_SSL_CERTDB_EXTRA, KW_SSL_CERT_PASSWORD)},
				});
				// clang-format on
			}
			else if (!ssl_cert_password_cfg->is_null()) {
				ssl_cert_password_ = ssl_cert_password_cfg->get_ref<const std::string&>();
			}
		}

		const auto amqp_sasl_cfg = _plugin_specific_configuration.find(KW_SASL);
		if ((amqp_sasl_cfg == _plugin_specific_configuration.end()) || amqp_sasl_cfg->is_null()) {
			sasl_enabled_ = defaults::sasl_enabled;
			sasl_mechanisms_ = defaults::sasl_mechanisms;
			sasl_allow_insecure_ = defaults::sasl_allow_insecure;
		}
		else if (!amqp_sasl_cfg->is_object()) {
			const std::string& errmsg = fmt::format(FMT_COMPILE("{} is invalid type."), KW_SASL);
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message", errmsg},
			});
			// clang-format on
			return ERROR(KEY_TYPE_MISMATCH, errmsg);
		}
		else {
			const auto& sasl_cfg = *amqp_sasl_cfg;

			const auto sasl_enabled_cfg = sasl_cfg.find(KW_SASL_ENABLE);
			if (sasl_enabled_cfg == sasl_cfg.end()) {
				sasl_enabled_ = defaults::sasl_enabled;
			}
			else if (!sasl_enabled_cfg->is_null()) {
				sasl_enabled_ = sasl_enabled_cfg->get<bool>();
			}

			const auto mechanisms_cfg = sasl_cfg.find(KW_SASL_MECHANISMS);
			if (mechanisms_cfg == sasl_cfg.end()) {
				sasl_mechanisms_ = defaults::sasl_mechanisms;
			}
			else if (mechanisms_cfg->is_array()) {
				const auto& mechanisms_arr = mechanisms_cfg->get_ref<const nlohmann::json::array_t&>();
				if (mechanisms_arr.empty()) {
					sasl_mechanisms_ = "";
				}
				else {
					auto mechanisms_itr = mechanisms_arr.begin();
					std::stringstream mechanisms_ss;
					mechanisms_ss << mechanisms_itr->get_ref<const std::string&>();
					while (++mechanisms_itr != mechanisms_arr.end()) {
						mechanisms_ss << ' ' << mechanisms_itr->get_ref<const std::string&>();
					}
					sasl_mechanisms_ = mechanisms_ss.str();
				}
			}
			else if (!mechanisms_cfg->is_null()) {
				sasl_mechanisms_ = mechanisms_cfg->get_ref<const std::string&>();
			}

			const auto sasl_allow_insecure_cfg = sasl_cfg.find(KW_SASL_ALLOW_INSECURE);
			if (sasl_allow_insecure_cfg == sasl_cfg.end()) {
				sasl_allow_insecure_ = defaults::sasl_allow_insecure;
			}
			else if (!sasl_allow_insecure_cfg->is_null()) {
				sasl_allow_insecure_ = sasl_allow_insecure_cfg->get<bool>();
			}
		}

		const auto max_frame_size_cfg = _plugin_specific_configuration.find(KW_CONNECTION_MAX_FRAME_SIZE);
		if (max_frame_size_cfg == _plugin_specific_configuration.end()) {
			connection_max_frame_size_ = defaults::connection_max_frame_size;
		}
		else if (!max_frame_size_cfg->is_null()) {
			const auto max_frame_size = max_frame_size_cfg->get<nlohmann::json::number_unsigned_t>();
			if (max_frame_size > std::numeric_limits<std::uint32_t>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Max frame size must not exceed {}."),
					             std::numeric_limits<std::uint32_t>::max())},
					{KW_CONNECTION_MAX_FRAME_SIZE, std::to_string(max_frame_size)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Max frame size greater than {}."),
				                         std::numeric_limits<std::uint32_t>::max()));
			}
			connection_max_frame_size_ = static_cast<std::uint32_t>(max_frame_size);
		}

		const auto max_sessions_cfg = _plugin_specific_configuration.find(KW_CONNECTION_MAX_SESSIONS);
		if (max_sessions_cfg == _plugin_specific_configuration.end()) {
			connection_max_sessions_ = defaults::connection_max_sessions;
		}
		else if (!max_sessions_cfg->is_null()) {
			const auto max_sessions = max_sessions_cfg->get<nlohmann::json::number_unsigned_t>();
			if (max_sessions > std::numeric_limits<std::uint16_t>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Max sessions must not exceed {}."),
					             std::numeric_limits<std::uint16_t>::max())},
					{KW_CONNECTION_MAX_SESSIONS, std::to_string(max_sessions)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Max sessions greater than {}."),
				                         std::numeric_limits<std::uint16_t>::max()));
			}
			connection_max_sessions_ = static_cast<std::uint16_t>(max_sessions);
		}

		const auto idle_timeout_cfg = _plugin_specific_configuration.find(KW_CONNECTION_IDLE_TIMEOUT);
		if (idle_timeout_cfg == _plugin_specific_configuration.end()) {
			connection_idle_timeout_ = defaults::connection_idle_timeout;
		}
		else if (!idle_timeout_cfg->is_null()) {
			const auto idle_timeout = idle_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (idle_timeout > std::numeric_limits<proton::duration::numeric_type>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Idle timeout must not exceed {}."),
					             std::numeric_limits<proton::duration::numeric_type>::max())},
					{KW_CONNECTION_IDLE_TIMEOUT, std::to_string(idle_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Idle timeout greater than {}."),
				                         std::numeric_limits<proton::duration::numeric_type>::max()));
			}
			connection_idle_timeout_ = static_cast<proton::duration::numeric_type>(idle_timeout);
		}

		const auto virtual_host_cfg = _plugin_specific_configuration.find(KW_CONNECTION_VIRTUAL_HOST);
		if (virtual_host_cfg == _plugin_specific_configuration.end()) {
			connection_virtual_host_ = defaults::connection_virtual_host;
		}
		else if (!virtual_host_cfg->is_null()) {
			connection_virtual_host_ = virtual_host_cfg->get_ref<const std::string&>();
		}

		const auto connection_open_timeout_cfg = _plugin_specific_configuration.find(KW_CONNECTION_OPEN_TIMEOUT);
		if (connection_open_timeout_cfg == _plugin_specific_configuration.end()) {
			connection_open_timeout_ = defaults::connection_open_timeout;
		}
		else if (!connection_open_timeout_cfg->is_null()) {
			const auto connection_open_timeout = connection_open_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (connection_open_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Connection open timeout must not exceed {}."),
					             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
					{KW_CONNECTION_OPEN_TIMEOUT, std::to_string(connection_open_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Connection open timeout greater than {}."),
				                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
			}
			connection_open_timeout_ =
				std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(connection_open_timeout));
		}

		const auto connection_close_timeout_cfg = _plugin_specific_configuration.find(KW_CONNECTION_CLOSE_TIMEOUT);
		if (connection_close_timeout_cfg == _plugin_specific_configuration.end()) {
			connection_close_timeout_ = defaults::connection_close_timeout;
		}
		else if (!connection_close_timeout_cfg->is_null()) {
			const auto connection_close_timeout =
				connection_close_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (connection_close_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Connection close timeout must not exceed {}."),
					             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
					{KW_CONNECTION_CLOSE_TIMEOUT, std::to_string(connection_close_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Connection close timeout greater than {}."),
				                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
			}
			connection_close_timeout_ =
				std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(connection_close_timeout));
		}

		const auto reconnect_delay_cfg = _plugin_specific_configuration.find(KW_RECONNECT_BASE_DELAY);
		if (reconnect_delay_cfg == _plugin_specific_configuration.end()) {
			reconnect_delay_ = defaults::reconnect_delay;
		}
		else if (!reconnect_delay_cfg->is_null()) {
			const auto reconnect_delay = reconnect_delay_cfg->get<nlohmann::json::number_unsigned_t>();
			if (reconnect_delay > std::numeric_limits<proton::duration::numeric_type>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Base reconnect delay must not exceed {}."),
					             std::numeric_limits<proton::duration::numeric_type>::max())},
					{KW_RECONNECT_BASE_DELAY, std::to_string(reconnect_delay)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Base reconnect delay greater than {}."),
				                         std::numeric_limits<proton::duration::numeric_type>::max()));
			}
			reconnect_delay_ = static_cast<proton::duration::numeric_type>(reconnect_delay);
		}

		const auto reconnect_delay_multiplier_cfg = _plugin_specific_configuration.find(KW_RECONNECT_DELAY_MULTIPLIER);
		if (reconnect_delay_multiplier_cfg == _plugin_specific_configuration.end()) {
			reconnect_delay_multiplier_ = defaults::reconnect_delay_multiplier;
		}
		else if (!reconnect_delay_multiplier_cfg->is_null()) {
			reconnect_delay_multiplier_ =
				static_cast<float>(reconnect_delay_multiplier_cfg->get<nlohmann::json::number_float_t>());
		}

		const auto reconnect_max_delay_cfg = _plugin_specific_configuration.find(KW_RECONNECT_MAX_DELAY);
		if (reconnect_max_delay_cfg == _plugin_specific_configuration.end()) {
			reconnect_max_delay_ = defaults::reconnect_max_delay;
		}
		else if (!reconnect_max_delay_cfg->is_null()) {
			const auto reconnect_max_delay = reconnect_max_delay_cfg->get<nlohmann::json::number_unsigned_t>();
			if (reconnect_max_delay > std::numeric_limits<proton::duration::numeric_type>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Max reconnect delay must not exceed {}."),
					             std::numeric_limits<proton::duration::numeric_type>::max())},
					{KW_RECONNECT_MAX_DELAY, std::to_string(reconnect_max_delay)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Max reconnect delay greater than {}."),
				                         std::numeric_limits<proton::duration::numeric_type>::max()));
			}
			reconnect_max_delay_ = static_cast<proton::duration::numeric_type>(reconnect_max_delay);
		}

		const auto reconnect_max_attempts_cfg = _plugin_specific_configuration.find(KW_RECONNECT_MAX_ATTEMPTS);
		if (reconnect_max_attempts_cfg == _plugin_specific_configuration.end()) {
			reconnect_max_attempts_ = defaults::reconnect_max_attempts;
		}
		else if (!reconnect_max_attempts_cfg->is_null()) {
			const auto reconnect_max_attempts = reconnect_max_attempts_cfg->get<nlohmann::json::number_unsigned_t>();
			if (reconnect_max_attempts > std::numeric_limits<int>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Max reconnect attempts must not exceed {}."),
					             std::numeric_limits<int>::max())},
					{KW_RECONNECT_MAX_ATTEMPTS, std::to_string(reconnect_max_attempts)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Max reconnect attempts greater than {}."),
				                         std::numeric_limits<int>::max()));
			}
			reconnect_max_attempts_ = static_cast<int>(reconnect_max_attempts);
		}

		const auto amqp_sender_cfg = _plugin_specific_configuration.find(KW_SENDER);
		if ((amqp_sender_cfg == _plugin_specific_configuration.end()) || amqp_sender_cfg->is_null()) {
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
		}
		else if (!amqp_sender_cfg->is_object()) {
			const std::string& errmsg = fmt::format(FMT_COMPILE("{} is invalid type."), KW_SENDER);
			// clang-format off
			log_re::error({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message", errmsg},
			});
			// clang-format on
			return ERROR(KEY_TYPE_MISMATCH, errmsg);
		}
		else {
			const auto& sender_cfg = *amqp_sender_cfg;

			const auto sender_delivery_mode_cfg = sender_cfg.find(KW_LINK_DELIVERY_MODE);
			if (sender_delivery_mode_cfg == sender_cfg.end()) {
				sender_delivery_mode_ = defaults::sender_delivery_mode;
			}
			else if (!sender_delivery_mode_cfg->is_null()) {
				const std::string& sender_delivery_mode = sender_delivery_mode_cfg->get_ref<const std::string&>();

				if (boost::iequals(sender_delivery_mode, "NONE")) {
					sender_delivery_mode_.emplace(proton::delivery_mode::modes::NONE);
				}
				else if (boost::iequals(sender_delivery_mode, "AT_MOST_ONCE")) {
					sender_delivery_mode_.emplace(proton::delivery_mode::modes::AT_MOST_ONCE);
				}
				else if (boost::iequals(sender_delivery_mode, "AT_LEAST_ONCE")) {
					sender_delivery_mode_.emplace(proton::delivery_mode::modes::AT_LEAST_ONCE);
				}
				else {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("{} must be one of [NONE, AT_MOST_ONCE, AT_LEAST_ONCE]."),
						             KW_LINK_DELIVERY_MODE)},
						{fmt::format(FMT_COMPILE("{}:{}"), KW_SENDER, KW_LINK_DELIVERY_MODE), sender_delivery_mode},
					});
					// clang-format on
					return ERROR(
						SYS_CONFIG_FILE_ERR,
						fmt::format(FMT_COMPILE("Unrecognized {}:{} value."), KW_SENDER, KW_LINK_DELIVERY_MODE));
				}
			}

			const auto sender_auto_settle_cfg = sender_cfg.find(KW_LINK_AUTO_SETTLE);
			if (sender_auto_settle_cfg == sender_cfg.end()) {
				sender_auto_settle_ = defaults::sender_auto_settle;
			}
			else if (!sender_auto_settle_cfg->is_null()) {
				sender_auto_settle_ = sender_auto_settle_cfg->get<bool>();
			}

			const auto sender_open_timeout_cfg = sender_cfg.find(KW_LINK_OPEN_TIMEOUT);
			if (sender_open_timeout_cfg == sender_cfg.end()) {
				sender_open_timeout_ = defaults::sender_open_timeout;
			}
			else if (!sender_open_timeout_cfg->is_null()) {
				const auto sender_open_timeout = sender_open_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
				if (sender_open_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("Sender open timeout must not exceed {}."),
						             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
						{fmt::format(FMT_COMPILE("{}:{}"), KW_SENDER, KW_LINK_OPEN_TIMEOUT),
						 std::to_string(sender_open_timeout)},
					});
					// clang-format on
					return ERROR(SYS_CONFIG_FILE_ERR,
					             fmt::format(FMT_COMPILE("Sender open timeout greater than {}."),
					                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
				}
				sender_open_timeout_ =
					std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(sender_open_timeout));
			}

			const auto sender_close_timeout_cfg = sender_cfg.find(KW_LINK_CLOSE_TIMEOUT);
			if (sender_close_timeout_cfg == sender_cfg.end()) {
				sender_close_timeout_ = defaults::sender_close_timeout;
			}
			else if (!sender_close_timeout_cfg->is_null()) {
				const auto sender_close_timeout = sender_close_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
				if (sender_close_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
					// clang-format off
					log_re::error({
						{"rule_engine_plugin", rule_engine_name},
						{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
						{"log_message",
						 fmt::format(FMT_COMPILE("Sender close timeout must not exceed {}."),
						             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
						{fmt::format(FMT_COMPILE("{}:{}"), KW_SENDER, KW_LINK_CLOSE_TIMEOUT),
						 std::to_string(sender_close_timeout)},
					});
					// clang-format on
					return ERROR(SYS_CONFIG_FILE_ERR,
					             fmt::format(FMT_COMPILE("Sender close timeout greater than {}."),
					                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
				}
				sender_close_timeout_ =
					std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(sender_close_timeout));
			}

			const auto amqp_sender_source_cfg = sender_cfg.find(KW_LINK_SOURCE);
			if ((amqp_sender_source_cfg == sender_cfg.end()) || amqp_sender_source_cfg->is_null()) {
				sender_source_address_ = defaults::sender_source_address;
				sender_source_dynamic_ = defaults::sender_source_dynamic;
				sender_source_anonymous_ = defaults::sender_source_anonymous;
				sender_source_distribution_mode_ = defaults::sender_source_distribution_mode;
				sender_source_durability_mode_ = defaults::sender_source_durability_mode;
				sender_source_timeout_ = defaults::sender_source_timeout;
				sender_source_expiry_policy_ = defaults::sender_source_expiry_policy;
			}
			else if (!amqp_sender_source_cfg->is_object()) {
				const std::string& errmsg =
					fmt::format(FMT_COMPILE("{}:{} is invalid type."), KW_SENDER, KW_LINK_SOURCE);
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message", errmsg},
				});
				// clang-format on
				return ERROR(KEY_TYPE_MISMATCH, errmsg);
			}
			else {
				const auto& sender_source_cfg = *amqp_sender_source_cfg;

				const auto source_address_cfg = sender_source_cfg.find(KW_TERMINUS_ADDRESS);
				if (source_address_cfg == sender_source_cfg.end()) {
					sender_source_address_ = defaults::sender_source_address;
				}
				else if (!source_address_cfg->is_null()) {
					sender_source_address_ = source_address_cfg->get_ref<const std::string&>();
				}

				const auto source_dynamic_cfg = sender_source_cfg.find(KW_TERMINUS_DYNAMIC);
				if (source_dynamic_cfg == sender_source_cfg.end()) {
					sender_source_dynamic_ = defaults::sender_source_dynamic;
				}
				else if (!source_dynamic_cfg->is_null()) {
					sender_source_dynamic_ = source_dynamic_cfg->get<bool>();
				}

				const auto source_anonymous_cfg = sender_source_cfg.find(KW_TERMINUS_ANONYMOUS);
				if (source_anonymous_cfg == sender_source_cfg.end()) {
					sender_source_anonymous_ = defaults::sender_source_dynamic;
				}
				else if (!source_anonymous_cfg->is_null()) {
					sender_source_anonymous_ = source_anonymous_cfg->get<bool>();
				}

				const auto source_distribution_mode_cfg = sender_source_cfg.find(KW_SOURCE_DISTRIBUTION_MODE);
				if (source_distribution_mode_cfg == sender_source_cfg.end()) {
					sender_source_distribution_mode_ = defaults::sender_source_distribution_mode;
				}
				else if (!source_distribution_mode_cfg->is_null()) {
					const std::string& source_distribution_mode =
						source_distribution_mode_cfg->get_ref<const std::string&>();

					if (boost::iequals(source_distribution_mode, "UNSPECIFIED")) {
						sender_source_distribution_mode_ = proton::source::distribution_mode::UNSPECIFIED;
					}
					else if (boost::iequals(source_distribution_mode, "COPY")) {
						sender_source_distribution_mode_ = proton::source::distribution_mode::COPY;
					}
					else if (boost::iequals(source_distribution_mode, "MOVE")) {
						sender_source_distribution_mode_ = proton::source::distribution_mode::MOVE;
					}
					else {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("{} must be one of [UNSPECIFIED, COPY, MOVE]."),
							             KW_SOURCE_DISTRIBUTION_MODE)},
							{fmt::format(FMT_COMPILE("{}:{}:{}"),
							             KW_SENDER, KW_LINK_SOURCE, KW_SOURCE_DISTRIBUTION_MODE),
							 source_distribution_mode},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Unrecognized {}:{}:{} value."),
						                         KW_SENDER,
						                         KW_LINK_SOURCE,
						                         KW_SOURCE_DISTRIBUTION_MODE));
					}
				}

				const auto source_durability_mode_cfg = sender_source_cfg.find(KW_TERMINUS_DURABILITY_MODE);
				if (source_durability_mode_cfg == sender_source_cfg.end()) {
					sender_source_durability_mode_ = defaults::sender_source_durability_mode;
				}
				else if (!source_durability_mode_cfg->is_null()) {
					const std::string& source_durability_mode =
						source_durability_mode_cfg->get_ref<const std::string&>();

					if (boost::iequals(source_durability_mode, "NONDURABLE")) {
						sender_source_durability_mode_ = proton::source::durability_mode::NONDURABLE;
					}
					else if (boost::iequals(source_durability_mode, "CONFIGURATION")) {
						sender_source_durability_mode_ = proton::source::durability_mode::CONFIGURATION;
					}
					else if (boost::iequals(source_durability_mode, "UNSETTLED_STATE") ||
					         boost::iequals(source_durability_mode, "DELIVERIES"))
					{
						sender_source_durability_mode_ = proton::source::durability_mode::UNSETTLED_STATE;
					}
					else {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("{} must be one of "
							                         "[NONDURABLE, CONFIGURATION, UNSETTLED_STATE, DELIVERIES]."),
							             KW_TERMINUS_DURABILITY_MODE)},
							{fmt::format(FMT_COMPILE("{}:{}:{}"),
							             KW_SENDER, KW_LINK_SOURCE, KW_TERMINUS_DURABILITY_MODE),
							 source_durability_mode},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Unrecognized {}:{}:{} value."),
						                         KW_SENDER,
						                         KW_LINK_SOURCE,
						                         KW_TERMINUS_DURABILITY_MODE));
					}
				}

				const auto source_timeout_cfg = sender_source_cfg.find(KW_TERMINUS_TIMEOUT);
				if (source_timeout_cfg == sender_source_cfg.end()) {
					sender_source_timeout_ = defaults::sender_source_timeout;
				}
				else if (!source_timeout_cfg->is_null()) {
					const auto source_timeout = source_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
					if (source_timeout > std::numeric_limits<proton::duration::numeric_type>::max()) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("Sender source timeout must not exceed {}."),
							             std::numeric_limits<proton::duration::numeric_type>::max())},
							{fmt::format(FMT_COMPILE("{}:{}:{}"), KW_SENDER, KW_LINK_SOURCE, KW_TERMINUS_TIMEOUT),
							 std::to_string(source_timeout)},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Sender source timeout greater than {}."),
						                         std::numeric_limits<proton::duration::numeric_type>::max()));
					}
					sender_source_timeout_ = static_cast<proton::duration::numeric_type>(source_timeout);
				}

				const auto source_expiry_policy_cfg = sender_source_cfg.find(KW_TERMINUS_EXPIRY_POLICY);
				if (source_expiry_policy_cfg == sender_source_cfg.end()) {
					sender_source_expiry_policy_ = defaults::sender_source_expiry_policy;
				}
				else if (!source_expiry_policy_cfg->is_null()) {
					const std::string& source_expiry_policy = source_expiry_policy_cfg->get_ref<const std::string&>();

					if (boost::iequals(source_expiry_policy, "LINK_CLOSE")) {
						sender_source_expiry_policy_ = proton::source::expiry_policy::LINK_CLOSE;
					}
					else if (boost::iequals(source_expiry_policy, "SESSION_CLOSE")) {
						sender_source_expiry_policy_ = proton::source::expiry_policy::SESSION_CLOSE;
					}
					else if (boost::iequals(source_expiry_policy, "CONNECTION_CLOSE")) {
						sender_source_expiry_policy_ = proton::source::expiry_policy::CONNECTION_CLOSE;
					}
					else if (boost::iequals(source_expiry_policy, "NEVER")) {
						sender_source_expiry_policy_ = proton::source::expiry_policy::NEVER;
					}
					else {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("{} must be one of "
							                         "[LINK_CLOSE, SESSION_CLOSE, CONNECTION_CLOSE, NEVER]."),
							             KW_TERMINUS_EXPIRY_POLICY)},
							{fmt::format(FMT_COMPILE("{}:{}:{}"),
							             KW_SENDER, KW_LINK_SOURCE, KW_TERMINUS_EXPIRY_POLICY),
							 source_expiry_policy},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Unrecognized {}:{}:{} value."),
						                         KW_SENDER,
						                         KW_LINK_SOURCE,
						                         KW_TERMINUS_EXPIRY_POLICY));
					}
				}
			}

			const auto amqp_sender_target_cfg = sender_cfg.find(KW_LINK_TARGET);
			if ((amqp_sender_target_cfg == sender_cfg.end()) || amqp_sender_target_cfg->is_null()) {
				sender_target_address_ = defaults::sender_target_address;
				sender_target_dynamic_ = defaults::sender_target_dynamic;
				sender_target_anonymous_ = defaults::sender_target_anonymous;
				sender_target_durability_mode_ = defaults::sender_target_durability_mode;
				sender_target_timeout_ = defaults::sender_target_timeout;
				sender_target_expiry_policy_ = defaults::sender_target_expiry_policy;
			}
			else if (!amqp_sender_target_cfg->is_object()) {
				const std::string& errmsg =
					fmt::format(FMT_COMPILE("{}:{} is invalid type."), KW_SENDER, KW_LINK_TARGET);
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message", errmsg},
				});
				// clang-format on
				return ERROR(KEY_TYPE_MISMATCH, errmsg);
			}
			else {
				const auto& sender_target_cfg = *amqp_sender_target_cfg;

				const auto target_address_cfg = sender_target_cfg.find(KW_TERMINUS_ADDRESS);
				if (target_address_cfg == sender_target_cfg.end()) {
					sender_target_address_ = defaults::sender_target_address;
				}
				else if (!target_address_cfg->is_null()) {
					sender_target_address_ = target_address_cfg->get_ref<const std::string&>();
				}

				const auto target_dynamic_cfg = sender_target_cfg.find(KW_TERMINUS_DYNAMIC);
				if (target_dynamic_cfg == sender_target_cfg.end()) {
					sender_target_dynamic_ = defaults::sender_target_dynamic;
				}
				else if (!target_dynamic_cfg->is_null()) {
					sender_target_dynamic_ = target_dynamic_cfg->get<bool>();
				}

				const auto target_anonymous_cfg = sender_target_cfg.find(KW_TERMINUS_ANONYMOUS);
				if (target_anonymous_cfg == sender_target_cfg.end()) {
					sender_target_anonymous_ = defaults::sender_target_dynamic;
				}
				else if (!target_anonymous_cfg->is_null()) {
					sender_target_anonymous_ = target_anonymous_cfg->get<bool>();
				}

				const auto target_durability_mode_cfg = sender_target_cfg.find(KW_TERMINUS_DURABILITY_MODE);
				if (target_durability_mode_cfg == sender_target_cfg.end()) {
					sender_target_durability_mode_ = defaults::sender_target_durability_mode;
				}
				else if (!target_durability_mode_cfg->is_null()) {
					const std::string& target_durability_mode =
						target_durability_mode_cfg->get_ref<const std::string&>();

					if (boost::iequals(target_durability_mode, "NONDURABLE")) {
						sender_target_durability_mode_ = proton::target::durability_mode::NONDURABLE;
					}
					else if (boost::iequals(target_durability_mode, "CONFIGURATION")) {
						sender_target_durability_mode_ = proton::target::durability_mode::CONFIGURATION;
					}
					else if (boost::iequals(target_durability_mode, "UNSETTLED_STATE") ||
					         boost::iequals(target_durability_mode, "DELIVERIES"))
					{
						sender_target_durability_mode_ = proton::target::durability_mode::UNSETTLED_STATE;
					}
					else {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("{} must be one of "
							                         "[NONDURABLE, CONFIGURATION, UNSETTLED_STATE, DELIVERIES]."),
							             KW_TERMINUS_DURABILITY_MODE)},
							{fmt::format(FMT_COMPILE("{}:{}:{}"),
							             KW_SENDER, KW_LINK_TARGET, KW_TERMINUS_DURABILITY_MODE),
							 target_durability_mode},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Unrecognized {}:{}:{} value."),
						                         KW_SENDER,
						                         KW_LINK_TARGET,
						                         KW_TERMINUS_DURABILITY_MODE));
					}
				}

				const auto target_timeout_cfg = sender_target_cfg.find(KW_TERMINUS_TIMEOUT);
				if (target_timeout_cfg == sender_target_cfg.end()) {
					sender_target_timeout_ = defaults::sender_target_timeout;
				}
				else if (!target_timeout_cfg->is_null()) {
					const auto target_timeout = target_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
					if (target_timeout > std::numeric_limits<proton::duration::numeric_type>::max()) {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("Sender target timeout must not exceed {}."),
							             std::numeric_limits<proton::duration::numeric_type>::max())},
							{fmt::format(FMT_COMPILE("{}:{}:{}"), KW_SENDER, KW_LINK_TARGET, KW_TERMINUS_TIMEOUT),
							 std::to_string(target_timeout)},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Sender target timeout greater than {}."),
						                         std::numeric_limits<proton::duration::numeric_type>::max()));
					}
					sender_target_timeout_ = static_cast<proton::duration::numeric_type>(target_timeout);
				}

				const auto target_expiry_policy_cfg = sender_target_cfg.find(KW_TERMINUS_EXPIRY_POLICY);
				if (target_expiry_policy_cfg == sender_target_cfg.end()) {
					sender_target_expiry_policy_ = defaults::sender_target_expiry_policy;
				}
				else if (!target_expiry_policy_cfg->is_null()) {
					const std::string& target_expiry_policy = target_expiry_policy_cfg->get_ref<const std::string&>();

					if (boost::iequals(target_expiry_policy, "LINK_CLOSE")) {
						sender_target_expiry_policy_ = proton::target::expiry_policy::LINK_CLOSE;
					}
					else if (boost::iequals(target_expiry_policy, "SESSION_CLOSE")) {
						sender_target_expiry_policy_ = proton::target::expiry_policy::SESSION_CLOSE;
					}
					else if (boost::iequals(target_expiry_policy, "CONNECTION_CLOSE")) {
						sender_target_expiry_policy_ = proton::target::expiry_policy::CONNECTION_CLOSE;
					}
					else if (boost::iequals(target_expiry_policy, "NEVER")) {
						sender_target_expiry_policy_ = proton::target::expiry_policy::NEVER;
					}
					else {
						// clang-format off
						log_re::error({
							{"rule_engine_plugin", rule_engine_name},
							{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
							{"log_message",
							 fmt::format(FMT_COMPILE("{} must be one of "
							                         "[LINK_CLOSE, SESSION_CLOSE, CONNECTION_CLOSE, NEVER]."),
							             KW_TERMINUS_EXPIRY_POLICY)},
							{fmt::format(FMT_COMPILE("{}:{}:{}"),
							             KW_SENDER, KW_LINK_TARGET, KW_TERMINUS_EXPIRY_POLICY),
							 target_expiry_policy},
						});
						// clang-format on
						return ERROR(SYS_CONFIG_FILE_ERR,
						             fmt::format(FMT_COMPILE("Unrecognized {}:{}:{} value."),
						                         KW_SENDER,
						                         KW_LINK_TARGET,
						                         KW_TERMINUS_EXPIRY_POLICY));
					}
				}
			}
		}

		const auto amqp_durable_messages_cfg = _plugin_specific_configuration.find(KW_DURABLE_MESSAGES);
		if (amqp_durable_messages_cfg == _plugin_specific_configuration.end()) {
			durable_messages_ = defaults::durable_messages;
		}
		else if (amqp_durable_messages_cfg->is_string()) {
			const auto& amqp_durable_messages_str = amqp_durable_messages_cfg->get_ref<const std::string&>();
			durable_messages_ = boost::iequals(amqp_durable_messages_str, "true");
		}
		else if (!amqp_durable_messages_cfg->is_null()) {
			durable_messages_ = amqp_durable_messages_cfg->get<bool>();
		}

		const auto message_send_timeout_cfg = _plugin_specific_configuration.find(KW_MESSAGE_SEND_TIMEOUT);
		if (message_send_timeout_cfg == _plugin_specific_configuration.end()) {
			message_send_timeout_ = defaults::message_send_timeout;
		}
		else if (!message_send_timeout_cfg->is_null()) {
			const auto message_send_timeout = message_send_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (message_send_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Message send timeout must not exceed {}."),
					             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
					{KW_MESSAGE_SEND_TIMEOUT, std::to_string(message_send_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Message send timeout greater than {}."),
				                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
			}
			message_send_timeout_ =
				std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(message_send_timeout));
		}

		const auto session_open_timeout_cfg = _plugin_specific_configuration.find(KW_SESSION_OPEN_TIMEOUT);
		if (session_open_timeout_cfg == _plugin_specific_configuration.end()) {
			session_open_timeout_ = defaults::session_close_timeout;
		}
		else if (!session_open_timeout_cfg->is_null()) {
			const auto session_open_timeout = session_open_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (session_open_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Session open timeout must not exceed {}."),
					             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
					{KW_SESSION_OPEN_TIMEOUT, std::to_string(session_open_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Session open timeout greater than {}."),
				                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
			}
			session_open_timeout_ =
				std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(session_open_timeout));
		}

		const auto session_close_timeout_cfg = _plugin_specific_configuration.find(KW_SESSION_CLOSE_TIMEOUT);
		if (session_close_timeout_cfg == _plugin_specific_configuration.end()) {
			session_close_timeout_ = defaults::session_close_timeout;
		}
		else if (!session_close_timeout_cfg->is_null()) {
			const auto session_close_timeout = session_close_timeout_cfg->get<nlohmann::json::number_unsigned_t>();
			if (session_close_timeout > std::numeric_limits<std::chrono::milliseconds::rep>::max()) {
				// clang-format off
				log_re::error({
					{"rule_engine_plugin", rule_engine_name},
					{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
					{"log_message",
					 fmt::format(FMT_COMPILE("Session close timeout must not exceed {}."),
					             std::numeric_limits<std::chrono::milliseconds::rep>::max())},
					{KW_SESSION_CLOSE_TIMEOUT, std::to_string(session_close_timeout)}
				});
				// clang-format on
				return ERROR(SYS_CONFIG_FILE_ERR,
				             fmt::format(FMT_COMPILE("Session close timeout greater than {}."),
				                         std::numeric_limits<std::chrono::milliseconds::rep>::max()));
			}
			session_close_timeout_ =
				std::chrono::milliseconds(static_cast<std::chrono::milliseconds::rep>(session_close_timeout));
		}

		// look for amqp_options and log a warning if it is present
		const auto amqp_options_cfg = _plugin_specific_configuration.find(KW_DEPRECATED_OPTIONS);
		if (amqp_options_cfg != _plugin_specific_configuration.end()) {
			// clang-format off
			log_re::warn({
				{"rule_engine_plugin", rule_engine_name},
				{irods::KW_CFG_INSTANCE_NAME, _re_instance_name},
				{"log_message",
				 fmt::format(FMT_COMPILE("Found {} configuration setting. This setting is no longer used and should "
				                         "be removed from the plugin configuration."),
				             KW_DEPRECATED_OPTIONS)},
			});
			// clang-format on
		}

		is_initialized_ = true;
		return SUCCESS();
	}

	void amqp_config::configure_connection(proton::connection_options& _conn_opts, const std::string& _re_instance_name)
	{
		if (!failover_endpoints_.empty()) {
			_conn_opts.failover_urls(failover_endpoints_);
		}
		if (user_.has_value()) {
			_conn_opts.user(*user_);
		}
		if (password_.has_value()) {
			_conn_opts.password(*password_);
		}
		if (connection_max_frame_size_.has_value()) {
			_conn_opts.max_frame_size(*connection_max_frame_size_);
		}
		if (connection_max_sessions_.has_value()) {
			_conn_opts.max_sessions(*connection_max_sessions_);
		}
		if (connection_idle_timeout_.has_value()) {
			_conn_opts.idle_timeout(*connection_idle_timeout_);
		}
		if (connection_virtual_host_.has_value()) {
			_conn_opts.virtual_host(*connection_virtual_host_);
		}
		if (sasl_enabled_.has_value()) {
			_conn_opts.sasl_enabled(*sasl_enabled_);
		}
		if (sasl_mechanisms_.has_value()) {
			_conn_opts.sasl_allowed_mechs(*sasl_mechanisms_);
		}
		if (sasl_allow_insecure_.has_value()) {
			_conn_opts.sasl_allow_insecure_mechs(*sasl_allow_insecure_);
		}

		if (ssl_trust_db_.has_value()) {
			if (ssl_certdb_main_.has_value()) {
				if (ssl_certdb_extra_.has_value()) {
					if (ssl_cert_password_.has_value()) {
						const proton::ssl_certificate ssl_cert(
							*ssl_certdb_main_, *ssl_certdb_extra_, *ssl_cert_password_);
						if (ssl_verify_mode_.has_value()) {
							// set: verify_mode, trust_db, certdb_main, certdb_extra, cert_password
							_conn_opts.ssl_client_options(
								proton::ssl_client_options(ssl_cert, *ssl_trust_db_, *ssl_verify_mode_));
						}
						else {
							// set: trust_db, certdb_main, certdb_extra, cert_password
							// unset: verify_mode
							_conn_opts.ssl_client_options(proton::ssl_client_options(ssl_cert, *ssl_trust_db_));
						}
					}
					else {
						const proton::ssl_certificate ssl_cert(*ssl_certdb_main_, *ssl_certdb_extra_);
						if (ssl_verify_mode_.has_value()) {
							// set: verify_mode, trust_db, certdb_main, certdb_extra
							// unset: cert_password
							_conn_opts.ssl_client_options(
								proton::ssl_client_options(ssl_cert, *ssl_trust_db_, *ssl_verify_mode_));
						}
						else {
							// set: trust_db, certdb_main, certdb_extra
							// unset: verify_mode, cert_password
							_conn_opts.ssl_client_options(proton::ssl_client_options(ssl_cert, *ssl_trust_db_));
						}
					}
				}
				else {
					const proton::ssl_certificate ssl_cert(*ssl_certdb_main_);
					if (ssl_verify_mode_.has_value()) {
						// set: verify_mode, trust_db, certdb_main
						// unset: certdb_extra, cert_password
						_conn_opts.ssl_client_options(
							proton::ssl_client_options(ssl_cert, *ssl_trust_db_, *ssl_verify_mode_));
					}
					else {
						// set: trust_db, certdb_main
						// unset: verify_mode, certdb_extra, cert_password
						_conn_opts.ssl_client_options(proton::ssl_client_options(ssl_cert, *ssl_trust_db_));
					}
				}
			}
			else {
				if (ssl_verify_mode_.has_value()) {
					// set: verify_mode, trust_db
					// unset: certdb_main, certdb_extra, cert_password
					_conn_opts.ssl_client_options(proton::ssl_client_options(*ssl_trust_db_, *ssl_verify_mode_));
				}
				else {
					// set: trust_db
					// unset: verify_mode, certdb_extra, cert_password, certdb_main
					_conn_opts.ssl_client_options(proton::ssl_client_options(*ssl_trust_db_));
				}
			}
		}
		else if (ssl_verify_mode_.has_value()) {
			// set: verify_mode
			// unset: trust_db, certdb_main, certdb_extra, cert_password
			_conn_opts.ssl_client_options(proton::ssl_client_options(*ssl_verify_mode_));
		}

		proton::reconnect_options reconn_opts;
		if (reconnect_delay_.has_value()) {
			reconn_opts.delay(*reconnect_delay_);
		}
		if (reconnect_delay_multiplier_.has_value()) {
			reconn_opts.delay_multiplier(*reconnect_delay_multiplier_);
		}
		if (reconnect_max_delay_.has_value()) {
			reconn_opts.max_delay(*reconnect_max_delay_);
		}
		if (reconnect_max_attempts_.has_value()) {
			reconn_opts.max_attempts(*reconnect_max_attempts_);
		}
		_conn_opts.reconnect(reconn_opts);

		const std::map<proton::symbol, proton::value> conn_props{
			{"product", "iRODS"},
			{"version", IRODS_VERSION},
			{"version_integer", static_cast<std::uint64_t>(IRODS_VERSION_INTEGER)},
			{"rule_engine_plugin", rule_engine_name},
			{"rule_engine_plugin_version", IRODS_AUDIT_AMQP_VERSION},
			{"rule_engine_plugin_version_integer", IRODS_AUDIT_AMQP_VERSION_INTEGER},
			{"rule_engine_plugin_instance_name", _re_instance_name},
			{"platform", "C++/Qpid-Proton"},
			{"platform_version", IRODS_QPID_PROTON_VERSION},
			{"platform_version_integer", static_cast<std::uint64_t>(IRODS_QPID_PROTON_VERSION_INTEGER)},
		};
		_conn_opts.properties(conn_props);
	}

	void amqp_config::configure_sender(proton::sender_options& _sender_opts)
	{
		if (sender_delivery_mode_.has_value()) {
			_sender_opts.delivery_mode(*sender_delivery_mode_);
		}
		if (sender_auto_settle_.has_value()) {
			_sender_opts.auto_settle(*sender_auto_settle_);
		}

		proton::source_options source_opts;
		if (sender_source_address_.has_value()) {
			source_opts.address(*sender_source_address_);
		}
		if (sender_source_dynamic_.has_value()) {
			source_opts.dynamic(*sender_source_dynamic_);
		}
		if (sender_source_anonymous_.has_value()) {
			source_opts.anonymous(*sender_source_anonymous_);
		}
		if (sender_source_distribution_mode_.has_value()) {
			source_opts.distribution_mode(*sender_source_distribution_mode_);
		}
		if (sender_source_durability_mode_.has_value()) {
			source_opts.durability_mode(*sender_source_durability_mode_);
		}
		if (sender_source_timeout_.has_value()) {
			source_opts.timeout(*sender_source_timeout_);
		}
		if (sender_source_expiry_policy_.has_value()) {
			source_opts.expiry_policy(*sender_source_expiry_policy_);
		}
		_sender_opts.source(source_opts);

		proton::target_options target_opts;
		if (sender_target_address_.has_value()) {
			target_opts.address(*sender_target_address_);
		}
		if (sender_target_dynamic_.has_value()) {
			target_opts.dynamic(*sender_target_dynamic_);
		}
		if (sender_target_anonymous_.has_value()) {
			target_opts.anonymous(*sender_target_anonymous_);
		}
		if (sender_target_durability_mode_.has_value()) {
			target_opts.durability_mode(*sender_target_durability_mode_);
		}
		if (sender_target_timeout_.has_value()) {
			target_opts.timeout(*sender_target_timeout_);
		}
		if (sender_target_expiry_policy_.has_value()) {
			target_opts.expiry_policy(*sender_target_expiry_policy_);
		}
		_sender_opts.target(target_opts);
	}
} //namespace irods::plugin::rule_engine::audit_amqp
