#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_CPP17_OPTIONAL
#include <catch2/catch_all.hpp>

#include "irods/private/audit_amqp.hpp"
#include "irods/private/amqp_config.hpp"
#include "irods/private/audit_config.hpp"

#include <irods/irods_configuration_keywords.hpp>
#include <irods/irods_error.hpp>
#include <irods/irods_logger.hpp>
#include <irods/irods_server_properties.hpp>
#include <irods/rodsErrorTable.h>

#include <fmt/format.h>
#include <fmt/compile.h>

#include <nlohmann/json.hpp>

#include <proton/delivery_mode.hpp>
#include <proton/duration.hpp>
#include <proton/ssl.hpp>
#include <proton/source.hpp>
#include <proton/target.hpp>
#include <proton/terminus.hpp>

#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <limits>

static constexpr const char* const re_instance_name = "irods_rule_engine_plugin-audit_amqp-instance";
static constexpr const std::uint32_t too_big_for_uint16 =
	static_cast<std::uint32_t>(std::numeric_limits<std::uint16_t>::max()) + 2U;
static constexpr const std::uint64_t too_big_for_uint32 =
	static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 2U;
static constexpr const std::uint64_t too_big_for_int64 =
	static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()) + 2U;
static constexpr const std::uint64_t too_big_for_ms =
	static_cast<std::uint64_t>(std::numeric_limits<std::chrono::milliseconds::rep>::max()) + 2U;

namespace audit_amqp = irods::plugin::rule_engine::audit_amqp;

const nlohmann::json server_config_init = {
	// clang-format off
	{irods::KW_CFG_LOG_LEVEL, {
		{irods::KW_CFG_LOG_LEVEL_CATEGORY_AGENT, "info"},
		{irods::KW_CFG_LOG_LEVEL_CATEGORY_AGENT_FACTORY, "info"},
		{irods::KW_CFG_LOG_LEVEL_CATEGORY_RULE_ENGINE, "trace"},
	}},
	{irods::KW_CFG_PLUGIN_CONFIGURATION, {
		{irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE, {
			{
				{irods::KW_CFG_INSTANCE_NAME, "irods_rule_engine_plugin-irods_rule_language-instance"},
				{irods::KW_CFG_PLUGIN_NAME, "irods_rule_engine_plugin-irods_rule_language"},
				{irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION, nlohmann::json::object()},
			},
			{
				{irods::KW_CFG_INSTANCE_NAME, re_instance_name},
				{irods::KW_CFG_PLUGIN_NAME, "irods_rule_engine_plugin-audit_amqp"},
				{irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION, {
					{audit_amqp::plugin_config::KW_FAILSAFE_MODE, "BLOCK_OPERATION"},
					{audit_amqp::plugin_config::KW_PEP_REGEX, "pep_.+"},
					{audit_amqp::amqp_config::KW_ENDPOINTS, {
						{
							{audit_amqp::amqp_config::KW_ENDPOINT_SCHEME, nullptr},
							{audit_amqp::amqp_config::KW_ENDPOINT_HOST, "localhost"},
							{audit_amqp::amqp_config::KW_ENDPOINT_PORT, 5672},
							{audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS, nullptr},
							{audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT, nullptr},
						}
					}},
					{audit_amqp::amqp_config::KW_USER, "a_username"},
					{audit_amqp::amqp_config::KW_PASSWORD, "the_password"},
					{audit_amqp::amqp_config::KW_PATH, audit_amqp::amqp_config::defaults::path},
					{audit_amqp::amqp_config::KW_PATH_PARAMETERS, nullptr},
					{audit_amqp::amqp_config::KW_PATH_FRAGMENT, nullptr},
					{audit_amqp::amqp_config::KW_SSL, {
						{audit_amqp::amqp_config::KW_SSL_VERIFY_MODE, nullptr},
						{audit_amqp::amqp_config::KW_SSL_TRUST_DB, nullptr},
						{audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN, nullptr},
						{audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA, nullptr},
						{audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD, nullptr},
					}},
					{audit_amqp::amqp_config::KW_SASL, {
						{audit_amqp::amqp_config::KW_SASL_ENABLE, nullptr},
						{audit_amqp::amqp_config::KW_SASL_MECHANISMS, nullptr},
						{audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE, nullptr},
					}},
					{audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE, nullptr},
					{audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS, nullptr},
					{audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT, nullptr},
					{audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST, nullptr},
					{audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT, nullptr},
					{audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT, nullptr},
					{audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY, nullptr},
					{audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER, nullptr},
					{audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY, nullptr},
					{audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS, nullptr},
					{audit_amqp::amqp_config::KW_SENDER, {
						{audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE, nullptr},
						{audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE, nullptr},
						{audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT, nullptr},
						{audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT, nullptr},
						{audit_amqp::amqp_config::KW_LINK_SOURCE, {
							{audit_amqp::amqp_config::KW_TERMINUS_ADDRESS, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS, nullptr},
							{audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY, nullptr},
						}},
						{audit_amqp::amqp_config::KW_LINK_TARGET, {
							{audit_amqp::amqp_config::KW_TERMINUS_ADDRESS, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT, nullptr},
							{audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY, nullptr},
						}},
					}},
					{audit_amqp::amqp_config::KW_DURABLE_MESSAGES, nullptr},
					{audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT, nullptr},
					{audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT, nullptr},
					{audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT, nullptr},
				}},
			},
			{
				{irods::KW_CFG_INSTANCE_NAME, "irods_rule_engine_plugin-cpp_default_policy-instance"},
				{irods::KW_CFG_PLUGIN_NAME, "irods_rule_engine_plugin-cpp_default_policy"},
				{irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION, nlohmann::json::object()},
			}
		}}
	}}
	// clang-format on
};

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
TEST_CASE("basic configuration loading")
{
	irods::experimental::log::init(getpid(), false, false);

	nlohmann::json server_config = server_config_init;

	auto& plugin_config_json =
		server_config[irods::KW_CFG_PLUGIN_CONFIGURATION][irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE][1];
	auto& psc_json = plugin_config_json[irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION];

	audit_amqp::plugin_config plugin_config;

	SECTION(fmt::format(FMT_COMPILE("{} absent"), irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION))
	{
		plugin_config_json.erase(plugin_config_json.find(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION))
	{
		plugin_config_json[irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION))
	{
		plugin_config_json[irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION] = "psc";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION))
	{
		plugin_config_json[irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION] = nlohmann::json::object();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json.erase(psc_json.find(audit_amqp::plugin_config::KW_FAILSAFE_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::defaults::failsafe_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::defaults::failsafe_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = -123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::defaults::failsafe_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{} BLOCK_OPERATION"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "BLOCK_OPERATION";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::BLOCK_OPERATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{} ALLOW_OPERATION"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "ALLOW_OPERATION";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{} BLOCK_OPERATION+bad_config"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "BLOCK_OPERATION";
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::BLOCK_OPERATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{} ALLOW_OPERATION+bad_config"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "ALLOW_OPERATION";
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_value"), audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "bad_value";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.failsafe_mode() == audit_amqp::plugin_config::defaults::failsafe_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json.erase(psc_json.find(audit_amqp::plugin_config::KW_PEP_REGEX));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		CHECK_FALSE(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		CHECK_FALSE(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = -123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		CHECK_FALSE(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = "audit_.+";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated+bad_config"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = "audit_.+";
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_value"), audit_amqp::plugin_config::KW_PEP_REGEX))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = "+";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == INVALID_REGEXP);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
		CHECK_FALSE(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_ENDPOINTS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nlohmann::json::array();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = "localhost";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_HOST))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint.erase(endpoint.find(audit_amqp::amqp_config::KW_ENDPOINT_HOST));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}[0]:{} null"), audit_amqp::amqp_config::KW_ENDPOINTS, audit_amqp::amqp_config::KW_ENDPOINT_HOST))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_HOST]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_HOST))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_HOST]
		        = nlohmann::json::object();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_HOST))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_HOST]
		        = "127.0.0.1";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "127.0.0.1:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_SCHEME))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint.erase(endpoint.find(audit_amqp::amqp_config::KW_ENDPOINT_SCHEME));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_SCHEME))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_SCHEME]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_SCHEME))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_SCHEME]
		        = -123;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_SCHEME))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_SCHEME]
		        = "amqp2";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "amqp2://localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint.erase(endpoint.find(audit_amqp::amqp_config::KW_ENDPOINT_PORT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}[0]:{} null"), audit_amqp::amqp_config::KW_ENDPOINTS, audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PORT]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PORT]
		        = "5672";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PORT]
		        = 100;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:100");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} below_range"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PORT]
		        = -10;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} above_range"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PORT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PORT]
		        = too_big_for_uint16;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint.erase(endpoint.find(audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = nlohmann::json::object();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = "?param1=value1";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nullptr}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", ""}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nlohmann::json::object()}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:good"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", "value1"}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=value1");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:null+null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nullptr}, {"param2", nullptr}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:null+empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nullptr}, {"param2", ""}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:null+bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nullptr}, {"param2", nlohmann::json::array()}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:null+good"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", nullptr}, {"param2", "value2"}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:empty+null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", ""}, {"param2", nullptr}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:empty+empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", ""}, {"param2", ""}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:empty+bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", ""}, {"param2", false}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:empty+good"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", ""}, {"param2", "value2"}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:bad_type+null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", -100}, {"param2", nullptr}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:bad_type+empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", 100}, {"param2", ""}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:bad_type+bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", true}, {"param2", false}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:bad_type+good"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", 0.5f}, {"param2", "value2"}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:good+null"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", "value1"}, {"param2", nullptr}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=value1&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:good+empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", "value1"}, {"param2", ""}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=value1&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:good+bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", "value1"}, {"param2", -0.0f}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated:good+good"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS]
		        = {{"param1", "value1"}, {"param2", "value2"}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=value1&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint.erase(endpoint.find(audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} empty"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT]
		        = "";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672#");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} bad_type"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT]
		        = 123;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{} populated"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0]
		        [audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT]
		        = "frag";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672#frag");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[0]:{}+{} populated"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT))
	{
		auto& endpoint = psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][0];
		endpoint[audit_amqp::amqp_config::KW_ENDPOINT_PARAMETERS] = {{"param1", "value1"}};
		endpoint[audit_amqp::amqp_config::KW_ENDPOINT_FRAGMENT] = "frag";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().primary_endpoint() == "localhost:5672/?param1=value1#frag");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[1] populated"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][1]
		        = {{"scheme", "amqps"}, {"host", "localhost"}, {"port", 1234}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().failover_endpoints()[0] == "amqps://localhost:1234");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[2] populated"), audit_amqp::amqp_config::KW_ENDPOINTS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][1]
		        = {{"scheme", "amqps"}, {"host", "localhost"}, {"port", 1234}};
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][2]
		        = {{"scheme", "amqp" }, {"host", "127.0.0.1"}, {"port", 5001}};
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().failover_endpoints()[0] == "amqps://localhost:1234");
		CHECK(plugin_config.amqp_config().failover_endpoints()[1] == "amqp://127.0.0.1:5001");
	}

	SECTION(fmt::format(FMT_COMPILE("{}[1]:{} absent"),
	                    audit_amqp::amqp_config::KW_ENDPOINTS,
	                    audit_amqp::amqp_config::KW_ENDPOINT_HOST))
	{
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS][1] = {{"scheme", "amqps"}, {"port", 1234}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_USER))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_USER));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().user() == audit_amqp::amqp_config::defaults::user);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_USER))
	{
		psc_json[audit_amqp::amqp_config::KW_USER] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().user().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_USER))
	{
		psc_json[audit_amqp::amqp_config::KW_USER] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().user().has_value());
		REQUIRE(plugin_config.amqp_config().user().value() == "");
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_USER))
	{
		psc_json[audit_amqp::amqp_config::KW_USER] = 123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_USER))
	{
		psc_json[audit_amqp::amqp_config::KW_USER] = "username";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().user().has_value());
		REQUIRE(plugin_config.amqp_config().user().value() == "username");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_PASSWORD))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_PASSWORD));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().password() == audit_amqp::amqp_config::defaults::password);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_PASSWORD))
	{
		psc_json[audit_amqp::amqp_config::KW_PASSWORD] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().password().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_PASSWORD))
	{
		psc_json[audit_amqp::amqp_config::KW_PASSWORD] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().password().has_value());
		REQUIRE(plugin_config.amqp_config().password().value() == "");
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_PASSWORD))
	{
		psc_json[audit_amqp::amqp_config::KW_PASSWORD] = 10.5f;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_PASSWORD))
	{
		psc_json[audit_amqp::amqp_config::KW_PASSWORD] = "asdf1234ASDF!@#$";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().password().has_value());
		REQUIRE(plugin_config.amqp_config().password().value() == "asdf1234ASDF!@#$");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_PATH))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_PATH));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_PATH))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_NOT_FOUND);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_PATH))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "");
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_PATH))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = 123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_PATH))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_PATH_PARAMETERS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = nlohmann::json::object();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = "?param1=value1";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nullptr}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", ""}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", 1234}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:good"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", "value1"}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=value1");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:null+null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nullptr}, {"param2", nullptr}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:null+empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nullptr}, {"param2", ""}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:null+bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nullptr}, {"param2", -100}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:null+good"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nullptr}, {"param2", "value2"}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:empty+null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", ""}, {"param2", nullptr}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:empty+empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", ""}, {"param2", ""}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:empty+bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", ""}, {"param2", true}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:empty+good"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", ""}, {"param2", "value2"}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:bad_type+null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", false}, {"param2", nullptr}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:bad_type+empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", 1234}, {"param2", ""}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:bad_type+bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", nlohmann::json::array()}, {"param2", -100}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:bad_type+good"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", 6.7f}, {"param2", "value2"}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:good+null"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", "value1"}, {"param2", nullptr}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=value1&param2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:good+empty"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", "value1"}, {"param2", ""}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=value1&param2=");
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:good+bad_type"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] =
			{{"param1", "value1"}, {"param2", nlohmann::json::object()}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated:good+good"), audit_amqp::amqp_config::KW_PATH_PARAMETERS))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", "value1"}, {"param2", "value2"}};
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=value1&param2=value2");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_PATH_FRAGMENT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_FRAGMENT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages");
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_FRAGMENT] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages#");
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_FRAGMENT] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_FRAGMENT] = "frag";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages#frag");
	}

	SECTION(fmt::format(FMT_COMPILE("{}+{} populated"),
	                    audit_amqp::amqp_config::KW_PATH_PARAMETERS,
	                    audit_amqp::amqp_config::KW_PATH_FRAGMENT))
	{
		psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/irods_audit_messages";
		psc_json[audit_amqp::amqp_config::KW_PATH_PARAMETERS] = {{"param1", "value1"}};
		psc_json[audit_amqp::amqp_config::KW_PATH_FRAGMENT] = "frag";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().path() == "/queues/irods_audit_messages?param1=value1#frag");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_SSL))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_SSL));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_verify_mode() == audit_amqp::amqp_config::defaults::ssl_verify_mode);
		CHECK(plugin_config.amqp_config().ssl_trust_db() == audit_amqp::amqp_config::defaults::ssl_trust_db);
		CHECK(plugin_config.amqp_config().ssl_certdb_main() == audit_amqp::amqp_config::defaults::ssl_certdb_main);
		CHECK(plugin_config.amqp_config().ssl_certdb_extra() == audit_amqp::amqp_config::defaults::ssl_certdb_extra);
		CHECK(plugin_config.amqp_config().ssl_cert_password() == audit_amqp::amqp_config::defaults::ssl_cert_password);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_SSL))
	{
		psc_json[audit_amqp::amqp_config::KW_SSL] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_verify_mode() == audit_amqp::amqp_config::defaults::ssl_verify_mode);
		CHECK(plugin_config.amqp_config().ssl_trust_db() == audit_amqp::amqp_config::defaults::ssl_trust_db);
		CHECK(plugin_config.amqp_config().ssl_certdb_main() == audit_amqp::amqp_config::defaults::ssl_certdb_main);
		CHECK(plugin_config.amqp_config().ssl_certdb_extra() == audit_amqp::amqp_config::defaults::ssl_certdb_extra);
		CHECK(plugin_config.amqp_config().ssl_cert_password() == audit_amqp::amqp_config::defaults::ssl_cert_password);
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_SSL))
	{
		psc_json[audit_amqp::amqp_config::KW_SSL] = nlohmann::json::object();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_verify_mode() == audit_amqp::amqp_config::defaults::ssl_verify_mode);
		CHECK(plugin_config.amqp_config().ssl_trust_db() == audit_amqp::amqp_config::defaults::ssl_trust_db);
		CHECK(plugin_config.amqp_config().ssl_certdb_main() == audit_amqp::amqp_config::defaults::ssl_certdb_main);
		CHECK(plugin_config.amqp_config().ssl_certdb_extra() == audit_amqp::amqp_config::defaults::ssl_certdb_extra);
		CHECK(plugin_config.amqp_config().ssl_cert_password() == audit_amqp::amqp_config::defaults::ssl_cert_password);
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_SSL))
	{
		psc_json[audit_amqp::amqp_config::KW_SSL] = "asdf";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl.erase(ssl.find(audit_amqp::amqp_config::KW_SSL_VERIFY_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_verify_mode() == audit_amqp::amqp_config::defaults::ssl_verify_mode);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().ssl_verify_mode().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = "";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = -123;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} VERIFY_PEER"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = "VERIFY_PEER";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_verify_mode() == proton::ssl::verify_mode::VERIFY_PEER);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} ANONYMOUS_PEER"),
	                    audit_amqp::amqp_config::KW_SSL,
	                    audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = "ANONYMOUS_PEER";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_verify_mode() == proton::ssl::verify_mode::ANONYMOUS_PEER);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} VERIFY_PEER_NAME"),
	                    audit_amqp::amqp_config::KW_SSL,
	                    audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = "VERIFY_PEER_NAME";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_verify_mode() == proton::ssl::verify_mode::VERIFY_PEER_NAME);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_value"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_VERIFY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_VERIFY_MODE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_TRUST_DB))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl.erase(ssl.find(audit_amqp::amqp_config::KW_SSL_TRUST_DB));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_trust_db() == audit_amqp::amqp_config::defaults::ssl_trust_db);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_TRUST_DB))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_TRUST_DB]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().ssl_trust_db().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_TRUST_DB))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_TRUST_DB]
		        = "";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_trust_db().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_trust_db() == "");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_TRUST_DB))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_TRUST_DB]
		        = -123;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} populated"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_TRUST_DB))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SSL]
		        [audit_amqp::amqp_config::KW_SSL_TRUST_DB]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().ssl_trust_db().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl.erase(ssl.find(audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_main() == audit_amqp::amqp_config::defaults::ssl_certdb_main);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		REQUIRE_FALSE(plugin_config.amqp_config().ssl_certdb_main().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_main().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_certdb_main().value() == "");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = -123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} populated"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_main().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl.erase(ssl.find(audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_extra() == audit_amqp::amqp_config::defaults::ssl_certdb_extra);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		REQUIRE_FALSE(plugin_config.amqp_config().ssl_certdb_extra().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_certdb_extra().value() == "");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = -123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} populated"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		REQUIRE(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_certdb_extra().value() == "zxcv");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		ssl.erase(ssl.find(audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		if (plugin_config.amqp_config().ssl_certdb_extra().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_extra().value() == "zxcv");
		}
		REQUIRE(plugin_config.amqp_config().ssl_cert_password() ==
		        audit_amqp::amqp_config::defaults::ssl_cert_password);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		ssl[audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		if (plugin_config.amqp_config().ssl_certdb_extra().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_extra().value() == "zxcv");
		}
		REQUIRE_FALSE(plugin_config.amqp_config().ssl_cert_password().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		ssl[audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD] = "";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		if (plugin_config.amqp_config().ssl_certdb_extra().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_extra().value() == "zxcv");
		}
		REQUIRE(plugin_config.amqp_config().ssl_cert_password().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_cert_password().value() == "");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		ssl[audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD] = -123;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} populated"), audit_amqp::amqp_config::KW_SSL, audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD))
	{
		auto& ssl = psc_json[audit_amqp::amqp_config::KW_SSL];
		ssl[audit_amqp::amqp_config::KW_SSL_TRUST_DB] = "asdf";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_MAIN] = "qwer";
		ssl[audit_amqp::amqp_config::KW_SSL_CERTDB_EXTRA] = "zxcv";
		ssl[audit_amqp::amqp_config::KW_SSL_CERT_PASSWORD] = "a1b2c3";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().ssl_trust_db().has_value());
		if (plugin_config.amqp_config().ssl_trust_db().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_trust_db().value() == "asdf");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_main().has_value());
		if (plugin_config.amqp_config().ssl_certdb_main().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_main().value() == "qwer");
		}
		CHECK(plugin_config.amqp_config().ssl_certdb_extra().has_value());
		if (plugin_config.amqp_config().ssl_certdb_extra().has_value()) {
			CHECK(plugin_config.amqp_config().ssl_certdb_extra().value() == "zxcv");
		}
		REQUIRE(plugin_config.amqp_config().ssl_cert_password().has_value());
		REQUIRE(plugin_config.amqp_config().ssl_cert_password().value() == "a1b2c3");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_SASL))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_SASL));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sasl_enabled() == audit_amqp::amqp_config::defaults::sasl_enabled);
		CHECK(plugin_config.amqp_config().sasl_mechanisms() == audit_amqp::amqp_config::defaults::sasl_mechanisms);
		CHECK(plugin_config.amqp_config().sasl_allow_insecure() ==
		      audit_amqp::amqp_config::defaults::sasl_allow_insecure);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_SASL))
	{
		psc_json[audit_amqp::amqp_config::KW_SASL] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sasl_enabled() == audit_amqp::amqp_config::defaults::sasl_enabled);
		CHECK(plugin_config.amqp_config().sasl_mechanisms() == audit_amqp::amqp_config::defaults::sasl_mechanisms);
		CHECK(plugin_config.amqp_config().sasl_allow_insecure() ==
		      audit_amqp::amqp_config::defaults::sasl_allow_insecure);
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_SASL))
	{
		psc_json[audit_amqp::amqp_config::KW_SASL] = nlohmann::json::object();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sasl_enabled() == audit_amqp::amqp_config::defaults::sasl_enabled);
		CHECK(plugin_config.amqp_config().sasl_mechanisms() == audit_amqp::amqp_config::defaults::sasl_mechanisms);
		CHECK(plugin_config.amqp_config().sasl_allow_insecure() ==
		      audit_amqp::amqp_config::defaults::sasl_allow_insecure);
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_SASL))
	{
		psc_json[audit_amqp::amqp_config::KW_SASL] = "asdf";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ENABLE))
	{
		auto& sasl = psc_json[audit_amqp::amqp_config::KW_SASL];
		sasl.erase(sasl.find(audit_amqp::amqp_config::KW_SASL_ENABLE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_enabled() == audit_amqp::amqp_config::defaults::sasl_enabled);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ENABLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ENABLE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sasl_enabled().has_value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ENABLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ENABLE]
		        = "1234";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} true"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ENABLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ENABLE]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_enabled().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_enabled().value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} false"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ENABLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ENABLE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_enabled().has_value());
		REQUIRE_FALSE(plugin_config.amqp_config().sasl_enabled().value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		auto& sasl = psc_json[audit_amqp::amqp_config::KW_SASL];
		sasl.erase(sasl.find(audit_amqp::amqp_config::KW_SASL_MECHANISMS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms() == audit_amqp::amqp_config::defaults::sasl_mechanisms);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sasl_mechanisms().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} empty_string"),
	                    audit_amqp::amqp_config::KW_SASL,
	                    audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = "";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms() == "");
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} empty_array"),
	                    audit_amqp::amqp_config::KW_SASL,
	                    audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = nlohmann::json::array();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms() == "");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = nlohmann::json::object();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} populated_string"),
	                    audit_amqp::amqp_config::KW_SASL,
	                    audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = "PLAIN ANONYMOUS";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms() == "PLAIN ANONYMOUS");
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} populated_array"),
	                    audit_amqp::amqp_config::KW_SASL,
	                    audit_amqp::amqp_config::KW_SASL_MECHANISMS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_MECHANISMS]
		        = nlohmann::json::array({"PLAIN", "ANONYMOUS"});
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_mechanisms() == "PLAIN ANONYMOUS");
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE))
	{
		auto& sasl = psc_json[audit_amqp::amqp_config::KW_SASL];
		sasl.erase(sasl.find(audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_allow_insecure() ==
		        audit_amqp::amqp_config::defaults::sasl_allow_insecure);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sasl_allow_insecure().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SASL,
	                    audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE]
		        = "1234";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} true"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_allow_insecure().has_value());
		REQUIRE(plugin_config.amqp_config().sasl_allow_insecure().value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} false"), audit_amqp::amqp_config::KW_SASL, audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SASL]
		        [audit_amqp::amqp_config::KW_SASL_ALLOW_INSECURE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sasl_allow_insecure().has_value());
		REQUIRE_FALSE(plugin_config.amqp_config().sasl_allow_insecure().value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_max_frame_size() ==
		        audit_amqp::amqp_config::defaults::connection_max_frame_size);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().connection_max_frame_size().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE] = 8192;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_max_frame_size().has_value());
		REQUIRE(plugin_config.amqp_config().connection_max_frame_size().value() == 8192);
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_FRAME_SIZE] = too_big_for_uint32;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_max_sessions() ==
		        audit_amqp::amqp_config::defaults::connection_max_sessions);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().connection_max_sessions().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS] = 8192;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_max_sessions().has_value());
		REQUIRE(plugin_config.amqp_config().connection_max_sessions().value() == 8192);
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_MAX_SESSIONS] = too_big_for_uint16;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_idle_timeout() ==
		        audit_amqp::amqp_config::defaults::connection_idle_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().connection_idle_timeout().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_idle_timeout().has_value());
		REQUIRE(plugin_config.amqp_config().connection_idle_timeout().value() == proton::duration(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_IDLE_TIMEOUT] = too_big_for_int64;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_virtual_host() ==
		        audit_amqp::amqp_config::defaults::connection_virtual_host);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().connection_virtual_host().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_VIRTUAL_HOST] = "a.test.host";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_virtual_host().has_value());
		REQUIRE(plugin_config.amqp_config().connection_virtual_host().value() == "a.test.host");
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_open_timeout() ==
		        audit_amqp::amqp_config::defaults::connection_open_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_open_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_open_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_OPEN_TIMEOUT] = too_big_for_ms;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_close_timeout() ==
		        audit_amqp::amqp_config::defaults::connection_close_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_close_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().connection_close_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_CONNECTION_CLOSE_TIMEOUT] = too_big_for_ms;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_delay() == audit_amqp::amqp_config::defaults::reconnect_delay);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().reconnect_delay().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY] = 10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_delay().has_value());
		REQUIRE(plugin_config.amqp_config().reconnect_delay().value() == proton::duration(10));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_BASE_DELAY] = too_big_for_int64;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_delay_multiplier() ==
		        audit_amqp::amqp_config::defaults::reconnect_delay_multiplier);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().reconnect_delay_multiplier().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_DELAY_MULTIPLIER] = 1.5f;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_delay_multiplier().has_value());
		REQUIRE(plugin_config.amqp_config().reconnect_delay_multiplier().value() == 1.5f);
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_max_delay() ==
		        audit_amqp::amqp_config::defaults::reconnect_max_delay);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().reconnect_max_delay().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY] = 16000;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_max_delay().has_value());
		REQUIRE(plugin_config.amqp_config().reconnect_max_delay().value() == proton::duration(16000));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_DELAY] = too_big_for_int64;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_max_attempts() ==
		        audit_amqp::amqp_config::defaults::reconnect_max_attempts);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().reconnect_max_attempts().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS] = 100;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().reconnect_max_attempts().has_value());
		REQUIRE(plugin_config.amqp_config().reconnect_max_attempts().value() == 100);
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS))
	{
		psc_json[audit_amqp::amqp_config::KW_RECONNECT_MAX_ATTEMPTS] = too_big_for_int64;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_SENDER))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_SENDER));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_delivery_mode() ==
		      audit_amqp::amqp_config::defaults::sender_delivery_mode);
		CHECK(plugin_config.amqp_config().sender_auto_settle() ==
		      audit_amqp::amqp_config::defaults::sender_auto_settle);
		CHECK(plugin_config.amqp_config().sender_open_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_open_timeout);
		CHECK(plugin_config.amqp_config().sender_close_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_close_timeout);
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_SENDER))
	{
		psc_json[audit_amqp::amqp_config::KW_SENDER] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_delivery_mode() ==
		      audit_amqp::amqp_config::defaults::sender_delivery_mode);
		CHECK(plugin_config.amqp_config().sender_auto_settle() ==
		      audit_amqp::amqp_config::defaults::sender_auto_settle);
		CHECK(plugin_config.amqp_config().sender_open_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_open_timeout);
		CHECK(plugin_config.amqp_config().sender_close_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_close_timeout);
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(FMT_COMPILE("{} empty"), audit_amqp::amqp_config::KW_SENDER))
	{
		psc_json[audit_amqp::amqp_config::KW_SENDER] = nlohmann::json::object();
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_delivery_mode() ==
		      audit_amqp::amqp_config::defaults::sender_delivery_mode);
		CHECK(plugin_config.amqp_config().sender_auto_settle() ==
		      audit_amqp::amqp_config::defaults::sender_auto_settle);
		CHECK(plugin_config.amqp_config().sender_open_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_open_timeout);
		CHECK(plugin_config.amqp_config().sender_close_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_close_timeout);
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_SENDER))
	{
		psc_json[audit_amqp::amqp_config::KW_SENDER] = "asdf";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode() ==
		        audit_amqp::amqp_config::defaults::sender_delivery_mode);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_delivery_mode().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} NONE"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = "NONE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().value() == proton::delivery_mode::modes::NONE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} AT_MOST_ONCE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = "AT_MOST_ONCE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().value() ==
		        proton::delivery_mode::modes::AT_MOST_ONCE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} AT_LEAST_ONCE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = "AT_LEAST_ONCE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_delivery_mode().value() ==
		        proton::delivery_mode::modes::AT_LEAST_ONCE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_DELIVERY_MODE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_auto_settle() ==
		        audit_amqp::amqp_config::defaults::sender_auto_settle);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_auto_settle().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE]
		        = "1234";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} true"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_auto_settle().has_value());
		REQUIRE(plugin_config.amqp_config().sender_auto_settle().value());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} false"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_AUTO_SETTLE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_auto_settle().has_value());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_auto_settle().value());
	}

		SECTION(fmt::format(FMT_COMPILE("{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_open_timeout() ==
		        audit_amqp::amqp_config::defaults::sender_open_timeout);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_open_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT]
		        = "1234";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT]
		        = 600;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_open_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} below_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT]
		        = -10;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} above_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_OPEN_TIMEOUT]
		        = too_big_for_ms;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_close_timeout() ==
		        audit_amqp::amqp_config::defaults::sender_close_timeout);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_close_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT]
		        = "1234";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT]
		        = 600;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_close_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} below_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT]
		        = -10;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{} above_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_CLOSE_TIMEOUT]
		        = too_big_for_ms;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_SOURCE))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_SOURCE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_SOURCE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_SOURCE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        = nlohmann::json::object();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_source_address() ==
		      audit_amqp::amqp_config::defaults::sender_source_address);
		CHECK(plugin_config.amqp_config().sender_source_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_source_dynamic);
		CHECK(plugin_config.amqp_config().sender_source_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_source_anonymous);
		CHECK(plugin_config.amqp_config().sender_source_distribution_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
		CHECK(plugin_config.amqp_config().sender_source_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_source_durability_mode);
		CHECK(plugin_config.amqp_config().sender_source_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_source_timeout);
		CHECK(plugin_config.amqp_config().sender_source_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_SOURCE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_ADDRESS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_address() ==
		        audit_amqp::amqp_config::defaults::sender_source_address);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_address().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = "a.test.host";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_address().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_address().value() == "a.test.host");
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_dynamic() ==
		        audit_amqp::amqp_config::defaults::sender_source_dynamic);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_dynamic().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} true"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_dynamic().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_dynamic().value() == true);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} false"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_dynamic().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_dynamic().value() == false);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_anonymous() ==
		        audit_amqp::amqp_config::defaults::sender_source_anonymous);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_anonymous().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} true"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_anonymous().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_anonymous().value() == true);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} false"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_anonymous().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_anonymous().value() == false);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode() ==
		        audit_amqp::amqp_config::defaults::sender_source_distribution_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_distribution_mode().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} UNSPECIFIED"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = "UNSPECIFIED";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().value() ==
		        proton::source::distribution_mode::UNSPECIFIED);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} COPY"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = "COPY";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().value() ==
		        proton::source::distribution_mode::COPY);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} MOVE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = "MOVE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_distribution_mode().value() ==
		        proton::source::distribution_mode::MOVE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_SOURCE_DISTRIBUTION_MODE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode() ==
		        audit_amqp::amqp_config::defaults::sender_source_durability_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_durability_mode().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} NONDURABLE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "NONDURABLE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().value() ==
		        proton::source::durability_mode::NONDURABLE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} CONFIGURATION"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "CONFIGURATION";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().value() ==
		        proton::source::durability_mode::CONFIGURATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} UNSETTLED_STATE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "UNSETTLED_STATE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().value() ==
		        proton::source::durability_mode::UNSETTLED_STATE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} DELIVERIES"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "DELIVERIES";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_durability_mode().value() ==
		        proton::source::durability_mode::UNSETTLED_STATE); // DELIVERIES is parsed as UNSETTLED_STATE
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_timeout() ==
		        audit_amqp::amqp_config::defaults::sender_source_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_timeout().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = 600;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_timeout().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_timeout().value() == proton::duration(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} below_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = -10;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} above_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = too_big_for_int64;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		auto& source = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_SOURCE];
		source.erase(source.find(audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy() ==
		        audit_amqp::amqp_config::defaults::sender_source_expiry_policy);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_source_expiry_policy().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} LINK_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "LINK_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().value() ==
		        proton::source::expiry_policy::LINK_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} SESSION_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "SESSION_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().value() ==
		        proton::source::expiry_policy::SESSION_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} CONNECTION_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "CONNECTION_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().value() ==
		        proton::source::expiry_policy::CONNECTION_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} NEVER"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "NEVER";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_source_expiry_policy().value() ==
		        proton::source::expiry_policy::NEVER);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_SOURCE,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_SOURCE]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} absent"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_TARGET))
	{
		auto& sender = psc_json[audit_amqp::amqp_config::KW_SENDER];
		sender.erase(sender.find(audit_amqp::amqp_config::KW_LINK_TARGET));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} null"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_TARGET))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} empty"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_TARGET))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        = nlohmann::json::object();
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.amqp_config().sender_target_address() ==
		      audit_amqp::amqp_config::defaults::sender_target_address);
		CHECK(plugin_config.amqp_config().sender_target_dynamic() ==
		      audit_amqp::amqp_config::defaults::sender_target_dynamic);
		CHECK(plugin_config.amqp_config().sender_target_anonymous() ==
		      audit_amqp::amqp_config::defaults::sender_target_anonymous);
		CHECK(plugin_config.amqp_config().sender_target_durability_mode() ==
		      audit_amqp::amqp_config::defaults::sender_target_durability_mode);
		CHECK(plugin_config.amqp_config().sender_target_timeout() ==
		      audit_amqp::amqp_config::defaults::sender_target_timeout);
		CHECK(plugin_config.amqp_config().sender_target_expiry_policy() ==
		      audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_TARGET))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(
		FMT_COMPILE("{}:{} bad_type"), audit_amqp::amqp_config::KW_SENDER, audit_amqp::amqp_config::KW_LINK_TARGET))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_ADDRESS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_address() ==
		        audit_amqp::amqp_config::defaults::sender_target_address);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_address().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ADDRESS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ADDRESS]
		        = "a.test.host";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_address().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_address().value() == "a.test.host");
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_dynamic() ==
		        audit_amqp::amqp_config::defaults::sender_target_dynamic);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_dynamic().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} true"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_dynamic().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_dynamic().value() == true);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} false"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DYNAMIC]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_dynamic().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_dynamic().value() == false);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_anonymous() ==
		        audit_amqp::amqp_config::defaults::sender_target_anonymous);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_anonymous().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} true"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = true;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_anonymous().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_anonymous().value() == true);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} false"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_ANONYMOUS]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_anonymous().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_anonymous().value() == false);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode() ==
		        audit_amqp::amqp_config::defaults::sender_target_durability_mode);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_durability_mode().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} NONDURABLE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "NONDURABLE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().value() ==
		        proton::target::durability_mode::NONDURABLE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} CONFIGURATION"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "CONFIGURATION";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().value() ==
		        proton::target::durability_mode::CONFIGURATION);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} UNSETTLED_STATE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "UNSETTLED_STATE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().value() ==
		        proton::target::durability_mode::UNSETTLED_STATE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} DELIVERIES"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "DELIVERIES";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_durability_mode().value() ==
		        proton::target::durability_mode::UNSETTLED_STATE); // DELIVERIES is parsed as UNSETTLED_STATE
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_DURABILITY_MODE]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_timeout() ==
		        audit_amqp::amqp_config::defaults::sender_target_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_timeout().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = "bad_Type";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} populated"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = 600;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_timeout().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_timeout().value() == proton::duration(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} below_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = -10;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} above_range"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_TIMEOUT]
		        = too_big_for_int64;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} absent"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		auto& target = psc_json[audit_amqp::amqp_config::KW_SENDER][audit_amqp::amqp_config::KW_LINK_TARGET];
		target.erase(target.find(audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy() ==
		        audit_amqp::amqp_config::defaults::sender_target_expiry_policy);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} null"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = nullptr;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().sender_target_expiry_policy().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_type"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = false;
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} LINK_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "LINK_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().value() ==
		        proton::target::expiry_policy::LINK_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} SESSION_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "SESSION_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().value() ==
		        proton::target::expiry_policy::SESSION_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} CONNECTION_CLOSE"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "CONNECTION_CLOSE";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().value() ==
		        proton::target::expiry_policy::CONNECTION_CLOSE);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} NEVER"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "NEVER";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().has_value());
		REQUIRE(plugin_config.amqp_config().sender_target_expiry_policy().value() ==
		        proton::target::expiry_policy::NEVER);
	}

	SECTION(fmt::format(FMT_COMPILE("{}:{}:{} bad_value"),
	                    audit_amqp::amqp_config::KW_SENDER,
	                    audit_amqp::amqp_config::KW_LINK_TARGET,
	                    audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY))
	{
		// clang-format off
		psc_json[audit_amqp::amqp_config::KW_SENDER]
		        [audit_amqp::amqp_config::KW_LINK_TARGET]
		        [audit_amqp::amqp_config::KW_TERMINUS_EXPIRY_POLICY]
		        = "asdf";
		// clang-format on
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_DURABLE_MESSAGES))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_DURABLE_MESSAGES));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().durable_messages() == audit_amqp::amqp_config::defaults::durable_messages);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_DURABLE_MESSAGES))
	{
		psc_json[audit_amqp::amqp_config::KW_DURABLE_MESSAGES] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE_FALSE(plugin_config.amqp_config().durable_messages().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_DURABLE_MESSAGES))
	{
		psc_json[audit_amqp::amqp_config::KW_DURABLE_MESSAGES] = 200;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} true"), audit_amqp::amqp_config::KW_DURABLE_MESSAGES))
	{
		psc_json[audit_amqp::amqp_config::KW_DURABLE_MESSAGES] = true;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().durable_messages().has_value());
		REQUIRE(plugin_config.amqp_config().durable_messages().value() == true);
	}

	SECTION(fmt::format(FMT_COMPILE("{} false"), audit_amqp::amqp_config::KW_DURABLE_MESSAGES))
	{
		psc_json[audit_amqp::amqp_config::KW_DURABLE_MESSAGES] = false;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().durable_messages().has_value());
		REQUIRE(plugin_config.amqp_config().durable_messages().value() == false);
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().message_send_timeout() ==
		        audit_amqp::amqp_config::defaults::message_send_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().message_send_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT] = "bad type yay";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().message_send_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_MESSAGE_SEND_TIMEOUT] = too_big_for_ms;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_open_timeout() ==
		        audit_amqp::amqp_config::defaults::session_open_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_open_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT] = "bad type yay";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_open_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_OPEN_TIMEOUT] = too_big_for_ms;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} absent"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json.erase(psc_json.find(audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT));
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_close_timeout() ==
		        audit_amqp::amqp_config::defaults::session_close_timeout);
	}

	SECTION(fmt::format(FMT_COMPILE("{} null"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT] = nullptr;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_close_timeout() == std::chrono::milliseconds(0));
	}

	SECTION(fmt::format(FMT_COMPILE("{} bad_type"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT] = "bad type yay";
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == KEY_TYPE_MISMATCH);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} populated"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT] = 600;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		REQUIRE(plugin_config.amqp_config().session_close_timeout() == std::chrono::milliseconds(600));
	}

	SECTION(fmt::format(FMT_COMPILE("{} below_range"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT] = -10;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}

	SECTION(fmt::format(FMT_COMPILE("{} above_range"), audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT))
	{
		psc_json[audit_amqp::amqp_config::KW_SESSION_CLOSE_TIMEOUT] = too_big_for_ms;
		INFO("server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		const irods::error err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(err.code() == SYS_CONFIG_FILE_ERR);
		CHECK_FALSE(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK_FALSE(plugin_config.amqp_config().is_initialized());
	}
}

TEST_CASE("failed second load")
{
	irods::experimental::log::init(getpid(), false, false);

	nlohmann::json server_config = server_config_init;

	auto& plugin_config_json =
		server_config[irods::KW_CFG_PLUGIN_CONFIGURATION][irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE][1];
	auto& psc_json = plugin_config_json[irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION];

	psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "ALLOW_OPERATION";
	psc_json[audit_amqp::amqp_config::KW_PATH] = "queues/amqp_audit_events";

	audit_amqp::plugin_config plugin_config;

	INFO("initial server config: " << server_config.dump(2));
	irods::server_properties::instance().set_configuration(server_config);
	irods::error err = plugin_config.initialize(re_instance_name);
	CHECK(err.ok());
	CHECK(plugin_config.is_configured());
	CHECK_FALSE(plugin_config.is_old_config());
	CHECK(plugin_config.amqp_config().is_initialized());
	CHECK(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION);
	CHECK(plugin_config.amqp_config().path() == "/queues/amqp_audit_events");

	SECTION("previous config preservation")
	{
		psc_json[audit_amqp::amqp_config::KW_ENDPOINTS] = nullptr;
		INFO("second load server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK(plugin_config.is_old_config());
		CHECK(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION);
		CHECK(plugin_config.amqp_config().path() == "/queues/amqp_audit_events");
	}

	SECTION(fmt::format(FMT_COMPILE("{} preserve on {} ALLOW_OPERATION"),
	                    audit_amqp::plugin_config::KW_PEP_REGEX,
	                    audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = nullptr;
		INFO("second load server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK(plugin_config.is_old_config());
		CHECK(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::ALLOW_OPERATION);
		CHECK(plugin_config.amqp_config().path() == "/queues/amqp_audit_events");
		CHECK(plugin_config.pep_regex().has_value());
	}

	SECTION(fmt::format(FMT_COMPILE("{} discard on {} BLOCK_OPERATION"),
	                    audit_amqp::plugin_config::KW_PEP_REGEX,
	                    audit_amqp::plugin_config::KW_FAILSAFE_MODE))
	{
		psc_json[audit_amqp::plugin_config::KW_FAILSAFE_MODE] = "BLOCK_OPERATION";
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = nullptr;
		INFO("second load server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK(plugin_config.is_old_config());
		CHECK(plugin_config.failsafe_mode() == audit_amqp::plugin_config::failsafe_mode::BLOCK_OPERATION);
		CHECK(plugin_config.amqp_config().path() == "/queues/amqp_audit_events");
		CHECK_FALSE(plugin_config.pep_regex().has_value());
	}

	SECTION("third config success")
	{
		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = nullptr;
		INFO("second load server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		err = plugin_config.initialize(re_instance_name);
		CHECK_FALSE(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK(plugin_config.is_old_config());

		psc_json[audit_amqp::plugin_config::KW_PEP_REGEX] = "audit_.+";
		INFO("third load server config: " << server_config.dump(2));
		irods::server_properties::instance().set_configuration(server_config);
		err = plugin_config.initialize(re_instance_name);
		CHECK(err.ok());
		CHECK(plugin_config.is_configured());
		CHECK_FALSE(plugin_config.is_old_config());
		CHECK(plugin_config.amqp_config().is_initialized());
		CHECK(plugin_config.pep_regex().has_value());
	}
}
// NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers,cppcoreguidelines-pro-bounds-avoid-unchecked-container-access)
