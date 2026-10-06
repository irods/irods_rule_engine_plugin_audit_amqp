#ifndef IRODS_AUDIT_AMQP_PROTON_FORMATTERS_HPP
#define IRODS_AUDIT_AMQP_PROTON_FORMATTERS_HPP

#include "irods/private/proton_object_type.hpp"

#include <irods/irods_logger.hpp>

#include <fmt/format.h>
#include <fmt/chrono.h>
#include <fmt/compile.h>
#include <fmt/xchar.h>
#if FMT_VERSION >= 90000
#  include <fmt/std.h>
#endif

#include <nlohmann/json.hpp>

#include <proton/annotation_key.hpp>
#include <proton/binary.hpp>
#include <proton/byte_array.hpp>
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/decimal.hpp>
#include <proton/codec/decoder.hpp>
#include <proton/delivery_mode.hpp>
#include <proton/duration.hpp>
#include <proton/error.hpp>
#include <proton/error_condition.hpp>
#include <proton/map.hpp>
#include <proton/message_id.hpp>
#include <proton/sasl.hpp>
#include <proton/scalar.hpp>
#include <proton/scalar_base.hpp>
#include <proton/sender.hpp>
#include <proton/session.hpp>
#include <proton/source.hpp>
#include <proton/ssl.hpp>
#include <proton/symbol.hpp>
#include <proton/target.hpp>
#include <proton/terminus.hpp>
#include <proton/timestamp.hpp>
#include <proton/tracker.hpp>
#include <proton/transfer.hpp>
#include <proton/transport.hpp>
#include <proton/type_id.hpp>
#include <proton/uuid.hpp>
#include <proton/value.hpp>

#include <chrono>
#include <cstdint>
#include <map>
#include <ratio>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

// NOLINTBEGIN(clazy-function-args-by-value, readability-convert-member-functions-to-static)

template <>
struct fmt::formatter<enum proton::delivery_mode::modes> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::delivery_mode::modes& _mode, FormatContext& _ctx) const
		-> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_mode) {
			case proton::delivery_mode::NONE:
				valstr = "NONE";
				break;
			case proton::delivery_mode::AT_MOST_ONCE:
				valstr = "AT_MOST_ONCE";
				break;
			case proton::delivery_mode::AT_LEAST_ONCE:
				valstr = "AT_LEAST_ONCE";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::delivery_mode::modes>>(_mode));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::sasl::outcome> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::sasl::outcome& _outcome, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_outcome) {
			case proton::sasl::NONE:
				valstr = "NONE";
				break;
			case proton::sasl::OK:
				valstr = "OK";
				break;
			case proton::sasl::AUTH:
				valstr = "AUTH";
				break;
			case proton::sasl::SYS:
				valstr = "SYS";
				break;
			case proton::sasl::PERM:
				valstr = "PERM";
				break;
			case proton::sasl::TEMP:
				valstr = "TEMP";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::sasl::outcome>>(_outcome));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::source::distribution_mode> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::source::distribution_mode& _mode, FormatContext& _ctx) const
		-> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_mode) {
			case proton::source::UNSPECIFIED:
				valstr = "UNSPECIFIED";
				break;
			case proton::source::COPY:
				valstr = "COPY";
				break;
			case proton::source::MOVE:
				valstr = "MOVE";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::source::distribution_mode>>(_mode));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::ssl::resume_status> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::ssl::resume_status& _status, FormatContext& _ctx) const
		-> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_status) {
			case proton::ssl::UNKNOWN:
				valstr = "UNKNOWN";
				break;
			case proton::ssl::NEW:
				valstr = "NEW";
				break;
			case proton::ssl::REUSED:
				valstr = "REUSED";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::ssl::resume_status>>(_status));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::ssl::verify_mode> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::ssl::verify_mode& _mode, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_mode) {
			case proton::ssl::VERIFY_PEER:
				valstr = "VERIFY_PEER";
				break;
			case proton::ssl::ANONYMOUS_PEER:
				valstr = "ANONYMOUS_PEER";
				break;
			case proton::ssl::VERIFY_PEER_NAME:
				valstr = "VERIFY_PEER_NAME";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::ssl::verify_mode>>(_mode));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::terminus::durability_mode> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::terminus::durability_mode& _mode, FormatContext& _ctx) const
		-> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_mode) {
			case proton::terminus::NONDURABLE:
				valstr = "NONDURABLE";
				break;
			case proton::terminus::CONFIGURATION:
				valstr = "CONFIGURATION";
				break;
			case proton::terminus::UNSETTLED_STATE:
				valstr = "UNSETTLED_STATE";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::terminus::durability_mode>>(_mode));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::terminus::expiry_policy> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::terminus::expiry_policy& _policy, FormatContext& _ctx) const
		-> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_policy) {
			case proton::terminus::LINK_CLOSE:
				valstr = "LINK_CLOSE";
				break;
			case proton::terminus::SESSION_CLOSE:
				valstr = "SESSION_CLOSE";
				break;
			case proton::terminus::CONNECTION_CLOSE:
				valstr = "CONNECTION_CLOSE";
				break;
			case proton::terminus::NEVER:
				valstr = "NEVER";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::terminus::expiry_policy>>(_policy));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<enum proton::transfer::state> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const enum proton::transfer::state& _state, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_state) {
			case proton::transfer::state::NONE:
				valstr = "NONE";
				break;
			case proton::transfer::state::RECEIVED:
				valstr = "RECEIVED";
				break;
			case proton::transfer::state::ACCEPTED:
				valstr = "ACCEPTED";
				break;
			case proton::transfer::state::REJECTED:
				valstr = "REJECTED";
				break;
			case proton::transfer::state::RELEASED:
				valstr = "RELEASED";
				break;
			case proton::transfer::state::MODIFIED:
				valstr = "MODIFIED";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<enum proton::transfer::state>>(_state));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<proton::type_id> : fmt::formatter<std::string_view>
{
	template <typename FormatContext>
	constexpr auto format(const proton::type_id& _type_id, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		std::string_view valstr;
		switch (_type_id) {
			case proton::NULL_TYPE:
				valstr = "NULL_TYPE";
				break;
			case proton::BOOLEAN:
				valstr = "BOOLEAN";
				break;
			case proton::UBYTE:
				valstr = "UBYTE";
				break;
			case proton::BYTE:
				valstr = "BYTE";
				break;
			case proton::USHORT:
				valstr = "USHORT";
				break;
			case proton::SHORT:
				valstr = "SHORT";
				break;
			case proton::UINT:
				valstr = "UINT";
				break;
			case proton::INT:
				valstr = "INT";
				break;
			case proton::CHAR:
				valstr = "CHAR";
				break;
			case proton::ULONG:
				valstr = "ULONG";
				break;
			case proton::LONG:
				valstr = "LONG";
				break;
			case proton::TIMESTAMP:
				valstr = "TIMESTAMP";
				break;
			case proton::FLOAT:
				valstr = "FLOAT";
				break;
			case proton::DOUBLE:
				valstr = "DOUBLE";
				break;
			case proton::DECIMAL32:
				valstr = "DECIMAL32";
				break;
			case proton::DECIMAL64:
				valstr = "DECIMAL64";
				break;
			case proton::DECIMAL128:
				valstr = "DECIMAL128";
				break;
			case proton::UUID:
				valstr = "UUID";
				break;
			case proton::BINARY:
				valstr = "BINARY";
				break;
			case proton::STRING:
				valstr = "STRING";
				break;
			case proton::SYMBOL:
				valstr = "SYMBOL";
				break;
			case proton::DESCRIBED:
				valstr = "DESCRIBED";
				break;
			case proton::ARRAY:
				valstr = "ARRAY";
				break;
			case proton::LIST:
				valstr = "LIST";
				break;
			case proton::MAP:
				valstr = "MAP";
				break;
			default:
				return format_to(_ctx.out(),
				                 FMT_COMPILE("UNKNOWN_{}"),
				                 static_cast<std::underlying_type_t<proton::type_id>>(_type_id));
				break;
		}
		return fmt::formatter<std::string_view>::format(valstr, _ctx);
	}
};

template <>
struct fmt::formatter<proton::duration>
	: fmt::formatter<std::chrono::duration<proton::duration::numeric_type, std::milli>>
{
	template <typename FormatContext>
	constexpr auto format(const proton::duration& _duration, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		return fmt::formatter<std::chrono::duration<proton::duration::numeric_type, std::milli>>::format(
			std::chrono::duration<proton::duration::numeric_type, std::milli>(_duration.milliseconds()), _ctx);
	}
};

template <>
struct fmt::formatter<proton::timestamp>
	: fmt::formatter<std::chrono::time_point<std::chrono::system_clock,
                                             std::chrono::duration<proton::timestamp::numeric_type, std::milli>>>
{
	template <typename FormatContext>
	constexpr auto format(const proton::timestamp& _timestamp, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		return fmt::formatter<
			std::chrono::time_point<std::chrono::system_clock,
			                        std::chrono::duration<proton::timestamp::numeric_type, std::milli>>>::
			format(std::chrono::time_point<std::chrono::system_clock,
			                               std::chrono::duration<proton::timestamp::numeric_type, std::milli>>(
					   std::chrono::duration<proton::timestamp::numeric_type, std::milli>(_timestamp.milliseconds())),
			       _ctx);
	}
};

namespace
{
	static inline constexpr const std::string_view hex_chars("0123456789abcdef");
}

template <std::size_t N>
struct fmt::formatter<proton::byte_array<N>> : fmt::formatter<char>
{
	template <typename FormatContext>
	constexpr auto format(const proton::byte_array<N>& _array, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		auto out = _ctx.out();
		out = format_to(out, "0x");
		for (const std::uint8_t val : _array) {
			out = fmt::formatter<char>::format(hex_chars[val >> 4], _ctx);
			out = fmt::formatter<char>::format(hex_chars[val & 0x0F], _ctx);
		}
		return out;
	}
};

template <>
struct fmt::formatter<proton::uuid> : fmt::formatter<std::string>
{
	template <typename FormatContext>
	constexpr auto format(const proton::uuid& _uuid, FormatContext& _ctx) const
	{
		return fmt::formatter<std::string>::format(_uuid.str(), _ctx);
	}
};

template <>
struct fmt::formatter<proton::binary> : fmt::formatter<std::string>
{
	template <typename FormatContext>
	constexpr auto format(const proton::binary& _binary, FormatContext& _ctx) const
	{
		return fmt::formatter<std::string>::format(static_cast<std::string>(_binary), _ctx);
	}
};

#if FMT_VERSION >= 100000
template <>
struct fmt::formatter<proton::symbol> : fmt::formatter<std::string>
{
	template <typename FormatContext>
	constexpr auto format(const proton::symbol& _symbol, FormatContext& _ctx) const
	{
		return fmt::formatter<std::string>::format(static_cast<std::string>(_symbol), _ctx);
	}
};
#endif

#if FMT_VERSION >= 90000
template <>
struct fmt::formatter<proton::delivery_mode> : fmt::formatter<enum proton::delivery_mode::modes>
{
	template <typename FormatContext>
	constexpr auto format(const proton::delivery_mode& _mode, FormatContext& _ctx) const -> decltype(_ctx.out())
	{
		return fmt::formatter<enum proton::delivery_mode::modes>::format(
			// const_cast because we haven't vendored qpid-proton yet. This is a read-only operation, it's fine.
			// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
			static_cast<const enum proton::delivery_mode::modes&>(const_cast<proton::delivery_mode&>(_mode)),
			_ctx);
	}
};
#endif

// NOLINTEND(clazy-function-args-by-value, readability-convert-member-functions-to-static)

namespace irods::plugin::rule_engine::audit_amqp
{
	using log_list = std::vector<irods::experimental::log::key_value>;

	template <typename T>
	inline void log_list_emplace(log_list& _log_kvs,
	                             const std::string& _key,
	                             const T& _value,
	                             const std::string& _key_prefix)
	{
		_log_kvs.emplace_back(fmt::format(FMT_COMPILE("{}{}"), _key_prefix, _key), fmt::to_string(_value));
	}

	template <typename T>
	inline void log_list_emplace_variant_simple(log_list& _log_kvs,
	                                            const std::string& _key,
	                                            const std::string& _type,
	                                            const T& _value,
	                                            const bool dump_type = true)
	{
		if (dump_type) {
			_log_kvs.emplace_back(_key, fmt::format(FMT_COMPILE("{}({})"), _type, fmt::to_string(_value)));
		}
		else {
			_log_kvs.emplace_back(_key, fmt::to_string(_value));
		}
	}

	// forward decls
	inline void dump_proton_value(log_list&, proton::codec::decoder&, const std::string&, const bool = true);
	inline void dump_proton_object(log_list&, const proton::transport&, const std::string&, const proton_object_type);
	inline void dump_proton_object(log_list&, const proton::session&, const std::string&, const proton_object_type);

	template <class T>
	inline void dump_proton_value(log_list& _log_kvs,
	                              const T& _variant,
	                              const std::string& _key,
	                              const bool dump_toplevel_type = true)
		requires(std::is_convertible_v<T, proton::value> || std::is_convertible_v<T, proton::scalar_base>)
	{
		const proton::type_id value_type = _variant.type();
		const std::string value_type_str = fmt::to_string(value_type);

		proton::codec::start c_start;

		switch (value_type) {
			case proton::BOOLEAN:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<bool>(_variant), dump_toplevel_type);
				break;
			case proton::UBYTE:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::uint8_t>(_variant), dump_toplevel_type);
				break;
			case proton::BYTE:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::int8_t>(_variant), dump_toplevel_type);
				break;
			case proton::USHORT:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::uint16_t>(_variant), dump_toplevel_type);
				break;
			case proton::SHORT:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::int16_t>(_variant), dump_toplevel_type);
				break;
			case proton::UINT:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::uint32_t>(_variant), dump_toplevel_type);
				break;
			case proton::INT:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::int32_t>(_variant), dump_toplevel_type);
				break;
			case proton::CHAR:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, std::to_string(proton::get<wchar_t>(_variant)), dump_toplevel_type);
				break;
			case proton::ULONG:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::uint64_t>(_variant), dump_toplevel_type);
				break;
			case proton::LONG:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::int64_t>(_variant), dump_toplevel_type);
				break;
			case proton::TIMESTAMP:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::timestamp>(_variant), dump_toplevel_type);
				break;
			case proton::FLOAT:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<float>(_variant), dump_toplevel_type);
				break;
			case proton::DOUBLE:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<double>(_variant), dump_toplevel_type);
				break;
			case proton::DECIMAL32:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::decimal32>(_variant), dump_toplevel_type);
				break;
			case proton::DECIMAL64:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::decimal64>(_variant), dump_toplevel_type);
				break;
			case proton::DECIMAL128:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::decimal128>(_variant), dump_toplevel_type);
				break;
			case proton::UUID:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::uuid>(_variant), dump_toplevel_type);
				break;
			case proton::BINARY:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::binary>(_variant), dump_toplevel_type);
				break;
			case proton::STRING:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<std::string>(_variant), dump_toplevel_type);
				break;
			case proton::SYMBOL:
				log_list_emplace_variant_simple(
					_log_kvs, _key, value_type_str, proton::get<proton::symbol>(_variant), dump_toplevel_type);
				break;
			case proton::DESCRIBED: {
				if (dump_toplevel_type) {
					log_list_emplace(_log_kvs, "::variant_type", value_type_str, _key);
				}
				proton::codec::decoder decoder(_variant);
				decoder >> c_start;
				dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::descriptor"), _key));
				dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::value"), _key));
				decoder >> proton::codec::finish();
				break;
			}
			case proton::ARRAY: {
				if (dump_toplevel_type) {
					log_list_emplace(_log_kvs, "::variant_type", value_type_str, _key);
				}
				proton::codec::decoder decoder(_variant);
				decoder >> c_start;
				const proton::type_id element_type = c_start.element;
				log_list_emplace(_log_kvs, "::element_type", c_start.element, _key);
				if (c_start.is_described) {
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::descriptor"), _key));
				}
				for (std::size_t idx = 0; idx < c_start.size; idx++) {
					dump_proton_value(_log_kvs,
					                  decoder,
					                  fmt::format(FMT_COMPILE("{}[{}]"), _key, idx),
					                  decoder.next_type() != element_type);
				}
				decoder >> proton::codec::finish();
				break;
			}
			case proton::LIST: {
				if (dump_toplevel_type) {
					log_list_emplace(_log_kvs, "::variant_type", value_type_str, _key);
				}
				proton::codec::decoder decoder(_variant);
				decoder >> c_start;
				if (c_start.is_described) {
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::descriptor"), _key));
				}
				for (std::size_t idx = 0; idx < c_start.size; idx++) {
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}[{}]"), _key, idx));
				}
				decoder >> proton::codec::finish();
				break;
			}
			case proton::MAP: {
				if (dump_toplevel_type) {
					log_list_emplace(_log_kvs, "::variant_type", value_type_str, _key);
				}
				proton::codec::decoder decoder(_variant);
				decoder >> c_start;
				if (c_start.is_described) {
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::descriptor"), _key));
				}
				const std::size_t map_sz = c_start.size / 2;
				for (std::size_t idx = 0; idx < map_sz; idx++) {
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}[{}]::key"), _key, idx));
					dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}[{}]::value"), _key, idx));
				}
				decoder >> proton::codec::finish();
				break;
			}
			case proton::NULL_TYPE:
			default:
				_log_kvs.emplace_back(_key, fmt::format(FMT_COMPILE("{}()"), value_type_str));
				break;
		}
	}

	inline void dump_proton_value(log_list& _log_kvs,
	                              proton::codec::decoder& _decoder,
	                              const std::string& _key,
	                              const bool dump_toplevel_type)
	{
		if (!_decoder.more()) {
			_log_kvs.emplace_back(_key, "<decode failed: no more data>");
			return;
		}
		try {
			proton::value variant;
			_decoder >> variant;
			dump_proton_value(_log_kvs, variant, _key, dump_toplevel_type);
		}
		catch (const std::exception& e) {
			_log_kvs.emplace_back(_key, fmt::format(FMT_COMPILE("<decode failed: {}>"), e.what()));
		}
	}

	inline void dump_proton_value(log_list& _log_kvs,
	                              const std::map<proton::symbol, proton::value>& _map,
	                              const std::string& _key)
	{
		std::size_t idx = 0;
		for (const auto& [key, value] : _map) {
			log_list_emplace(_log_kvs, fmt::format(FMT_COMPILE("[{}]::key"), idx), key, _key);
			dump_proton_value(_log_kvs, value, fmt::format(FMT_COMPILE("{}[{}]::value"), _key, idx));
		}
	}

	inline void dump_proton_value(log_list& _log_kvs,
	                              const proton::map<proton::symbol, proton::value>& _map,
	                              const std::string& _key)
	{
		proton::value map_v = _map.value();

		if (map_v.type() != proton::type_id::MAP) {
			dump_proton_value(_log_kvs, map_v, _key);
			return;
		}

		proton::codec::start c_start;
		proton::codec::decoder decoder(map_v);
		decoder >> c_start;

		if (c_start.is_described) {
			dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}::descriptor"), _key));
		}

		const std::size_t map_sz = c_start.size / 2;
		for (std::size_t idx = 0; idx < map_sz; idx++) {
			const auto key_name = fmt::format(FMT_COMPILE("{}[{}]::key"), _key, idx);
			if (!decoder.more()) {
				_log_kvs.emplace_back(_key, "<decode failed: no more data>");
			}
			else if (decoder.next_type() != proton::type_id::SYMBOL) {
				dump_proton_value(_log_kvs, decoder, key_name);
			}
			else {
				try {
					proton::value variant;
					decoder >> variant;
					if (variant.type() != proton::type_id::SYMBOL) {
						dump_proton_value(_log_kvs, variant, key_name);
					}
					else {
						_log_kvs.emplace_back(key_name, proton::get<proton::symbol>(variant));
					}
				}
				catch (const std::exception& e) {
					_log_kvs.emplace_back(_key, fmt::format(FMT_COMPILE("<decode failed: {}>"), e.what()));
				}
			}
			dump_proton_value(_log_kvs, decoder, fmt::format(FMT_COMPILE("{}[{}]::value"), _key, idx));
		}
		decoder >> proton::codec::finish();
	}

	inline void dump_proton_value(log_list& _log_kvs,
	                              const std::vector<proton::symbol>& _vector,
	                              const std::string& _key)
	{
		std::size_t idx = 0;
		for (const auto& element : _vector) {
			log_list_emplace(_log_kvs, fmt::format(FMT_COMPILE("[{}]"), idx), element, _key);
		}
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::source& _source,
	                               const std::string& _key_prefix = "proton_source::")
	{
		log_list_emplace(_log_kvs, "address", _source.address(), _key_prefix);
		log_list_emplace(_log_kvs, "distribution_mode", _source.distribution_mode(), _key_prefix);
		// const_cast because we haven't vendored qpid-proton yet. This is a read-only operation, it's fine.
		// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
		log_list_emplace(_log_kvs,
		                 "durability_mode",
		                 // const_cast because we haven't vendored qpid-proton yet. Read-only operation, it's fine.
		                 // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
		                 const_cast<proton::source&>(_source).durability_mode(),
		                 _key_prefix);
		log_list_emplace(_log_kvs, "expiry_policy", _source.expiry_policy(), _key_prefix);
		log_list_emplace(_log_kvs, "timeout", _source.timeout(), _key_prefix);
		log_list_emplace(_log_kvs, "dynamic", _source.dynamic(), _key_prefix);
		log_list_emplace(_log_kvs, "anonymous", _source.anonymous(), _key_prefix);

		dump_proton_value(_log_kvs, _source.filters(), fmt::format(FMT_COMPILE("{}filters"), _key_prefix));
		dump_proton_value(
			_log_kvs, _source.node_properties(), fmt::format(FMT_COMPILE("{}node_properties"), _key_prefix));
		dump_proton_value(_log_kvs, _source.capabilities(), fmt::format(FMT_COMPILE("{}capabilities"), _key_prefix));
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::target& _target,
	                               const std::string& _key_prefix = "proton_target::")
	{
		log_list_emplace(_log_kvs, "address", _target.address(), _key_prefix);
		log_list_emplace(_log_kvs,
		                 "durability_mode",
		                 // const_cast because we haven't vendored qpid-proton yet. Read-only operation, it's fine.
		                 // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
		                 const_cast<proton::target&>(_target).durability_mode(),
		                 _key_prefix);
		log_list_emplace(_log_kvs, "expiry_policy", _target.expiry_policy(), _key_prefix);
		log_list_emplace(_log_kvs, "timeout", _target.timeout(), _key_prefix);
		log_list_emplace(_log_kvs, "dynamic", _target.dynamic(), _key_prefix);
		log_list_emplace(_log_kvs, "anonymous", _target.anonymous(), _key_prefix);

		dump_proton_value(
			_log_kvs, _target.node_properties(), fmt::format(FMT_COMPILE("{}node_properties"), _key_prefix));
		dump_proton_value(_log_kvs, _target.capabilities(), fmt::format(FMT_COMPILE("{}capabilities"), _key_prefix));
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::error_condition& _ec,
	                               const std::string& _key_prefix = "error_condition::")
	{
		log_list_emplace(_log_kvs, "name", _ec.name(), _key_prefix);
		log_list_emplace(_log_kvs, "description", _ec.description(), _key_prefix);
		log_list_emplace(_log_kvs, "what", _ec.what(), _key_prefix);

		dump_proton_value(_log_kvs, _ec.properties(), fmt::format(FMT_COMPILE("{}properties"), _key_prefix));
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::ssl& _ssl,
	                               const std::string& _key_prefix = "proton_ssl::")
	{
		log_list_emplace(_log_kvs, "protocol", _ssl.protocol(), _key_prefix);
		log_list_emplace(_log_kvs, "remote_subject", _ssl.remote_subject(), _key_prefix);
		log_list_emplace(_log_kvs, "cipher", _ssl.cipher(), _key_prefix);
		log_list_emplace(_log_kvs, "ssf", _ssl.ssf(), _key_prefix);
		log_list_emplace(_log_kvs, "resume_status", _ssl.resume_status(), _key_prefix);
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::sasl& _sasl,
	                               const std::string& _key_prefix = "proton_sasl::")
	{
		log_list_emplace(_log_kvs, "outcome", _sasl.outcome(), _key_prefix);
		log_list_emplace(_log_kvs, "user", _sasl.user(), _key_prefix);
		log_list_emplace(_log_kvs, "mech", _sasl.mech(), _key_prefix);
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::connection& _connection,
	                               const std::string& _key_prefix = "proton_connection::",
	                               const proton_object_type _origin = proton_object_type::CONNECTION)
	{
		log_list_emplace(_log_kvs, "uninitialized", _connection.uninitialized(), _key_prefix);
		log_list_emplace(_log_kvs, "active", _connection.active(), _key_prefix);
		log_list_emplace(_log_kvs, "closed", _connection.closed(), _key_prefix);
		log_list_emplace(_log_kvs, "virtual_host", _connection.virtual_host(), _key_prefix);
		log_list_emplace(_log_kvs, "container_id", _connection.container_id(), _key_prefix);
		if (_origin != proton_object_type::TRACKER) {
			// These fields come from the connection object's internal transport object.
			// Under certain circumstances, a tracker objects's connection objects's internal transport object may be
			// uninitialized. Avoid making calls that would access it when the top-level object is a tracker.
			log_list_emplace(_log_kvs, "user", _connection.user(), _key_prefix);
			log_list_emplace(_log_kvs, "max_frame_size", _connection.max_frame_size(), _key_prefix);
			log_list_emplace(_log_kvs, "max_sessions", _connection.max_sessions(), _key_prefix);
			log_list_emplace(_log_kvs, "idle_timeout", _connection.idle_timeout(), _key_prefix);
		}
		log_list_emplace(_log_kvs, "reconnected", _connection.reconnected(), _key_prefix);
		log_list_emplace(_log_kvs, "container::id", _connection.container().id(), _key_prefix);

		dump_proton_value(_log_kvs,
		                  _connection.offered_capabilities(),
		                  fmt::format(FMT_COMPILE("{}offered_capabilities"), _key_prefix));
		dump_proton_value(_log_kvs,
		                  _connection.desired_capabilities(),
		                  fmt::format(FMT_COMPILE("{}desired_capabilities"), _key_prefix));
		dump_proton_value(_log_kvs, _connection.properties(), fmt::format(FMT_COMPILE("{}properties"), _key_prefix));

		// only skip transport if we started there or in a tracker (see the comment above)
		if ((_origin != proton_object_type::TRANSPORT) && (_origin != proton_object_type::TRACKER)) {
			dump_proton_object(
				_log_kvs, _connection.transport(), fmt::format(FMT_COMPILE("{}transport::"), _key_prefix), _origin);
		}
		// only add sessions if we started here or in transport
		else if ((_origin == proton_object_type::TRANSPORT) || (_origin == proton_object_type::CONNECTION)) {
			std::uint64_t s_ctr = 0;
			for (const proton::session& session : _connection.sessions()) {
				dump_proton_object(
					_log_kvs, session, fmt::format(FMT_COMPILE("{}sessions[{}]::"), _key_prefix, s_ctr++), _origin);
			}
		}

		// for the same reasons as above, skip accessing the error object if we started with a tracker
		if ((_origin != proton_object_type::TRACKER) && _connection.error()) {
			dump_proton_object(_log_kvs, _connection.error(), fmt::format(FMT_COMPILE("{}error::"), _key_prefix));
		}
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::sender& _sender,
	                               const std::string& _key_prefix = "proton_sender::",
	                               const proton_object_type _origin = proton_object_type::SENDER)
	{
		log_list_emplace(_log_kvs, "uninitialized", _sender.uninitialized(), _key_prefix);
		log_list_emplace(_log_kvs, "active", _sender.active(), _key_prefix);
		log_list_emplace(_log_kvs, "closed", _sender.closed(), _key_prefix);
		log_list_emplace(_log_kvs, "credit", _sender.credit(), _key_prefix);
		log_list_emplace(_log_kvs,
		                 "draining",
		                 // const_cast because we haven't vendored qpid-proton yet. Read-only operation, it's fine.
		                 // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
		                 const_cast<proton::sender&>(_sender).draining(),
		                 _key_prefix);
		log_list_emplace(_log_kvs, "name", _sender.name(), _key_prefix);
		log_list_emplace(_log_kvs, "container::id", _sender.container().id(), _key_prefix);

		dump_proton_value(_log_kvs, _sender.properties(), fmt::format(FMT_COMPILE("{}properties"), _key_prefix));

		// only add connection and session if we started here
		if (_origin == proton_object_type::SENDER) {
			dump_proton_object(
				_log_kvs, _sender.connection(), fmt::format(FMT_COMPILE("{}connection::"), _key_prefix), _origin);
			dump_proton_object(
				_log_kvs, _sender.session(), fmt::format(FMT_COMPILE("{}session::"), _key_prefix), _origin);
		}

		dump_proton_object(_log_kvs, _sender.source(), fmt::format(FMT_COMPILE("{}source::"), _key_prefix));
		dump_proton_object(_log_kvs, _sender.target(), fmt::format(FMT_COMPILE("{}target::"), _key_prefix));

		if (_sender.error()) {
			dump_proton_object(_log_kvs, _sender.error(), fmt::format(FMT_COMPILE("{}error::"), _key_prefix));
		}
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::tracker& _tracker,
	                               const std::string& _key_prefix = "proton_tracker::",
	                               const proton_object_type _origin = proton_object_type::TRACKER)
	{
		log_list_emplace(_log_kvs, "tag", _tracker.tag(), _key_prefix);
		log_list_emplace(_log_kvs, "state", _tracker.state(), _key_prefix);
		log_list_emplace(_log_kvs, "container::id", _tracker.container().id(), _key_prefix);
		dump_proton_object(
			_log_kvs, _tracker.connection(), fmt::format(FMT_COMPILE("{}connection::"), _key_prefix), _origin);
		dump_proton_object(_log_kvs, _tracker.session(), fmt::format(FMT_COMPILE("{}session::"), _key_prefix), _origin);
		dump_proton_object(_log_kvs, _tracker.sender(), fmt::format(FMT_COMPILE("{}sender::"), _key_prefix), _origin);
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::transport& _transport,
	                               const std::string& _key_prefix = "proton_transport::",
	                               const proton_object_type _origin = proton_object_type::TRANSPORT)
	{
		dump_proton_object(_log_kvs, _transport.ssl(), fmt::format(FMT_COMPILE("{}ssl::"), _key_prefix));
		dump_proton_object(_log_kvs, _transport.sasl(), fmt::format(FMT_COMPILE("{}sasl::"), _key_prefix));

		// only add connection if we started here
		if (_origin == proton_object_type::TRANSPORT) {
			dump_proton_object(
				_log_kvs, _transport.connection(), fmt::format(FMT_COMPILE("{}connection::"), _key_prefix), _origin);
		}

		if (_transport.error()) {
			dump_proton_object(_log_kvs, _transport.error(), fmt::format(FMT_COMPILE("{}error::"), _key_prefix));
		}
	}

	inline void dump_proton_object(log_list& _log_kvs,
	                               const proton::session& _session,
	                               const std::string& _key_prefix = "proton_session::",
	                               const proton_object_type _origin = proton_object_type::SESSION)
	{
		log_list_emplace(_log_kvs, "uninitialized", _session.uninitialized(), _key_prefix);
		log_list_emplace(_log_kvs, "active", _session.active(), _key_prefix);
		log_list_emplace(_log_kvs, "closed", _session.closed(), _key_prefix);
		log_list_emplace(_log_kvs, "incoming_bytes", _session.incoming_bytes(), _key_prefix);
		log_list_emplace(_log_kvs, "outgoing_bytes", _session.outgoing_bytes(), _key_prefix);
		log_list_emplace(_log_kvs, "container::id", _session.container().id(), _key_prefix);

		switch (_origin) {
			case proton_object_type::SESSION:
				dump_proton_object(
					_log_kvs, _session.connection(), fmt::format(FMT_COMPILE("{}connection::"), _key_prefix), _origin);
				[[fallthrough]];
			case proton_object_type::TRANSPORT:
			case proton_object_type::CONNECTION: {
				std::uint64_t s_ctr = 0;
				for (const proton::sender& sender : _session.senders()) {
					dump_proton_object(
						_log_kvs, sender, fmt::format(FMT_COMPILE("{}senders[{}]::"), _key_prefix, s_ctr++), _origin);
				}
				break;
			}
			default:
				break;
		}

		if (_session.error()) {
			dump_proton_object(_log_kvs, _session.error(), fmt::format(FMT_COMPILE("{}error::"), _key_prefix));
		}
	}
} //namespace irods::plugin::rule_engine::audit_amqp

#endif // IRODS_AUDIT_AMQP_PROTON_FORMATTERS_HPP
