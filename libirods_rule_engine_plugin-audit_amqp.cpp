// irods includes
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_serialization.hpp>
#include <irods/irods_server_properties.hpp>

#undef LIST

// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <chrono>
#include <ctime>
#include <sstream>
#include <map>
#include <iostream>
#include <fstream>
#include <mutex>

// boost includes
#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

static std::string audit_pep_regex_to_match = "audit_.*";
static std::string audit_amqp_topic         = "audit_messages";
static std::string audit_amqp_location      = "localhost:5672";
static std::string url;
static std::string audit_amqp_options       = "";
static std::string log_path_prefix          = "/tmp";
static bool test_mode                       = false;
static std::ofstream log_file_ofstream;

static std::mutex  audit_plugin_mutex;

// proton-cpp includes
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>
#include <proton/listener.hpp>
#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/value.hpp>
#include <proton/tracker.hpp>
#include <proton/types.hpp>
#include <proton/sender.hpp>

// See qpid-cpp docs (https://qpid.apache.org/releases/qpid-proton-0.27.0/proton/cpp/api/simple_send_8cpp-example.html)
// Phillip Davis, 7/5/22
class send_handler : public proton::messaging_handler {
    private:
        std::string url;
        proton::sender sender;
        std::string message;

    public:
        send_handler(
            const std::string& _url,
            const std::string& _message
        ) : url(_url)
            ,message(_message)
        { }

        send_handler(){ }

        void on_container_start(proton::container &c) override {
            sender = c.open_sender(url);
        }

        void on_tracker_accept(proton::tracker& t) override {
            t.connection().close(); // we're only sending one message
                                    // so we don't care about the credit system
                                    // or tracking confirmed messages
        }

        void on_sendable(proton::sender &s) override {
            proton::message m(message);
            s.send(m);
        }
}; // class send_handler

void insert_or_parse_as_bin (
    nlohmann::json& json_obj,
    const std::string_view&    key,
    const std::string_view&    val
) {
    try {
        json_obj[key] = nlohmann::json::parse("\"" + val + "\"");
    } catch(const nlohmann::json::exception& e) {
        json_obj[key] = nlohmann::json::binary(
            std::vector<uint8_t>(val.begin(), val.end())
        );
        irods::log(
            LOG_DEBUG,
            fmt::runtime(
                "[AUDIT] - Message with timestamp:[{}] had invalid UTF-8 in key:[{}] and was stored as binary.",
                json_obj["time_stamp"],
                key
            )
        ); // irods::log
    }
}

// Insert the key arg into arg_map and storing the number of insertions of arg as the value.
// The value (number of insertions) is returned.
int insert_arg_into_counter_map(std::map<std::string, int>& arg_map, const std::string& arg) {
    std::map<std::string, int>::iterator iter = arg_map.find(arg);
    if (iter == arg_map.end()) {
        arg_map.insert(std::make_pair(arg, 1));
        return 1;
    } else {
        iter->second = iter->second+1;
        return iter->second;
    }
}

irods::error get_re_configs(
    const std::string& _instance_name ) {
    try {
        const auto& rule_engines = irods::get_server_property< const nlohmann::json& >(std::vector<std::string>{ irods::KW_CFG_PLUGIN_CONFIGURATION, irods::KW_CFG_PLUGIN_TYPE_RULE_ENGINE } );
        for ( const auto& rule_engine : rule_engines ) {
            const auto& inst_name = rule_engine.at( irods::KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
            if ( inst_name == _instance_name ) {
                if ( rule_engine.count( irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION) > 0 ) {

                    const auto& plugin_spec_cfg = rule_engine.at(irods::KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);

                    audit_pep_regex_to_match  = plugin_spec_cfg.at("pep_regex_to_match").get<std::string>();
                    audit_amqp_topic          = plugin_spec_cfg.at("amqp_topic").get<std::string>();
                    audit_amqp_location       = plugin_spec_cfg.at("amqp_location").get<std::string>();
                    audit_amqp_options        = plugin_spec_cfg.at("amqp_options").get<std::string>();
                    url                       = audit_amqp_location + audit_amqp_topic;

                    // look for a test mode setting.  if it doesn't exist just keep test_mode at false.
                    // if test_mode = true and log_path_prefix isn't set just leave the default
                    try {
                        const std::string& test_mode_str = plugin_spec_cfg.at("test_mode").get_ref<const std::string&>();
                        test_mode = boost::iequals(test_mode_str, "true");
                        if (test_mode) {
                             log_path_prefix  = plugin_spec_cfg.at("log_path_prefix").get<std::string>();
                        }
                    } catch (const std::out_of_range& e1) {}

                } else {
                    rodsLog(
                        LOG_DEBUG,
                        "%s - using default configuration: regex - %s, topic - %s, location - %s",
                        audit_pep_regex_to_match.c_str(),
                        audit_amqp_topic.c_str(),
                        audit_amqp_location.c_str() );
                }

                return SUCCESS();
            }
        }
    } catch ( const boost::bad_any_cast& e ) {
        return ERROR( INVALID_ANY_CAST, e.what() );
    } catch ( const std::out_of_range& e ) {
        return ERROR( KEY_NOT_FOUND, e.what() );
    } catch (...) {
        return ERROR(SYS_UNKNOWN_ERROR, fmt::format("[{}:{}] - unknown error occurred", __func__, __LINE__));
    }

    std::stringstream msg;
    msg << "failed to find configuration for audit_amqp plugin ["
        << _instance_name << "]";
    rodsLog( LOG_ERROR, "%s", msg.str().c_str() );
    return ERROR( SYS_INVALID_INPUT_PARAM, msg.str() );;
}


irods::error start(irods::default_re_ctx& _u,const std::string& _instance_name) {
    (void) _u;

    std::lock_guard<std::mutex> lock(audit_plugin_mutex);

    irods::error ret = get_re_configs( _instance_name );
    if( !ret.ok() ) {
        irods::log(PASS(ret));
    }

    nlohmann::json json_obj;

    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;

    char host_name[MAX_NAME_LEN];
    gethostname( host_name, MAX_NAME_LEN );

    pid_t pid = getpid();

    std::string log_file = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
    std::string msg_str;
    try {
        json_obj["time_stamp"] = std::to_string(time_ms);
        insert_or_parse_as_bin(
            json_obj,
            "hostname",
            host_name
        );
        json_obj["pid"] = std::to_string(pid);
        json_obj["action"] = "START";

        if (test_mode) {
            insert_or_parse_as_bin(
                json_obj,
                "log_file",
                log_file
            );
        }

    }
    catch (const irods::exception& e) {
        rodsLog(LOG_NOTICE, e.client_display_what());
        return ERROR(e.code(), fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (const std::exception& e) {
        rodsLog(LOG_NOTICE, e.what());
        return ERROR(SYS_INTERNAL_ERR, fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (const nlohmann::json::exception& e) {
        const std::string msg = fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what());
        rodsLog(LOG_NOTICE, msg.data());
        return ERROR(SYS_LIBRARY_ERROR, msg);
    }
    catch (...) {
        return ERROR(SYS_UNKNOWN_ERROR, fmt::format("[{}:{}] - unknown error occurred", __func__, __LINE__));
    }

    msg_str = json_obj.dump();
    send_handler s(url, msg_str);
    proton::container(s).run();

    if (test_mode) {
        log_file_ofstream.open(log_file);
        log_file_ofstream << msg_str << std::endl;
    }

    return SUCCESS();
}

irods::error stop(irods::default_re_ctx& _u,const std::string& _instance_name) {

    std::lock_guard<std::mutex> lock(audit_plugin_mutex);

    nlohmann::json json_obj;

    std::string msg_str;
    try {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
        json_obj["time_stamp"] = std::to_string(time_ms);

        char host_name[MAX_NAME_LEN];
        gethostname( host_name, MAX_NAME_LEN );
        json_obj["hostname"] = host_name;

        pid_t pid = getpid();
        json_obj["pid"] = std::to_string(pid);

        json_obj["action"] = "STOP";


        if (test_mode) {
            json_obj["log_file"] = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
        }
    }
    catch (const irods::exception& e) {
        rodsLog(LOG_NOTICE, e.client_display_what());
        return ERROR(e.code(), fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (const std::exception& e) {
        rodsLog(LOG_NOTICE, e.what());
        return ERROR(SYS_INTERNAL_ERR, fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (const nlohmann::json::exception& e) {
        const std::string msg = fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what());
        rodsLog(LOG_NOTICE, msg.data());
        return ERROR(SYS_LIBRARY_ERROR, msg);
    }
    catch (...) {
        return ERROR(SYS_UNKNOWN_ERROR, fmt::format("[{}:{}] - unknown error occurred", __func__, __LINE__));
    }

    msg_str = json_obj.dump();
    send_handler s(url, msg_str);
    proton::container(s).run();

    if (test_mode) {
        log_file_ofstream << msg_str << std::endl;
        log_file_ofstream.close();
    }

    return SUCCESS();
}

irods::error rule_exists(irods::default_re_ctx&, const std::string& _rn, bool& _ret) {

    try {
        boost::smatch matches;
        boost::regex expr( audit_pep_regex_to_match );
        _ret =  boost::regex_match( _rn, matches, expr );
    }
    catch ( const boost::exception& _e ) {
        std::string what = boost::diagnostic_information(_e);
        return ERROR(
                SYS_INTERNAL_ERR,
                what.c_str() );
    }

    return SUCCESS();
}

irods::error list_rules(irods::default_re_ctx&, std::vector<std::string>&) {
    return SUCCESS();
}

irods::error exec_rule(
    irods::default_re_ctx&,
    const std::string&     _rn,
    std::list<boost::any>& _ps,
    irods::callback        _eff_hdlr) {

    std::lock_guard<std::mutex> lock(audit_plugin_mutex);

    using namespace std::chrono;


    // stores a counter of unique arg types
    std::map<std::string, int> arg_type_map;

    ruleExecInfo_t* rei = nullptr;
    irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    nlohmann::json json_obj;
    std::string msg_str;

    try {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
        json_obj["time_stamp"] = std::to_string(time_ms);

        char host_name[MAX_NAME_LEN];
        gethostname( host_name, MAX_NAME_LEN );
        insert_or_parse_as_bin(
            json_obj,
            "hostname",
            host_name
        );

        pid_t pid = getpid();
        json_obj["pid"] = std::to_string(pid);

        json_obj["rule_name"] = _rn;

        for( const auto itr : _ps ) {
            // The BytesBuf parameter should not be serialized because this commonly contains
            // the entirety of the contents of files. These could be very big and cause the
            // message broker to explode.
            if (std::type_index(typeid(BytesBuf*)) == std::type_index(itr.type())) {
                rodsLog(LOG_DEBUG9, "[{}:{}] - skipping serialization of BytesBuf parameter",
                    __FILE__, __LINE__);
                continue;
            }

            // serialize the parameter to a map
            irods::re_serialization::serialized_parameter_t param;
            irods::error ret = irods::re_serialization::serialize_parameter(itr, param);
            if(!ret.ok()) {
                 rodsLog(
                     LOG_ERROR,
                     "unsupported argument for calling re rules from the rule language");
                 continue;
            }

            for( const auto elem : param ) {

                size_t ctr = insert_arg_into_counter_map(arg_type_map, elem.first);
                std::stringstream ctr_str;
                ctr_str << ctr;

                std::string key = elem.first;
                if (ctr > 1) {
                    key += "__";
                    key += ctr_str.str();
                }

                insert_or_parse_as_bin(
                    json_obj,
                    key,
                    elem.second
                );

                ++ctr;
                ctr_str.clear();

            } // for elem
        } // for itr
    }
    catch (const irods::exception& e) {
        rodsLog(LOG_NOTICE, e.client_display_what());
        return ERROR(e.code(), fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (const nlohmann::json::exception& e) {
        const std::string msg = fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what());
        rodsLog(LOG_NOTICE, msg.data());
        return ERROR(SYS_LIBRARY_ERROR, msg);
    }
    catch (const std::exception& e) {
        rodsLog(LOG_NOTICE, e.what());
        return ERROR(SYS_INTERNAL_ERR, fmt::format("[{}:{}] - [{}]", __func__, __LINE__, e.what()));
    }
    catch (...) {
        return ERROR(SYS_UNKNOWN_ERROR, fmt::format("[{}:{}] - unknown error occurred", __func__, __LINE__));
    }

    msg_str = json_obj.dump();
    send_handler s(url, msg_str);
    proton::container(s).run();

    if (test_mode) {
        log_file_ofstream << msg_str << std::endl;
    }

    return err;
}

//
// Plugin Factory
//

using pluggable_rule_engine = irods::pluggable_rule_engine<irods::default_re_ctx>;

extern "C"
auto plugin_factory(const std::string& _inst_name, const std::string& _context) -> pluggable_rule_engine*
{
    const auto not_supported = [](auto&&...) { return ERROR(SYS_NOT_SUPPORTED, "Not supported."); };

    auto* re = new irods::pluggable_rule_engine<irods::default_re_ctx>( _inst_name , _context);

    re->add_operation("start", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(start));

    re->add_operation("stop", std::function<irods::error(irods::default_re_ctx&, const std::string&)>(stop));

    re->add_operation("rule_exists", std::function<irods::error(irods::default_re_ctx&, const std::string&, bool&)>(rule_exists));

    re->add_operation("list_rules", std::function<irods::error(irods::default_re_ctx&, std::vector<std::string>&)>(list_rules));

    re->add_operation("exec_rule", std::function<irods::error(irods::default_re_ctx&, const std::string&, std::list<boost::any>&, irods::callback)>(exec_rule));

    re->add_operation("exec_rule_text", std::function<irods::error(irods::default_re_ctx&, const std::string&, msParamArray_t*, const std::string&, irods::callback)>(not_supported));

    re->add_operation("exec_rule_expression", std::function<irods::error(irods::default_re_ctx&, const std::string&, msParamArray_t*, irods::callback)>(not_supported));

    return re;
}