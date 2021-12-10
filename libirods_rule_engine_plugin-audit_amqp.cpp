// =-=-=-=-=-=-=-
// irods includes
#include "irods_re_plugin.hpp"
#include "irods_re_serialization.hpp"
#include "irods_server_properties.hpp"

#undef LIST

// =-=-=-=-=-=-=-
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

// =-=-=-=-=-=-=-
// boost includes
#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

// =-=-=-=-=-=-=-
// proton includes
#include "proton/message.h"
#include "proton/messenger.h"

#include "jansson.h"


static std::string audit_pep_regex_to_match = "audit_.*";
static std::string audit_amqp_topic         = "irods_audit_messages";
static std::string audit_amqp_location      = "localhost:5672";
static std::string audit_amqp_options       = "";
static std::string log_path_prefix          = "/tmp";
static bool test_mode                       = false;
static std::ofstream log_file_ofstream; 

static pn_messenger_t * messenger = nullptr;

static std::mutex  audit_plugin_mutex;

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
        const auto& rule_engines = irods::get_server_property< const nlohmann::json& >(std::vector<std::string>{ irods::CFG_PLUGIN_CONFIGURATION_KW, irods::PLUGIN_TYPE_RULE_ENGINE } );
        for ( const auto& rule_engine : rule_engines ) {
            const auto& inst_name = rule_engine.at( irods::CFG_INSTANCE_NAME_KW ).get_ref<const std::string&>();
            if ( inst_name == _instance_name ) {
                if ( rule_engine.count( irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW ) > 0 ) {

                    const auto& plugin_spec_cfg = rule_engine.at(irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW);

                    audit_pep_regex_to_match  = plugin_spec_cfg.at("pep_regex_to_match").get<std::string>();
                    audit_amqp_topic          = plugin_spec_cfg.at("amqp_topic").get<std::string>();
                    audit_amqp_location       = plugin_spec_cfg.at("amqp_location").get<std::string>();
                    audit_amqp_options        = plugin_spec_cfg.at("amqp_options").get<std::string>();

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

    messenger = pn_messenger(NULL);
    pn_messenger_start(messenger);
    pn_messenger_set_blocking(messenger, false);  // do not block
    
    json_t* obj = json_object();
    if( !obj ) {
        return ERROR(SYS_MALLOC_ERR, "json_object() failed");
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    std::stringstream time_str; time_str << time_ms;
    json_object_set(obj, "time_stamp", json_string(time_str.str().c_str()));

    char host_name[MAX_NAME_LEN];
    gethostname( host_name, MAX_NAME_LEN );
    json_object_set(obj, "hostname", json_string(host_name));

    pid_t pid = getpid();
    std::stringstream pid_str; pid_str << pid;
    json_object_set(obj, "pid", json_string(pid_str.str().c_str()));

    json_object_set(obj, "action", json_string("START"));

    std::string log_file;
    if (test_mode) {
        log_file = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
        json_object_set(obj, "log_file", json_string(log_file.c_str()));
    }

    char* tmp_buf = json_dumps( obj, JSON_INDENT( 0 ) );
    std::string msg_str = std::string("__BEGIN_JSON__") + std::string(tmp_buf) + std::string("__END_JSON__");

    pn_message_t * message;
    pn_data_t * body;

    message = pn_message();

    std::string address = audit_amqp_location + "/" + audit_amqp_topic;

    pn_message_set_address(message, address.c_str());
    body = pn_message_body(message);
    pn_data_put_string(body, pn_bytes(msg_str.length(), msg_str.c_str()));
    pn_messenger_put(messenger, message);
    pn_messenger_send(messenger, -1);
    
    pn_message_free(message);

    free(tmp_buf);
    json_decref(obj);

    if (test_mode) {
        //std::ofstream log_file_ofstream; 
        log_file_ofstream.open(log_file);
        log_file_ofstream << msg_str << std::endl;
    }

    return SUCCESS();
}

irods::error stop(irods::default_re_ctx& _u,const std::string& _instance_name) {

    std::lock_guard<std::mutex> lock(audit_plugin_mutex);

    json_t* obj = json_object();
    if( !obj ) {
        return ERROR(SYS_MALLOC_ERR, "json_object() failed");
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    std::stringstream time_str; time_str << time_ms;
    json_object_set(obj, "time_stamp", json_string(time_str.str().c_str()));

    char host_name[MAX_NAME_LEN];
    gethostname( host_name, MAX_NAME_LEN );
    json_object_set(obj, "hostname", json_string(host_name));

    pid_t pid = getpid();
    std::stringstream pid_str; pid_str << pid;
    json_object_set(obj, "pid", json_string(pid_str.str().c_str()));

    json_object_set(obj, "action", json_string("END"));

    std::string log_file;
    if (test_mode) {
        log_file = str(boost::format("%s/%06i.txt") % log_path_prefix % pid);
        json_object_set(obj, "log_file", json_string(log_file.c_str()));

    }

    char* tmp_buf = json_dumps( obj, JSON_INDENT( 0 ) );
    std::string msg_str = std::string("__BEGIN_JSON__") + std::string(tmp_buf) + std::string("__END_JSON__");

    pn_message_t * message;
    pn_data_t * body;

    message = pn_message();
    std::string address = audit_amqp_location + "/" + audit_amqp_topic;
    pn_message_set_address(message, address.c_str());

    body = pn_message_body(message);

    pn_data_put_string(body, pn_bytes(msg_str.length(), msg_str.c_str()));
    pn_messenger_put(messenger, message);
    pn_messenger_send(messenger, -1);
   
    pn_message_free(message);

    free(tmp_buf);
    json_decref(obj);

    pn_messenger_stop(messenger);
    pn_messenger_free(messenger);

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
    const std::string&            _rn,
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

    json_t* obj = json_object();
    if( !obj ) {
        return ERROR(
                  SYS_MALLOC_ERR,
                  "json_object() failed");
    }

    struct timeval tv;
    gettimeofday(&tv, NULL);
    unsigned long time_ms = tv.tv_sec * 1000 + tv.tv_usec / 1000;
    std::stringstream time_str; time_str << time_ms;
    json_object_set(
        obj,
        "time_stamp",
        json_string(time_str.str().c_str()));

    char host_name[MAX_NAME_LEN];
    gethostname( host_name, MAX_NAME_LEN );
    json_object_set(
        obj,
        "hostname",
        json_string(host_name));

    pid_t pid = getpid();
    std::stringstream pid_str; pid_str << pid;
    json_object_set(
        obj,
        "pid",
        json_string(pid_str.str().c_str()));

    json_object_set(
        obj,
        "rule_name",
        json_string(_rn.c_str()));

    for( auto itr : _ps ) {
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

        for( auto elem : param ) {

            size_t ctr = insert_arg_into_counter_map(arg_type_map, elem.first);
            std::stringstream ctr_str;
            ctr_str << ctr;
            
            std::string key = elem.first;
            if (ctr > 1) {
                key += "__";
                key += ctr_str.str();
            }

            json_object_set(
                obj,
                key.c_str(),
                json_string(elem.second.c_str()));

            ++ctr; 
            ctr_str.clear();

        } // for elem
    } // for itr

    char* tmp_buf = json_dumps( obj, JSON_INDENT( 0 ) );
    std::string msg_str = std::string("__BEGIN_JSON__") + std::string(tmp_buf) + std::string("__END_JSON__");

    pn_message_t * message;
    pn_data_t * body;

    message = pn_message();

    std::string address = audit_amqp_location + "/" + audit_amqp_topic;

    pn_message_set_address(message, address.c_str());
    body = pn_message_body(message);
    pn_data_put_string(body, pn_bytes(msg_str.length(), msg_str.c_str()));
    pn_messenger_put(messenger, message);
    pn_messenger_send(messenger, -1);
    
    pn_message_free(message);

    free(tmp_buf);
    json_decref(obj);

    if (test_mode) {
        log_file_ofstream << msg_str << std::endl;
    }

    return err;
}

irods::error exec_rule_text(irods::default_re_ctx&, const std::string& _rt, std::list<boost::any>& _ps, irods::callback _eff_hdlr) {
    return ERROR(SYS_NOT_SUPPORTED,"not supported");
}

irods::error exec_rule_expression(irods::default_re_ctx&, const std::string& _rt, std::list<boost::any>& _ps, irods::callback _eff_hdlr) {
    return ERROR(SYS_NOT_SUPPORTED,"not supported");
}

extern "C"
irods::pluggable_rule_engine<irods::default_re_ctx>* plugin_factory( const std::string& _inst_name,
                                 const std::string& _context ) {
    irods::pluggable_rule_engine<irods::default_re_ctx>* re = new irods::pluggable_rule_engine<irods::default_re_ctx>( _inst_name , _context);
    re->add_operation<irods::default_re_ctx&,const std::string&>(
            "start",
            std::function<irods::error(irods::default_re_ctx&,const std::string&)>( start ) );

    re->add_operation<irods::default_re_ctx&,const std::string&>(
            "stop",
            std::function<irods::error(irods::default_re_ctx&,const std::string&)>( stop ) );

    re->add_operation<irods::default_re_ctx&, const std::string&, bool&>(
            "rule_exists",
            std::function<irods::error(irods::default_re_ctx&, const std::string&, bool&)>( rule_exists ) );

    re->add_operation<irods::default_re_ctx&, std::vector<std::string>&>(
            "list_rules",
            std::function<irods::error(irods::default_re_ctx&, std::vector<std::string>&)>( list_rules ) );

    re->add_operation<irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback>(
            "exec_rule",
            std::function<irods::error(irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback)>( exec_rule ) );

    re->add_operation<irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback>(
            "exec_rule_text",
            std::function<irods::error(irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback)>( exec_rule_text ) );

    re->add_operation<irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback>(
            "exec_rule_expression",
            std::function<irods::error(irods::default_re_ctx&,const std::string&,std::list<boost::any>&,irods::callback)>( exec_rule_expression ) );

    return re;

}
