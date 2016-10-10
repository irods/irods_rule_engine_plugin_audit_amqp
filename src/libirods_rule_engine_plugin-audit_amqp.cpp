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

// =-=-=-=-=-=-=-
// boost includes
#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>

// =-=-=-=-=-=-=-
// proton includes
#include "proton/message.h"
#include "proton/messenger.h"

#include "jansson.h"


static std::string audit_pep_regex_to_match = "audit_.*";
static std::string audit_amqp_topic         = "irods_audit_messages";
static std::string audit_amqp_location      = "localhost:5672";
static std::string audit_amqp_options       = "";

irods::error get_re_configs(
    const std::string& _instance_name ) {
    try {
        const auto& rule_engines = irods::get_server_property< const std::vector< boost::any >& >(std::vector<std::string>{ irods::CFG_PLUGIN_CONFIGURATION_KW, irods::PLUGIN_TYPE_RULE_ENGINE } );
        for ( const auto& elem : rule_engines ) {
            const auto& rule_engine = boost::any_cast< const std::unordered_map< std::string, boost::any >& >( elem );
            const auto& inst_name = boost::any_cast< const std::string& >( rule_engine.at( irods::CFG_INSTANCE_NAME_KW ) );
            if ( inst_name == _instance_name ) {
                if ( rule_engine.count( irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW ) > 0 ) {
                    const auto& plugin_spec_cfg = boost::any_cast< const std::unordered_map< std::string, boost::any >& >( rule_engine.at( irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW ) );

                    audit_pep_regex_to_match = boost::any_cast< std::string >( plugin_spec_cfg.at( "pep_regex_to_match" ) );
                    audit_amqp_topic         = boost::any_cast< std::string >( plugin_spec_cfg.at( "amqp_topic" ) );
                    audit_amqp_location      = boost::any_cast< std::string >( plugin_spec_cfg.at( "amqp_location" ) );
                    audit_amqp_options       = boost::any_cast< std::string >( plugin_spec_cfg.at( "amqp_options" ) );
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

    irods::error ret = get_re_configs( _instance_name );
    if( !ret.ok() ) {
        irods::log(PASS(ret));
    }

    return SUCCESS();
}
#include "proton/message.h"
#include "proton/messenger.h"

irods::error stop(irods::default_re_ctx& _u,const std::string&) {

    return SUCCESS();
}

irods::error rule_exists(irods::default_re_ctx&, std::string _rn, bool& _ret) {
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

irods::error exec_rule(
    irods::default_re_ctx&,
    std::string            _rn,
    std::list<boost::any>& _ps,
    irods::callback        _eff_hdlr) {

    using namespace std::chrono;

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


    system_clock::time_point tp = system_clock::now();
    std::time_t t = system_clock::to_time_t(tp);
    std::stringstream time_str; time_str << std::ctime(&t);
    json_object_set(
        obj,
        "0__time_stamp",
        json_string(time_str.str().c_str()));

    char host_name[MAX_NAME_LEN];
    gethostname( host_name, MAX_NAME_LEN );
    json_object_set(
        obj,
        "1__hostname",
        json_string(host_name));

    pid_t pid = getpid();
    std::stringstream pid_str; pid_str << pid;
    json_object_set(
        obj,
        "2__pid",
        json_string(pid_str.str().c_str()));

    json_object_set(
        obj,
        "3__rule_name",
        json_string(_rn.c_str()));

    size_t ctr = 3;
    for( auto itr : _ps ) {
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
            std::stringstream ctr_str;
            ctr_str << ctr;
            
            std::string key;
            key += ctr_str.str();
            key += "__";
            key += elem.first;

            json_object_set(
                obj,
                key.c_str(),
                json_string(elem.second.c_str()));
            
            ++ctr;
            ctr_str.clear();

        } // for elem
    } // for itr

    //char* tmp_buf = json_dumps( obj, JSON_INDENT( 0 ) );
    char* tmp_buf = json_dumps( obj, JSON_COMPACT | JSON_SORT_KEYS );

    
    std::string msg(tmp_buf);
    //rodsLog(LOG_NOTICE, "msg=%s", msg.c_str());

    pn_message_t * message;
    pn_messenger_t * messenger;
    pn_data_t * body;

    message = pn_message();
    messenger = pn_messenger(NULL);
  
    pn_messenger_start(messenger);

    std::string address = audit_amqp_location + "/" + audit_amqp_topic;

    pn_message_set_address(message, address.c_str());
    body = pn_message_body(message);
    pn_data_put_string(body, pn_bytes(strlen(tmp_buf), tmp_buf));
    pn_messenger_put(messenger, message);
    //rodsLog(LOG_NOTICE, "pn_messenger_put errno = %i", pn_messenger_errno(messenger));
    pn_messenger_send(messenger, -1);
    //rodsLog(LOG_NOTICE, "pn_messenger_send errno = %i", pn_messenger_errno(messenger));

    pn_messenger_stop(messenger);
    pn_messenger_free(messenger);
    pn_message_free(message);



    free(tmp_buf);
    json_decref(obj);

    return err;
}

irods::error exec_rule_text(irods::default_re_ctx&, std::string _rt, std::list<boost::any>& _ps, irods::callback _eff_hdlr) {
    return ERROR(SYS_NOT_SUPPORTED,"not supported");
}

irods::error exec_rule_expression(irods::default_re_ctx&, std::string _rt, std::list<boost::any>& _ps, irods::callback _eff_hdlr) {
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

    re->add_operation<irods::default_re_ctx&, std::string, bool&>(
            "rule_exists",
            std::function<irods::error(irods::default_re_ctx&, std::string, bool&)>( rule_exists ) );

    re->add_operation<irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback>(
            "exec_rule",
            std::function<irods::error(irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback)>( exec_rule ) );
    re->add_operation<irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback>(
            "exec_rule_text",
            std::function<irods::error(irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback)>( exec_rule_text ) );
    re->add_operation<irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback>(
            "exec_rule_expression",
            std::function<irods::error(irods::default_re_ctx&,std::string,std::list<boost::any>&,irods::callback)>( exec_rule_expression ) );

    return re;

}
