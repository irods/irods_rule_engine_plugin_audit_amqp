# iRODS Rule Engine Plugin - Audit via AMQP

This C++ plugin provides the iRODS platform a rule engine that emits a single AMQP message to the configured topic for every policy enforcement point (PEP) encountered by the iRODS server.

# Build

Building the iRODS Audit Rule Engine Plugin requires iRODS 4.2.1 (http://github.com/irods/irods).

```
cd irods_rule_engine_plugin_audit_amqp
mkdir build
cd build
cmake ../
make package
```

# Install

The packages produced by CMake will install the Audit plugin shared object file:

`/usr/lib/irods/plugins/rule_engines/libirods_rule_engine_plugin-audit_amqp.so`

# Configuration

After installing the plugin, `/etc/irods/server_config.json` needs to be configured to use the plugin.

Add a new stanza to the "rule_engines" array within `server_config.json`:

```json
{
    "instance_name": "irods_rule_engine_plugin-audit_amqp-instance",
    "plugin_name": "irods_rule_engine_plugin-audit_amqp",
    "plugin_specific_configuration" : {
        "pep_regex_to_match" : "audit_.*",
        "amqp_topic" : "amq.topic",
        "amqp_location" : "localhost:5672",
        "amqp_options" : ""
    }
}
```

Add the new `audit_` namespace to the "rule_engine_namespaces" array within `server_config.json`:

```
    "rule_engine_namespaces": [
        "", 
        "audit_"
    ], 
```

Further information on this plugin is described in the slide deck available here: http://slides.com/irods/ugm2016-auditing-rule-engine-amqp

Citations:

Hao Xu, Jason Coposky, Ben Keller, Terrell Russell (2015) Pluggable Rule Engine Architecture. 7th iRODS User Group Meeting, University of North Carolina at Chapel Hill. June 2015. ([PDF](https://irods.org/uploads/2015/01/xu2015-pluggable_rule_engine.pdf))

Hao Xu, Jason Coposky, Dan Bedard, Jewel H. Ward, Terrell Russell, Arcot Rajasekar, Reagan Moore, Ben Keller, Zoey Greer (2015) A Method for the Systematic Generation of Audit Logs in a Digital Preservation Environment and Its Experimental Implementation In a Production Ready System. 12th International Conference on Digital Preservation, University of North Carolina at Chapel Hill. November 2-6, 2015. ([PDF](https://irods.org/uploads/2015/01/xu2015_ipres-preservation_audit_logs_production.pdf)) ([direct link](https://phaidra.univie.ac.at/detail_object/o:429566)) 
