# iRODS Rule Engine Plugin - Audit

This C++ plugin provides the iRODS platform a rule engine that emits a single AMQP message to the configured topic for every policy enforcement point (PEP) encountered by the iRODS server.

# Build

Building the iRODS Audit Rule Engine Plugin requires version 4.2 of the iRODS software from github (http://github.com/irods/irods).

```
cd irods_rule_engine_plugin_audit_amqp
mkdir build
cd build
cmake ../
make package
```

# Install

The packages produced by CMake will install the Audit plugin shared object file:

`/var/lib/irods/plugins/rule_engines/libirods_rule_engine_plugin-audit_amqp.so`

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

Further information on this plugin is described in the slide deck available here: http://slides.com/irods/ugm2016-auditing-rule-engine-amqp

