from __future__ import print_function

from irods.configuration import IrodsConfig

def main():
    irods_config = IrodsConfig()
    irods_config.server_config['plugin_configuration']['rule_engines'].insert(1,
            {
                "instance_name": "irods_rule_engine_plugin-audit_amqp-instance",
                "plugin_name": "irods_rule_engine_plugin-audit_amqp",
                "plugin_specific_configuration": {
                    "failsafe_mode": "ALLOW_OPERATION",
                    "pep_regex_to_match": "audit_.*",
                    "amqp_endpoints": [
                        {
                            "scheme": "amqp",
                            "host": "localhost",
                            "port": 5672
                        }
                    ],
                    "amqp_user": "ANONYMOUS",
                    "amqp_path": "queues/audit_messages",
                    "amqp_sasl": {
                        "enable": True,
                        "allow_insecure": True
                    },
                    "amqp_connection_max_frame_size": 65536,
                    "amqp_sender": {
                        "delivery_mode": "AT_LEAST_ONCE",
                        "target": {
                            "durability_mode": "UNSETTLED_STATE"
                        }
                    },
                    "amqp_durable_messages": True,
                    "test_mode": True,
                    "log_path_prefix": "/tmp/irods"
                }
            }
        )
    irods_config.server_config["rule_engine_namespaces"].append("audit_")
    irods_config.server_config["log_level"]["rule_engine"] = "trace"
    irods_config.commit(irods_config.server_config, irods_config.server_config_path, make_backup=True)

if __name__ == '__main__':
    main()
