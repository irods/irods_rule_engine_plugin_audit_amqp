# iRODS Rule Engine Plugin - Audit via AMQP 1.0

This C++ plugin provides the iRODS platform a rule engine that can emit a single AMQP 1.0 message to the configured topic for every policy enforcement point (PEP) encountered by the iRODS server.

# Build

Building the iRODS Audit Rule Engine Plugin requires iRODS 4.3.0+ (http://github.com/irods/irods).

This plugin requires the iRODS development and runtime packages to be installed on the build machine.

Also, use the iRODS-built CMake (or CMake 3.11+):

```
export PATH=/opt/irods-externals/cmake3.21.4-0/bin:$PATH
```

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

After installation, the plugin must be added to the `rule_engines` array in the server configuration (`/etc/irods/server_config.json`) in order to be used.

## Plugin-Specific Configuration Stanza Structure

The JSON block below represents all supported configuration options in `plugin_specific_configuration`.

> [!NOTE]
> Starting with @VERSION@, we expose most of the Qpid Proton settings as configuration options prefixed with `amqp_` in the plugin configuration. In the interest of not making the plugin more complicated than it already is, most of these configuration options coorespond directly or almost directly with a Qpid Proton setting. If more insight is needed into a configuration option than is provided here, [Qpid Proton's C++ API documentation](https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/annotated.html) may be helpful. 

> [!IMPORTANT]
> The comments in the following JSON block are present in this README for documentation puposes only. iRODS does not support comments in the server configuration file and will fail to load the configuration if any are present.

> [!WARNING]
> If configuration loading fails during a reload, the values from the previous successfully loaded configuration will be used except where otherwise noted. If

```js
{
    // How plugin failures should be handled. Must be one of the following (case-insensitive):
    // - `BLOCK_OPERATION`: Block operations matching pep_regex_to_match when audit messages cannot
    //                      be sent or plugin is in an error state.
    // - `ALLOW_OPERATION`: Allow operations to continue when audit messages cannot be sent or
    //                      plugin is in an error state.
    // Optional. Default is `BLOCK_OPERATION`. Will be required in a future version of the plugin.
    // Previous value always discarded during reload. If this option fails to load, the value will
    // be assumed to be `BLOCK_OPERATION`.
    "failsafe_mode": "BLOCK_OPERATION",

    // Regular expression to match PEPs to generate audit messages for.
    // Follows the C++ variant of EMCAScript grammar: https://en.cppreference.com/w/cpp/regex/ecmascript.html
    // REQUIRED. Previous value discarded during reload unless this option fails to load and
    // `failsafe_mode` is `ALLOW_OPERATION`. Fallback is unset, which results in errors from
    // `rule_exists`.
    "pep_regex_to_match": "pep_.+",

    // List of AMQP endpoints to send messages to. Must have at least one endpoint.
    // All strings are used as-is, so some urlencoding may be necessary.
    // REQUIRED. Fallback is `amqp://localhost:5672`.
    "amqp_endpoints": [
        {
            // AMQP URL scheme. Known valid schemes are `amqp` and `amqps`.
            // Optional. Default is unset.
            "scheme": "amqp",
            // Host to establish AMQP connection to.
            // REQUIRED.
            "host": "localhost",
            // Port on host to connect to. When unset, will derive based on `scheme`.
            // Optional. Default is unset.
            "port": 5672,
            // AMQP URL parameters. Values can be strings or `null`.
            // Optional. Default is unset.
            "parameters": {
                "param1": "value1",
                "param2": null
            },
            // AMQP URL fragment.
            // Optional. Default is unset.
            "fragment": "frag"
        }
        // The above endpoint results in the URL `amqp://localhost:5672/?param1=value1&param2#frag`.
    ],

    // Username for authenticating with AMQP endpoints. Will attempt to connect anonymously if
    // unset.
    // Optional. Default is unset.
    "amqp_user": "USERNAME",
    // Password for authenticating with AMQP endpoints.
    // Optional. Default is unset.
    "amqp_password": "PASSWORD",

    // AMQP target address path. Typically a queue or exchange. String used as-is, so some
    // urlencoding may be necessary.
    // See https://www.rabbitmq.com/docs/amqp#target-address-v2
    // REQUIRED. Fallback is `queues/irods_audit_messages`.
    "amqp_path": "queues/audit_messages",
    // AMQP target address parameters. Values can be strings or `null`. Must be urlencoded. Strings
    // are used as-is, so some urlencoding may be necessary.
    // Optional. Default is unset.
    "amqp_path_parameters": {
        "param1": "value1",
        "param2": null
    },
    // AMQP target address fragment. String used as-is, so some urlencoding may be necessary.
    // Optional. Default is unset.
    "amqp_path_fragment": "",
    // The above amqp_path* configuration results in the URI `/queues/audit_messages?param1=value1&param2#`.

    // SSL options for AMQP connection.
    // Optional. Default is unset (equivalent to leaving all sub-options as defaults).
    "amqp_ssl": {
        // Peer validation level. Must be one of the following (case-insensitive):
        // - `VERIFY_PEER`: require peer to provide valid identifying certificate.
        // - `ANONYMOUS_PEER`: no certificate nor cipher authorization required.
        // - `VERIFY_PEER_NAME`: require valid certificate and matching name.
        // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/c/api/group__ssl.html#gae5e33024ed6af3432d4c76d1484d7ecb
        // Optional. Default is unset (equvalent to `VERIFY_PEER_NAME`).
        "verify_mode": "ANONYMOUS_PEER",
        // Database of trusted CAs, used to authenticate peers. Uses system trust database if
        // unset.
        // Optional. Default is unset.
        "trust_db": "",
        // Database containing client certificate.
        // Optional. Default is unset. Ignored if `trust_db` is unset.
        "certdb_main": "",
        // Key to access client certificate.
        // Optional. Default is unset. Ignored if `certdb_main` is unset.
        "certdb_extra": "",
        // Password for client certificate.
        // Optional. Default is unset. Ignored if `certdb_extra` is unset.
        "cert_password": ""
    },

    // SASL options for AMQP authentication.
    // Optional. Default is unset (equivalent to leaving all sub-options as defaults).
    "amqp_sasl": {
        // Whether or not SASL is enabled for authentication with the AMQP endpoints.
        // Optional. Default is unset (equivalent to `true`).
        "enable": true,
        // List of SASL mechanisms that are to be considered for authentication. Can be an array of
        // strings, or a single space-delimited string.
        // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/c/api/group__sasl.html#ga73299a6a22e141e7911a739590032625
        // Optional. Default is unset.
        "mechanisms": [
            "PLAIN",
            "ANONYMOUS"
        ],
        // Whether or not to allow use of clear text authentication methods.
        // Optional. Default is unset (equivalent to `false`).
        "allow_insecure": true
    },

    // Maximum frame size for AMQP connection. Unlimited if unset.
    // Optional. Default is unset.
    "amqp_connection_max_frame_size": 65536,
    // Maximum number of open sessions for AMQP connection.
    // Optional. Default is unset (equivalent to 32767).
    "amqp_connection_max_sessions": 32767,
    // Idle timeout in milliseconds for AMQP connection. No timeout if unset.
    // Optional. Default is unset.
    "amqp_connection_idle_timeout": 10000,
    // Virtual host name for AMQP connection.
    // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/classproton_1_1connection__options.html#a0300a2a68ecca3f684e91e637a45e028
    // Optional. Default is unset.
    "amqp_connection_virtual_host": "localhost",
    // Timeout in milliseconds for establishing AMQP connection. 0 means no timeout.
    // Optional. Default is 5000.
    "amqp_connection_open_timeout": 5000,
    // Timeout in milliseconds for closing AMQP connection. 0 means no timeout.
    // Optional. Default is 3000.
    "amqp_connection_close_timeout": 3000,
    // Base value in milliseconds for reconnection delays.
    // Optional. Default is unset (equivalent to 10).
    "amqp_reconnect_base_delay": 100,
    // Scaling multiplier for successive reconnect delays.
    // Optional. Default is unset (equivalent to 2.0).
    "amqp_reconnect_delay_multiplier": 1.25,
    // Maximum delay in milliseconds between successive connect attempts. Unlimited if unset.
    // Optional. Default is unset.
    "amqp_reconnect_max_delay": 10000,
    // Maximum number of reconnect attempts. 0 is equivalent to no limit.
    // Optional. Default is unset (equivalent to 0 or no limit).
    "amqp_reconnect_max_attempts": 15,

    // Options for AMQP sender.
    // Optional. Default is unset (equivalent to leaving all sub-options as defaults).
    "amqp_sender": {
        // Message delivery policy to establish when opening a link.
        // Must be one of the following (case-insensitive):
        // - `NONE`: No set policy. The application must settle messages itself according to its
        //           own policy.
        // - `AT_MOST_ONCE`: Outgoing messages are settled immediately by the link.
        // - `AT_LEAST_ONCE`: The receiver settles the delivery first with an accept/reject/release
        //                    disposition. The sender waits to settle until after the disposition
        //                    notification is received.
        // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/structproton_1_1delivery__mode.html#a811fe196a5d9d37857c2f8adeeaac3c6
        // Optional. Default is unset (equivalent to `NONE`).
        "delivery_mode": "NONE",
        // Whether to automatically settle messages.
        // Optional. Default is unset (equivalent to `true`).
        "auto_settle": true,
        // Timeout in milliseconds for opening AMQP sender. 0 means no timeout.
        // Optional. Default is 3000.
        "open_timeout": 3000,
        // Timeout in milliseconds for closing AMQP sender. 0 means no timeout.
        // Optional. Default is 3000.
        "close_timeout": 3000,

        // Options for source node.
        // Optional. Default is unset (equivalent to leaving all sub-options as defaults).
        "source": {
            // Address for the node. Ignored if `dynamic` is `true`.
            // Optional. Default is unset.
            "address": "",
            // Request that a node be dynamically created by the remote peer.
            // Optional. Default is unset (equivalent to `false`).
            "dynamic": true,
            // Request an anonymous node on the remote peer.
            // Optional. Default is unset (equivalent to `true`).
            "anonymous": false,
            // Control whether messages are browsed or consumed.
            // Must be one of the following (case-insensitive):
            // - `UNSPECIFIED`: The behavior is defined by the node.
            // - `COPY`: Once transferred, the message remains available to other links.
            // - `MOVE`: Once transferred, the message is unavailable to other links.
            // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/c/api/group__terminus.html#gac6fb89a5fa96476db51b60f10dc785d0
            // Optional. Default is unset (equivalent to `MOVE`).
            "distribution_mode": "MOVE",
            // Persistence of the source node. Must be one of the following (case-insensitive):
            // - `NONDURABLE`: No persistence.
            // - `CONFIGURATION`: Only configuration is persisted.
            // - `UNSETTLED_STATE`: Configuration and delivery state are persisted.
            // - `DELIVERIES`: Same as `UNSETTLED_STATE`.
            // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/classproton_1_1terminus.html#a61db0571ab7d1a29ad77549ff99d6b3d
            // Optional. Default is unset (equivalent to `NONDURABLE`).
            "durability_mode": "NONDURABLE",
            // How long in milliseconds an orphaned source can persist. No timeout if unset.
            // Optional. Default is unset.
            "timeout": 604800000,
            // When a source is considered orphaned.
            // Must be one of the following (case-insensitive):
            // - `LINK_CLOSE`: When the link is closed.
            // - `SESSION_CLOSE`: When the containing session is closed.
            // - `CONNECTION_CLOSE`: When the containing connection is closed.
            // - `NEVER`: Never.
            // See: https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/c/api/group__terminus.html#ga213267363be45848f3299471ea93089b
            // Optional. Default is unset (equivalent to `LINK_CLOSE`).
            "expiry_policy": "LINK_CLOSE"
        },

        // Options for target node.
        // Optional. Default is unset (equivalent to leaving all sub-options as defaults).
        "target": {
            // Address for the node. Ignored if `dynamic` is `true`.
            // Optional. Default is unset.
            "address": "",
            // Request that a node be dynamically created by the remote peer.
            // Optional. Default is unset (equivalent to `false`).
            "dynamic": true,
            // Request an anonymous node on the remote peer.
            // Optional. Default is unset (equivalent to `true`).
            "anonymous": false,
            // Persistence of the target node. Must be one of the following (case-insensitive):
            // - `NONDURABLE`: No persistence.
            // - `CONFIGURATION`: Only configuration is persisted.
            // - `UNSETTLED_STATE`: Configuration and delivery state are persisted.
            // - `DELIVERIES`: Same as `UNSETTLED_STATE`.
            // See https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/cpp/api/classproton_1_1terminus.html#a61db0571ab7d1a29ad77549ff99d6b3d
            // Optional. Default is `UNSETTLED_STATE`. Can be set to `null` to unset explicitly (equivalent to
            // `NONDURABLE`).
            "durability_mode": "UNSETTLED_STATE",
            // How long in milliseconds an orphaned target can persist. No timeout if unset.
            // Optional. Default is unset.
            "timeout": 604800000,
            // When a target is considered orphaned.
            // Must be one of the following (case-insensitive):
            // - `LINK_CLOSE`: When the link is closed.
            // - `SESSION_CLOSE`: When the containing session is closed.
            // - `CONNECTION_CLOSE`: When the containing connection is closed.
            // - `NEVER`: Never.
            // See: https://qpid.apache.org/releases/qpid-proton-0.36.0/proton/c/api/group__terminus.html#ga213267363be45848f3299471ea93089b
            // Optional. Default is unset (equivalent to `LINK_CLOSE`).
            "expiry_policy": "LINK_CLOSE"
        }
    },

    // Whether to set the durable flag on outgoing AMQP messages.
    // Optional. Default is `true`. Can be set to `null` to unset explicitly (equivalent to
    // `false`).
    "amqp_durable_messages": true,
    // Timeout in milliseconds for sending individual AMQP messages. 0 means no timeout.
    // Optional. Default is 5000.
    "amqp_message_send_timeout": 5000,

    // Timeout in milliseconds for opening AMQP session. 0 means no timeout.
    // Optional. Default is 3000.
    "amqp_session_open_timeout": 3000
    // Timeout in milliseconds for closing AMQP session. 0 means no timeout.
    // Optional. Default is 3000.
    "amqp_session_close_timeout": 3000
}
```

## Example Rule Engine Instance Configuration Stanza

```json
{
    "instance_name": "irods_rule_engine_plugin-audit_amqp-instance",
    "plugin_name": "irods_rule_engine_plugin-audit_amqp",
    "plugin_specific_configuration": {
        "failsafe_mode": "BLOCK_OPERATION",
        "pep_regex_to_match": "pep_.+",
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
            "enable": true,
            "allow_insecure": true
        },
        "amqp_connection_max_frame_size": 65536,
        "amqp_sender": {
            "delivery_mode": "AT_LEAST_ONCE",
            "target": {
                "durability_mode": "UNSETTLED_STATE"
            }
        },
        "amqp_durable_messages": true
    }
}
```

Further information on this plugin is described in the slide deck available here: http://slides.com/irods/ugm2016-auditing-rule-engine-amqp

Citations:

Hao Xu, Jason Coposky, Ben Keller, Terrell Russell (2015) Pluggable Rule Engine Architecture. 7th iRODS User Group Meeting, University of North Carolina at Chapel Hill. June 2015. ([PDF](https://irods.org/uploads/2015/01/xu2015-pluggable_rule_engine.pdf))

Hao Xu, Jason Coposky, Dan Bedard, Jewel H. Ward, Terrell Russell, Arcot Rajasekar, Reagan Moore, Ben Keller, Zoey Greer (2015) A Method for the Systematic Generation of Audit Logs in a Digital Preservation Environment and Its Experimental Implementation In a Production Ready System. 12th International Conference on Digital Preservation, University of North Carolina at Chapel Hill. November 2-6, 2015. ([PDF](https://irods.org/uploads/2015/01/xu2015_ipres-preservation_audit_logs_production.pdf)) ([direct link](https://phaidra.univie.ac.at/detail_object/o:429566)) 
