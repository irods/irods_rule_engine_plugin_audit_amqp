from __future__ import print_function
import json
import sys
import multiprocessing
import os
import shutil
import unittest

from . import session
from .. import lib
from .. import paths
from ..configuration import IrodsConfig
from ..controller import IrodsController
from .queue_listener import QueueListener
from .test_resource_types import Test_Resource_Unixfilesystem

class TestAuditPlugin(unittest.TestCase):

    def setUp(self):
        # Create a test file
        self.largetestfile = "largefile.txt"
        lib.make_file(self.largetestfile, 64*1024*1024, 'arbitrary')

        with open('/etc/irods/server_config.json') as config_file:
            config = json.load(config_file)

        rule_engines = config["plugin_configuration"]["rule_engines"]
        for rule_engine in rule_engines:
            if rule_engine["instance_name"] == "irods_rule_engine_plugin-audit_amqp-instance":
                rule_engine_cfg = rule_engine["plugin_specific_configuration"]

                # Fetch and construct AMQP endpoint URLs
                self.amqp_endpoints = []
                first_endpoint_done = False
                for endpoint in rule_engine_cfg["amqp_endpoints"]:
                    scheme = endpoint.get("scheme", "")
                    scheme = (scheme + "://") if scheme else ""
                    if not first_endpoint_done:
                        self.amqp_location_scheme = scheme
                    host = endpoint["host"]
                    port = str(endpoint.get("port", ""))
                    port_pre = ":" if port else ""
                    e_params = "&".join([k if v is None else f"{k}={v}" for k, v in endpoint.get("parameters", {}).items()])
                    e_params_pre = "?" if e_params else ""
                    e_frag = ""
                    e_frag_pre = ""
                    if "fragment" in endpoint:
                        e_frag = endpoint["fragment"]
                        e_frag_pre = "#"
                        e_frag = "" if e_frag is None else e_frag
                    port_post = "/" if e_params or e_frag_pre else ""
                    endpoint_wo_scheme = f"{host}{port_pre}{port}{port_post}{e_params_pre}{e_params}{e_frag_pre}{e_frag}"
                    if not first_endpoint_done:
                        self.amqp_location = endpoint_wo_scheme
                        first_endpoint_done = True
                    self.amqp_endpoints.append(scheme + endpoint_wo_scheme)

                # Fetch AMQP credentials
                self.amqp_user = rule_engine_cfg.get("amqp_user", None)
                self.amqp_password = rule_engine_cfg.get("amqp_password", None)

                # Fetch and construct AMQP path URI
                path = rule_engine_cfg.get("amqp_path", None)
                path_pre = "" if path is None else "/"
                path = "" if path is None else path
                p_params = "&".join([k if v is None else f"{k}={v}" for k, v in rule_engine_cfg.get("amqp_path_parameters", {}).items()])
                p_parms_pre = "?" if p_params else ""
                p_frag = ""
                p_frag_pre = ""
                if "amqp_path_fragment" in rule_engine_cfg:
                    p_frag = rule_engine_cfg["amqp_path_fragment"]
                    p_frag_pre = "#"
                    p_frag = "" if p_frag is None else p_frag
                self.amqp_path = f"{path_pre}{path}{p_parms_pre}{p_params}{p_frag_pre}{p_frag}"

        # Reload configuration after edits are made so that they take effect in the server.
        IrodsController().reload_configuration()

    def tearDown(self):
        filepath = os.path.abspath(self.largetestfile)
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_audit_plugin(self):
        filename = self.largetestfile

        try:
            with session.make_session_for_existing_admin() as admin_session:
                admin_session.assert_icommand('iput -f {filename}'.format(**locals()), 'EMPTY')
                admin_session.assert_icommand('iget -f {filename}'.format(**locals()), 'EMPTY')
                admin_session.assert_icommand('irm -f {filename}'.format(**locals()), 'EMPTY')

            # Stop the server here because if we don't, messages will just keep coming in from the delay server
            # and other internal processes and it will just keep processing the messages forever.
            IrodsController().stop()

            # Establish communication queues
            pid_queue = multiprocessing.JoinableQueue()
            result_queue = multiprocessing.Queue()
            listener = QueueListener(pid_queue, result_queue, self.amqp_endpoints, self.amqp_user, self.amqp_password, self.amqp_path)
            listener.run()

            print("result queue size is ", result_queue.qsize())
            self.assertEqual(0, pid_queue.qsize(), "the joinable queue pid_queue should be empty")
            self.assertTrue(1 <= result_queue.qsize(), "the result queue size should at least be one")
            for i in range(result_queue.qsize()):
                result = result_queue.get()
                self.assertEqual(result, 'passed')

        finally:
            IrodsController().restart(test_mode=True)

    def test_delayed_rule_with_plugin_configured(self):
        rep_name = 'irods_rule_engine_plugin-audit_amqp-instance'
        rule_file = "test_audit_plugin_delayed_rule.r"
        rule_string = '''
test_audit_plugin_delayed_rule {{
    delay("<INST_NAME>{}</INST_NAME><PLUSET>1s</PLUSET>") {{
        *i = 0;
    }}
}}
INPUT null
OUTPUT ruleExecOut
'''.format(rep_name)

        with open(rule_file, 'w') as f:
            f.write(rule_string)

        try:
            with session.make_session_for_existing_admin() as admin_session:
                admin_session.assert_icommand(['irule', '-r', rep_name, '-F', rule_file], 'STDERR', 'SYS_NOT_SUPPORTED')

        finally:
            os.unlink(rule_file)

    def test_deprecated_endpoint_configuration__issue_106_109(self):
        try:
            with lib.file_backed_up(paths.server_config_path()):
                irods_config = IrodsConfig()
                for rule_engine in irods_config.server_config["plugin_configuration"]["rule_engines"]:
                    if rule_engine["instance_name"] == "irods_rule_engine_plugin-audit_amqp-instance":
                        rule_engine_cfg = rule_engine["plugin_specific_configuration"]

                        # remove AMQP endpoint/credentials/path config
                        rule_engine_cfg.pop("amqp_endpoints", None)
                        rule_engine_cfg.pop("amqp_user", None)
                        rule_engine_cfg.pop("amqp_password", None)
                        rule_engine_cfg.pop("amqp_path", None)
                        rule_engine_cfg.pop("amqp_path_parameters", None)
                        rule_engine_cfg.pop("amqp_path_fragment", None)

                        # construct and set amqp_location
                        cred_part = self.amqp_password or ""
                        if cred_part:
                            cred_part = (self.amqp_user or "ANONYMOUS") + ":" + cred_part + "@"
                        elif self.amqp_user:
                            cred_part = self.amqp_user + "@"
                        rule_engine_cfg["amqp_location"] = self.amqp_location_scheme + self.amqp_location

                        # set amqp_topic
                        rule_engine_cfg["amqp_topic"] = self.amqp_path
                print(irods_config.server_config)
                irods_config.commit(irods_config.server_config, irods_config.server_config_path, make_backup=True)
                # Reload configuration after edits are made so that they take effect in the server.
                IrodsController().reload_configuration()
                
                self.test_audit_plugin()

        finally:
            # Reload configuration again after server configuration is restored.
            IrodsController().reload_configuration()


    def test_missing_test_mode_config__issue_98(self):
        with lib.file_backed_up(paths.server_config_path()):
            with session.make_session_for_existing_admin() as admin_session:
                irods_config = IrodsConfig()
                print(irods_config.server_config)
                del irods_config.server_config['plugin_configuration']['rule_engines'][1]['plugin_specific_configuration']['test_mode']
                print(irods_config.server_config)
                irods_config.commit(irods_config.server_config, irods_config.server_config_path, make_backup=True)
                # Reload configuration after edits are made so that they take effect in the server.
                IrodsController().reload_configuration()
                admin_session.assert_icommand(['ils'], 'STDOUT', admin_session.home_collection)
        # Reload configuration again after server configuration is restored.
        IrodsController().reload_configuration()


    def test_missing_log_path_prefix_config__issue_98(self):
        with lib.file_backed_up(paths.server_config_path()):
            with session.make_session_for_existing_admin() as admin_session:
                irods_config = IrodsConfig()
                print(irods_config.server_config)
                del irods_config.server_config['plugin_configuration']['rule_engines'][1]['plugin_specific_configuration']['log_path_prefix']
                print(irods_config.server_config)
                irods_config.commit(irods_config.server_config, irods_config.server_config_path, make_backup=True)
                # Reload configuration after edits are made so that they take effect in the server.
                IrodsController().reload_configuration()
                admin_session.assert_icommand(['ils'], 'STDOUT', admin_session.home_collection)
        # Reload configuration again after server configuration is restored.
        IrodsController().reload_configuration()


@unittest.skip('Skipping due to long run time')
class test_resource_unixfilesystem__issue_19(Test_Resource_Unixfilesystem, unittest.TestCase):
    def __init__(self, *args, **kwargs):
        # Why: Run with this REP configured in order to exercise serialization of types in REPF
        super(test_resource_unixfilesystem__issue_19, self).__init__(*args, **kwargs)

del Test_Resource_Unixfilesystem
