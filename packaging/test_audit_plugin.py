from __future__ import print_function
import json
import sys
import multiprocessing
import os
import shutil

from . import session
from .. import lib
from .queue_listener import QueueListener

if sys.version_info < (2, 7):
    import unittest2 as unittest
else:
    import unittest

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
                self.url = rule_engine["plugin_specific_configuration"]["amqp_location"]
                self.queue_name = rule_engine["plugin_specific_configuration"]["amqp_topic"]
                log_directory = rule_engine["plugin_specific_configuration"]["log_path_prefix"]

        # create log directory
        if not os.path.exists(log_directory):
            os.makedirs(log_directory)

    def tearDown(self):
        filepath = os.path.abspath(self.largetestfile)
        if os.path.exists(filepath):
            os.unlink(filepath)

    def test_audit_plugin(self):
        filename = self.largetestfile

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iput -f {filename}'.format(**locals()), 'EMPTY')
            admin_session.assert_icommand('iget -f {filename}'.format(**locals()), 'EMPTY')
            admin_session.assert_icommand('irm -f {filename}'.format(**locals()), 'EMPTY')

        # Establish communication queues
        pid_queue = multiprocessing.JoinableQueue()
        result_queue = multiprocessing.Queue()
        listener = QueueListener(pid_queue, result_queue, self.url, self.queue_name)
        listener.run()

        print("result queue size is ", result_queue.qsize())
        self.assertEqual(0, pid_queue.qsize(), "the joinable queue pid_queue should be empty")
        self.assertTrue(1 <= result_queue.qsize(), "the result queue size should at least be one")
        for i in range(result_queue.qsize()):
            result = result_queue.get()
            self.assertEqual(result, 'passed')

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

