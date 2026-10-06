from __future__ import print_function
import time
import json

from proton.handlers import MessagingHandler


class MessageBrokerConsumer(MessagingHandler):
    def __init__(self, endpoints, user, password, path, pid_queue):
        super(MessageBrokerConsumer, self).__init__()
        self.endpoints = endpoints
        self.user = user
        self.password = password
        self.path = path
        self.pid_dictionary = {}
        self.pid_queue = pid_queue
        self.last_msg_received_at = time.time()

    def on_start(self, event):
        connect_kwargs = {
            "urls": self.endpoints,
            "sasl_enabled": True,
            "allow_insecure_mechs": True
        }
        if self.user:
            connect_kwargs["user"] = self.user
        if self.password:
            connect_kwargs["password"] = self.password
        self.listener_connection = event.container.connect(**connect_kwargs)
        self.msg_receiver = event.container.create_receiver(self.listener_connection, self.path)

    def on_transport_error(self, event):
        print('received an error "%s"' % event)

    def on_message(self, event):
        self.last_msg_received_at = time.time()

        message = event.message.body
        pid = 0

        json_data = json.loads(message)

        if 'pid' in json_data:
            pid = json_data['pid']

        if 'action' in json_data:
            action = json_data['action']
            if action == 'START':
                self.pid_dictionary[pid] = []
                print(pid)
                #print(message)
                self.pid_dictionary[pid].append(message)

            elif action == 'STOP' and pid in self.pid_dictionary:
                #print(message)
                #print(pid)
                self.pid_dictionary[pid].append(message)
                self.pid_queue.put(self.pid_dictionary[pid])
                del self.pid_dictionary[pid]

        else:
            if pid in self.pid_dictionary:
                print(message)
                self.pid_dictionary[pid].append(message)

