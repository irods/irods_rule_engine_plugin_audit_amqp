from __future__ import print_function

import time

from audit_plugin_message_worker import MessageWorker
from message_broker_consumer import MessageBrokerConsumer

from proton.reactor import Container


class QueueListener(object):
    def __init__(self, pid_queue, result_queue, url, queue_name):
        self.pid_queue = pid_queue
        self.result_queue = result_queue
        self.url = url
        self.queue_name = queue_name

    def run(self):

        timeout = 5
        num_workers = 5
        for _ in xrange(num_workers):
            worker = MessageWorker(self.pid_queue, self.result_queue)
            worker.start()

        consumer = MessageBrokerConsumer(self.url, self.queue_name, self.pid_queue)
        container = Container(consumer)

        class TimerClass(object):
            def __init__(self, container, consumer):
                self.container = container
                self.consumer = consumer

            def on_timer_task(self, event):
                if time.time() > self.consumer.last_msg_received_at + timeout:
                    self.consumer.msg_receiver.close()
                    self.consumer.listener_connection.close()
                else:
                    self.container.schedule(1, self)

        container.schedule(timeout, TimerClass(container, consumer))
        container.run()

        #Add poison pill to end the processes
        for _ in xrange(num_workers):
            self.pid_queue.put(None)

        self.pid_queue.join()