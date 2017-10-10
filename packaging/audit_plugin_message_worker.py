import os
import json
from multiprocessing import Process


class MessageWorker(Process):

    def __init__(self, pid_queue, result_queue):
        Process.__init__(self)
        self.pid_queue = pid_queue
        self.result_queue = result_queue

    def run(self):
        while True:
            next_task = self.pid_queue.get()
            if next_task is None:
                self.pid_queue.task_done()
                break

            start_str = json.loads(next_task[0])
            log_file = start_str['log_file']

            answer = 'passed'
            if os.path.exists(log_file):
                with open(log_file, 'r') as pid_file:
                    # variable count counts the number of lines in the pid file
                    count = 0
                    for line in pid_file:
                        new_line = line

                        if "__BEGIN_JSON__" in new_line:
                            new_line = new_line.split("__BEGIN_JSON__")[1]
                            if "__END_JSON__" in new_line:
                                new_line = new_line.split("__END_JSON__")[0]

                        json_str = json.loads(new_line)
                        next_str = json.loads(next_task[count])

                        if not next_str == json_str:
                            answer = 'failed'
                            break

                        count = count+1

                    self.result_queue.put(answer)

            self.pid_queue.task_done()

        return
