# Copyright 2017 Apex.AI, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Script to run a batch of performance experiments."""
from enum import Enum
import itertools
import os
import signal
import subprocess
import sys


class Type(Enum):
    """Define the enumeration for the experiment types."""

    PUBLISHER = 0
    SUBSCRIBER = 1
    BOTH = 2


class Instance:
    """perf_test process encapsulation."""

    def __init__(self, operation_type):
        """
        Constructor.

        :param operation_type: Type of the operation
        """
        # Experiment topics
        topics = [
            'Array16k', 'Array2m', 'Struct32k', 'PointCloud1m', 'PointCloud4m'
        ]

        # Experiment attributes
        rates = ['20', '50', '1000']
        num_subs = ['1', '3', '10']
        reliability = ['', '--reliable']
        durability = ['', '--transient']

        # List of combinations for running jobs
        self.product = list(itertools.product(
            topics, rates, num_subs, reliability, durability)
        )
        self.process = None
        self.type = operation_type

    def run(self, index):
        """
        Run the embedded perf_test process.

        :param index: The test configuration to run.
        :return: The child process returned by subprocess.Popen().
        """
        # The command to execute
        command = self.cmd(index)
        print(command)

        # Spawn a chil procress, capturing the output
        self.process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        # Comment out the following lines to run the experiments
        # with soft realtime priority.
        # We sleeping here to make sure the process is started before
        # changing its priority.
        # time.sleep(2)
        # Enabling (pseudo-)realtime
        # subprocess.Popen(
        #     'chrt -p 99 $(ps -o pid -C 'perf_test' --no-headers)', shell=True
        # )
        return self.process

    def cmd(self, index):
        """
        Return the command line necessary to execute the performance test.

        :param index: The test configuration the returned command line should
            contain.
        :return: The command line argument to execute the performance test.
        """
        command = 'ros2 run performance_test perf_test'
        fixed_args = ('--communication FastRTPS --keep_last ' +
                      '--use_rt_cpus 238 --use_rt_prio 49')

        c = list(self.product[index])  # List of arguments for this job

        # Type dependant arguments
        if self.type == Type.PUBLISHER:
            c[2] = '0'
            pubs_args = '-p1'
        elif self.type == Type.SUBSCRIBER:
            pubs_args = '-p0'
        elif self.type == Type.BOTH:
            pubs_args = '-p1'
        else:
            raise ValueError('Unsupported type')

        # Create directory to store the job results
        dir_name = 'rate_{}/subs_{}'.format(c[1], c[2])
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)

        # Job dependant arguments
        dyn_args = "-l '{}/log' --topic {} --rate {} -s {} {} {}".format(
            dir_name, c[0], c[1], c[2], c[3], c[4]
        )

        return '{} {} {} {}'.format(command, fixed_args, dyn_args, pubs_args)

    def kill(self):
        """Kill the associated performance test process."""
        if self.process is not None:
            self.process.kill()

    def num_runs(self):
        """Return the number of experiments runs this instance can execute."""
        return len(self.product)

    def __del__(self):
        """Kill the associated performance test process."""
        self.kill()


def signal_handler(sig, frame):
    """Signal handler to handle Ctrl-C."""
    print('You pressed Ctrl+C! Terminating experiment')
    # Kill running processes and exit
    subprocess.Popen('killall -9 perf_test', shell=True)
    sys.exit(-1)


def timer_handler(sig=None, frame=None):
    """Signal handler to handle the timer."""
    # Get global variables
    global current_index
    global full_list
    global can_kill
    global remaining_jobs

    # If is not the first time, kill all the running perf_test
    if can_kill:
        # Kill the children processes of the instances
        [e.kill() for e in full_list]
        # Make sure everything is killed
        subprocess.Popen('killall -9 perf_test', shell=True)
    else:
        can_kill = True

    # Keep track of whethet there are jobs to perform
    if current_index >= total_jobs:
        print('--------------------------------------------------------------')
        print('Done with experiments.')
        print('**************************************************************')
        sys.exit(0)
    else:
        # Run the jobs in all instances
        for e in full_list:
            print('----------------------------------------------------------')
            print('Running job {}. Remaining time: {} [s]'.format(
                current_index + 1, experiment_length * remaining_jobs)
            )
            # Run the job with index current_index for that instance
            ret = e.run(current_index)
            # Update the remaining jobs counter
            remaining_jobs -= 1
            # If the job returns and there was an error, stop the experiment
            if ret.returncode is not None and ret.returncode != 0:
                print('Process ended with NON-ZERO return code: {}'.format(
                    ret.returncode)
                )
                print('------------------------------------------------------')
                print('Stopping execution without finishing the experiments')
                print('******************************************************')
                sys.exit(ret.returncode)
        current_index = current_index + 1


if __name__ == '__main__':
    experiment_length = 120  # In seconds

    # Flag to evaluate whether is the first time and therefore no process
    # is running or we should kill the running processes
    can_kill = False

    current_index = 0
    # Number of processes for only publish, only subscribe, or both
    num_pub_processes = 0
    num_sub_processes = 0
    num_both = 1
    # Create a list of instances to perform tests
    pub_list = [Instance(Type.PUBLISHER) for _ in range(0, num_pub_processes)]
    sub_list = [Instance(Type.SUBSCRIBER) for _ in range(0, num_sub_processes)]
    both_list = [Instance(Type.BOTH) for _ in range(0, num_both)]
    full_list = pub_list + sub_list + both_list

    print('******************************************************************')
    print('*                         APEX EXPERIMENT                        *')
    print('******************************************************************')
    # Print experiment characteristics
    print('There are {} instances running jobs:'.format(len(full_list)))
    i = 1
    total_jobs = 0
    for instance in full_list:
        print(
            '  {} - Instance {} runs {} jobs'.format(i, i, instance.num_runs())
        )
        i += 1
        total_jobs += instance.num_runs()
    remaining_jobs = total_jobs
    print('TOTAL NUMBER OF JOBS: {}'.format(total_jobs))
    print('Jobs duration:  {} [s]'.format(experiment_length))
    print('TOTAL RUN TIME: {} [s]'.format(experiment_length * total_jobs))
    print('Press Ctrl+C to abort the experiment at any time')

    # Set signals handlers
    signal.signal(signal.SIGALRM, timer_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.setitimer(signal.ITIMER_REAL, experiment_length, experiment_length)

    # Execute timer_handler for avoiding waiting the first time
    timer_handler()
    while True:
        signal.pause()  # Wait until signal fires
