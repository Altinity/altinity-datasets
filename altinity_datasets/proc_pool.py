# Copyright (c) 2019 Altinity LTD
#
# This product is licensed to you under the
# Apache License, Version 2.0 (the "License").
# You may not use this product except in compliance with the License.
#
# This product may include a number of subcomponents with
# separate copyright notices and license terms. Your use of the source
# code for the these subcomponents is subject to the terms and
# conditions of the subcomponent's license, as noted in the LICENSE file.

import logging
import subprocess
import time

# Define logger
logger = logging.getLogger(__name__)

class ProcessPool:
    """Service for executing processes in parallel"""

    def __init__(self, size=5, dry_run=None, progress_reporter=None):
        """Instantiate a new pool
        :param size: (int): Number of concurrent processes to run
        :param dry_run: (boolean): If true just show what we would run
        :param progress_reporter: (function): If specified call function with string message showing progress
        """
        self.size = size
        if dry_run is None:
            self.dry_run = False
        else:
            self.dry_run = dry_run
        self.progress_reporter=progress_reporter
        self.slots = []
        self.outputs = []
        self.failed = 0
        self.succeeded = 0

    def exec(self, command):
        """Submit a command for execution, blocking if pool is full
        :param command: (str): Shell command to execute
        """
        if len(self.slots) >= self.size:
            self._wait()
        if self.dry_run:
            logger.info("Dry run: " + command)
        else:
            logger.info("Starting a new process: " + command)
            process = subprocess.Popen(command, shell=True)
            self.slots.append(process)

    def drain(self):
        """Wait for all pending commands to finish"""
        while len(self.slots) > 0:
            self._wait()

    def _wait(self):
        logger.info("Waiting for command to finish")
        cur_len = len(self.slots)
        while cur_len > 0 and cur_len == len(self.slots):
            for p in self.slots:
                status = p.poll()
                if status is None:
                    time.sleep(1)
                elif status == 0:
                    logger.info("Process completed: {}".format(p.args))
                    self.outputs.append(status)
                    self.succeeded += 1
                    self.slots.remove(p)
                    break
                else:
                    self._progress_and_info("Process failed: {}".format(p.args))
                    self.outputs.append(status)
                    self.failed += 1
                    self.slots.remove(p)
                    break

    def _progress_and_info(self, message):
        if self.progress_reporter is not None:
            self.progress_reporter(message)
        logger.info(message)
