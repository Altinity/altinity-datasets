#!/usr/bin/python3

"""Tests behavior of page location indexes"""
import logging
import os
import shutil
import unittest

import altinity_datasets

from altinity_datasets import api

# Define logger
logging.basicConfig(filename="test.log", level=logging.DEBUG)
logger = logging.getLogger(__name__)


class ApiTest(unittest.TestCase):
    def setUp(self):
        self.host = "localhost"
        self.user = "special"
        self.password = "secret"
        self.db = "altinity_ds_test"
        self.table = "^iris$"

    def test_01_dataset_load(self):
        """Load a dataset"""
        api.dataset_load("iris", 
            host=self.host,
            database=self.db,
            parallel=2,
            clean=True)

    def test_02_dataset_dump(self):
        """Dump and reload a dataset"""
        self._removedirs("iris_test")
        api.dataset_dump("iris_test", 
            host=self.host,
            database=self.db,
            parallel=2,
            overwrite=True)
        api.dataset_load("iris_test", 
            repo_path=".", 
            host=self.host,
            database=self.db,
            parallel=2,
            clean=True)
        self._removedirs("iris_test")

    def test_03_dataset_dump_compressed(self):
        """Dump and reload a dataset using compressed data files"""
        self._removedirs("output/iris_test_compressed")
        api.dataset_dump("iris_test_compressed", 
            host=self.host,
            database=self.db,
            repo_path="output",
            parallel=2,
            overwrite=True)
        api.dataset_load("iris_test_compressed", 
            repo_path="output", 
            host=self.host,
            database=self.db,
            parallel=2,
            clean=True)
        self._removedirs("output/iris_test_compressed")

    def test_04_load_and_dump_built_ins(self):
        """Load and dump all built-ins"""
        built_ins = api.dataset_search(None)
        for built_in in built_ins:
            name = built_in['name']
            db = "built_in_" + self.db
         
            api.dataset_load(name,
                host=self.host,
                database=db,
                parallel=2,
                clean=True)

            dump_dir = os.path.join("output", name)
            self._removedirs(dump_dir)
            api.dataset_dump(name,
                repo_path="output",
                host=self.host,
                database=db,
                parallel=2)

    def test_05_dataset_with_credentials(self):
        """Dump and reload a dataset using a user name and password"""
        self._removedirs("output/iris_test_user")
        api.dataset_dump("iris_test_user", 
            repo_path="output", 
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.password,
            parallel=2,
            overwrite=True)
        api.dataset_load("iris_test_user", 
            repo_path="output", 
            host=self.host,
            database=self.db,
            user=self.user,
            password=self.password,
            parallel=2,
            clean=True)
        self._removedirs("output/iris_test_user")

    def _removedirs(self, dir):
        if os.path.exists(dir):
            shutil.rmtree(dir) 

if __name__ == '__main__':
    unittest.main()
