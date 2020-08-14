import unittest

import barborapp.main


class TestSetupConfig(unittest.TestCase):
    def test_sanity_test(self):
        barborapp.main.hello()
        self.assertTrue(1)
