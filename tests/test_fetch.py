# import os
# import unittest

# import pandas as pd

# from runpandarun import Datastore


# class Test(unittest.TestCase):
#     # base fetching logic see `tests.test_datastore`

#     def setUp(self):
#         self.store = Datastore.from_yaml('./example/corona-hopkins/config.yml')
#         self.dataset = self.store.datasets[0]

#     def test_loop(self):
#         ds = self.dataset
#         ds.update()
