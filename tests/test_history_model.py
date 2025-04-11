import __init__
import unittest
import os

from models.history import HistoryModel
from models.models_set import ModelsSet


class TestHistoryModel(unittest.TestCase):
    def test_history_model(self):
        ms = ModelsSet("test_history")
        history = ms.history
        self.assertTrue(history.check_conn())

        history.truncate()
        self.assertEqual(history.count(), 0)

        # Test insert
        itemids = [1, 2, 3]
        clocks = [1, 2, 3]
        values = [0.1, 0.2, 0.3]

        # Insert data
        history.insert(itemids, clocks, values)

        self.assertEqual(history.count(), len(itemids))

        # Test get_data        
        data = history.get_data()
        self.assertEqual(len(data), len(itemids))

        self.assertEqual(history.count(), len(itemids))


if __name__ == "__main__":
    unittest.main()