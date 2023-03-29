import unittest

import data_processor


class TestUtils(unittest.TestCase):
    def test_counts(self):
        self.assertFalse(utils.is_prime(4))
        self.assertTrue(utils.is_prime(2))
        self.assertTrue(utils.is_prime(3))
        self.assertFalse(utils.is_prime(8))
        self.assertFalse(utils.is_prime(10))
        self.assertTrue(utils.is_prime(7))
        self.assertEqual(utils.is_prime(-3),
                         "Negative numbers are not allowed")


if __name__ == '__main__':
    unittest.main()