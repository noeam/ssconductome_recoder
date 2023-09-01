import unittest
import pandas as pd
import data_processor


class TestData_Processor(unittest.TestCase):
    def test_get_distribution(self):
        data = {'original': ['a', 'b', 'c', 'a', 'b', 'b'], 'recoded': [1, 2, 3, 1, 2, 2]}
        df = pd.DataFrame(data)
        how_recode = {'a': 1, 'b': 2, 'c': 3}
        result = data_processor.get_distribution(df, df, 'original', 'recoded', how_recode)
        self.assertTrue(result)

        data = {'original': ['a', 'b', 'c', 'a', 'b', 'b'], 'recoded': [1, 2, 3, 1, 2, 3]}
        df = pd.DataFrame(data)
        how_recode = {'a': 1, 'b': 2, 'c': 3}
        result = data_processor.get_distribution(df, df, 'original', 'recoded', how_recode)
        self.assertFalse(result)

        # Prueba 1: Cuando la relación de recodificación se cumple
        df2_data = pd.DataFrame({'col1': ['a', 'a', 'b', 'b', 'c', 'c']})
        df2_data_new = pd.DataFrame({'new_col': [1, 1, 2, 2, 3, 3]})
        how_recode = {'a': 1, 'b': 2, 'c': 3}
        result = data_processor.get_distribution(df2_data, df2_data_new, 'col1', 'new_col', how_recode)
        self.assertTrue(result)

        # Prueba 2: Cuando la relación de recodificación no se cumple
        df2_data = pd.DataFrame({'col1': ['a', 'a', 'b', 'b', 'c', 'c']})
        df2_data_new = pd.DataFrame({'new_col': [1, 2, 2, 2, 3, 3]})
        how_recode = {'a': 1, 'b': 2, 'c': 3}
        result = data_processor.get_distribution(df2_data, df2_data_new, 'col1', 'new_col', how_recode)
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()