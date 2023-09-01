import pandas as pd
from pandas.api.types import is_numeric_dtype
import numpy as np
import ast

#PATH = "~/Documents/git/gitlab/conductome-data-processing/"
PATH = "/Users/noeag/Documents/Git/gitc3/conductome-data-processing/"
INSTRUCTIONS_FNAME = "fmed-to-2014/data/instrucciones.csv"
DF1_DATA_FNAME = "fmed-to-2014/data/2014_datos.csv"
DF1_DICT_FNAME = "fmed-to-2014/data/2014_diccionario.csv"
DF2_DATA_FNAME = "fmed-to-2014/data/fmed_datos.csv"
DF2_DICT_FNAME = "fmed-to-2014/data/fmed_diccionario.csv"


def read_data():
    """
    Reads the data from the files and returns four dataframes.

    Args:
    PATH (str): Path to the directory containing the data files.

    Returns:
    df1_dict (pd.DataFrame): Data-Dictionary dataframe 1.
    df1_data (pd.DataFrame): Data dataframe 1.
    df_instructions (pd.DataFrame): Instructions dataframe.
    df2_data (pd.DataFrame): Data dataframe 2.
    """
    df1_dict = pd.read_csv(PATH + DF1_DICT_FNAME)
    df1_data = pd.read_csv(PATH + DF1_DATA_FNAME)
    df_instructions = pd.read_csv(PATH + INSTRUCTIONS_FNAME)
    df2_data = pd.read_csv(PATH + DF2_DATA_FNAME)
    return df1_dict, df1_data, df_instructions, df2_data


def create_dict():
    """
    Generate an empty dataframe with a Data-Dictionary structure.

    Returns:
    df_dict_new (pd.DataFrame): Data-Dictionary dataframe.
    """
    df_dict_new = pd.DataFrame(columns=['category', 'campo_unificado', 'description', 'options'])
    return df_dict_new


def read_instructions(df_instructions, n):
    """
    Reads the n-th row of the instructions dataframe and returns the required fields.

    Args:
    df_instructions (pd.DataFrame): Instructions dataframe.
    n (int): Row index to read from.

    Returns:
    new_column_name (str): New column name.
    description_df1 (str): Description of the variable in 2014.
    description_df2 (str): Description of the variable in fmed.
    description_final (str): Final description of the variable.
    value_options (str): String representation of the dictionary containing options.
    options_updated (str): String representation of the updated option's dictionary.
    actions (str): String representation of the dictionary containing actions to perform.
    df1_column_name (str): Column name in 2014 dataframe.
    df2_column_name (str): Column name in fmed dataframe.
    """
    row = df_instructions.iloc[n, :]

    new_column_name = row['campo_unificado']
    description_df1 = row['description_2014']
    description_df2 = row['description_fmed']
    description_final = description_df2  # default final description is the fmed one
    value_options = row['options']
    options_updated = value_options
    actions = row['acciones']
    df1_column_name = row['code_2014']
    df2_column_name = row['code_fmed_completo']
    category = row['subcategoria']
    return new_column_name, description_df1, description_df2, description_final,\
        value_options, options_updated, actions, df1_column_name, df2_column_name, category


def convert_to_dict(value_options, actions):
    """
    Reads the string representation of the dictionaries containing options and actions and returns its dictionary form.

    Args:
    value_options (str): String representation of the dictionary containing options.
    actions (str): String representation of the dictionary containing actions to perform.

    Returns:
    value_options (dict): Dictionary containing options.
    actions (dict): Dictionary containing actions to perform.
    """
    if value_options != value_options:
        value_options = "{}"  # assign an empty dictionary if options are missing
    value_options = ast.literal_eval(value_options)

    if actions != actions:
        actions = "{'actions':['none']}"  # assign an empty dictionary if actions are missing
    actions = ast.literal_eval(actions)

    return value_options, actions

def add_to_dict(actions, value_options):
    """
    Adds new options to an existing dictionary.
    If you are adding to a dict is because there is an existing 'options' key.
    If you have an 'options' key then your variable is categorical.
    
    Args:
    - value_options (dict):  Existing options to be updated.
    - actions (dict): Dictionary containing actions to perform. Key='add_to_dict' has the new options to be added.
    
    Returns:
    - options_updated (dict): Updated dictionary with the new options.
    """
    old_options = value_options['options']
    new_options = actions['add_to_dict']
    merged_options = old_options | new_options
    options_updated = value_options.copy()
    options_updated['options'].update(merged_options)
    return options_updated


def recode(df2_data, df2_data_new, new_column_name, df2_column_name, actions):
    """
    Recodes a column of a dataframe using a dictionary mapping and stores the result in a new column.
    
    Args:
    - df2_data (pd.DataFrame): Original dataframe.
    - df2_data_new (pd.DataFrame): New dataframe to store the recoded column.
    - new_column_name (str): Name of the new column.
    - df2_column_name (str): Name of the original column.
    - actions (dict): Mapping dictionary to be used for recoding.
    
    Returns:
    - df2_data_new (pd.DataFrame): Updated dataframe with new column and values recoded.
    """
    how_recode = actions['recode']
    # Aplicar el mapeo usando el método map
    df2_data_new[new_column_name] = df2_data[df2_column_name].map(how_recode)

    validacion_recode = get_distribution(df2_data, df2_data_new, df2_column_name, new_column_name, how_recode)
    
    if validacion_recode == True:
        print('Recodificación EXITOSA')
    else:
        print('PROBLEMAS en la recodificación')

    return df2_data_new


def recode_extend(df2_data, df2_data_new, new_column_name, df2_column_name, actions, description_df1):
    """
    Recodes a column of a dataframe using a dictionary mapping and
     stores the result in a new column with an extended description.
    
    Args:
        - df2_data (pd.DataFrame): Original dataframe.
        - df2_data_new (pd.DataFrame): New dataframe to store the recoded column.
        - new_column_name (str): Name of the new column.
        - df2_column_name (str): Name of the original column.
        - actions (dict): Mapping dictionary to be used for recoding.
        - description_df1 (str): Description to be appended to the final description.
    
    Returns:
        - df2_data_new (pd.DataFrame): Updated dataframe with new column and values recoded.
        - description_final (str): Description for the final Data-Dictionary
    """
    how_recode = actions['recode']
    # Aplicar el mapeo usando el método map
    df2_data_new[new_column_name] = df2_data[df2_column_name].map(how_recode)
    description_final = description_df1

    validacion_recode = get_distribution(df2_data, df2_data_new, df2_column_name, new_column_name, how_recode)
    if validacion_recode == True:
        print('Recodificación EXITOSA')
    else:
        print('PROBLEMAS en la recodificación')

    return df2_data_new, description_final

def convert_to_numeric(column):
    """
    Converts a column in a pandas DataFrame to numeric values if possible.
    If a value in the column is not numeric, it is kept as a string.

    Parameters:
        column (pd.Series): A pandas Series representing a column in a DataFrame.

    Returns:
        updated_column: (pd.Series): A pandas Series with numeric values where possible and strings otherwise.
    """
    if is_numeric_dtype(column.dropna()):
        return column
    else:
        # Convert the column to a pandas Series to use the str method
        column_series = pd.Series(column)
        # Use the str method to check if each value is numeric
        is_numeric = column_series.str.isnumeric()
        # Use the pandas to_numeric method to convert numeric values to floats or ints
        numeric_values = pd.to_numeric(column_series[is_numeric], errors='coerce')
        # Combine the numeric and non-numeric values back into a single Series
        result_series = pd.concat([numeric_values, column_series[~is_numeric]])
        # Return the result as a Series with the original index
        updated_column = pd.Series(result_series, index=column.index)
        return updated_column

def copy(df2_data, df2_data_new, new_column_name, df2_column_name, options_updated):
    """
    Copies a column of a dataframe to a new column and stores the range of values in a dictionary.
    
    Args:
    - df2_data (pd.DataFrame): Original dataframe.
    - df2_data_new (pd.DataFrame): New dataframe to store the copied column.
    - new_column_name (str): Name of the new column.
    - df2_column_name (str): Name of the original column.
    - options_updated (dict): Default options of the variable.
    
    Returns:
    - df2_data_new (pd.DataFrame): Updated dataframe with new column and values recoded.
    - options_updated (dict): Dictionary with the minimum and maximum values of the copied column.
    """
    column = df2_data[df2_column_name]  # Columna con puros strings
    nan = column.isna().sum()
    column = convert_to_numeric(column.dropna())  # Columna con strings y numericos ya localizados omite NaN
    column = column.replace('na', np.nan)  #OMITIR esta linea CUANDO 'na' tenga significado diferente a null/NaN
    # Count how many values were converted to floats
    float_count = pd.to_numeric(column, errors='coerce').notna().sum()
    # Count how many values were kept as strings
    def count_non_null_strings(x):
        if isinstance(x, str):
            return 1
        else:
            return 0
    # Count occurrences of non-null strings in column A
    string_count = column.apply(count_non_null_strings).sum()
    # Count how many NaN values there are
    nan += column.isna().sum()
    print(f'Numericos: {float_count}')
    #print(f'Numericos: {float_count},\nStrings: {string_count},\nNulls: {nan}')
    if float_count >= string_count:
        print('La columna es numerica y se podra encontrar un maximo y minimo')
        df2_data[df2_column_name] = pd.to_numeric(df2_data[df2_column_name], errors="coerce")
        df2_data_new[new_column_name] = df2_data[df2_column_name]
        maximo = df2_data_new[new_column_name].max()
        minimo = df2_data_new[new_column_name].min()
        options_updated = {'min': minimo, 'max': maximo}
    else:
        print(f'La columna es de strings y se quedan las opciones: {options_updated}')
        df2_data_new[new_column_name] = df2_data[df2_column_name]
        options_updated = options_updated
    return df2_data_new, options_updated


def new_options(actions):
    """
    Returns the new options dictionary from an actions dictionary.
    
    Args:
    - actions (dict): Actions dictionary with a 'new_options' key.
    
    Returns:
    - options_updated (dict): Dictionary with the new options.
    """
    options = actions['new_options']
    options_updated = {'options': None, 'is_category': 'true'}
    options_updated['options'] = options
    return options_updated

def add_to_new_dict(df2_dict_new, code, description, options, category):
    """
    Add a new register to a dictionary-like DataFrame.

    Args:
        df2_dict_new (pandas.DataFrame): The DataFrame where the new register will be added.
        code (str): The unified code of the new register.
        description (str): The description of the new register.
        options (str): The dictionary of options related to the new register.
        category (str): The category of the new register.

    Returns:
        df2_dict_new (pd.DataFrame): The updated DataFrame with the new register added.

    """
    new_register = {'campo_unificado': code, 'description': description, 'options': options, 'category': category}
    df2_dict_new.loc[len(df2_dict_new.index)] = new_register
    return df2_dict_new

def apply_actions(df2_data, df2_data_new, new_column_name, description_df1, description_df2, description_final,
                    value_options, options_updated, actions, df1_column_name, df2_column_name, category):
    """
    This function performs a set of actions on a given pandas DataFrame column.
    The actions are specified in a dictionary passed as the 'actions' argument.
    The resulting new column is added to a new DataFrame, 'df2_data_new'.

    Args:
    - df2_data (pandas.DataFrame): The DataFrame that contains the column to modify.
    - df2_data_new (pandas.DataFrame): The new DataFrame where the modified column will be added.
    - new_column_name (str): The name of the new column to be created.
    - description_df1 (str): The description of the first DataFrame.
    - description_df2 (str): The description of the second DataFrame.
    - description_final (str): The final description of the modified column.
    - value_options (dict): The initial options for the column.
    - options_updated (dict): The options that were updated during the actions.
    - actions (dict): A dictionary containing the set of actions to perform on the column.
    - df1_column_name (str): The name of the column in the first DataFrame.
    - df2_column_name (str): The name of the column in the second DataFrame.
    - category (str): The category of the variable.
    
    Returns:
    - code (str): The name of the modified column.
    - description (str): The description of the modified column.
    - options (str): The final options of the modified column.
    - category (str): The category of the variable.
    """
    for item in actions['actions']:
        if item == 'add_to_dict':
            # Call add_to_dict function to modify options_updated.
            options_updated = add_to_dict(actions, value_options)

        elif item == 'recode':
            # Call recode function to modify df2_data_new and options_updated.
            df2_data_new = recode(df2_data, df2_data_new, new_column_name, df2_column_name, actions)

        elif item == 'recode_extend':
            # Call recode_extend function to modify df2_data_new and options_updated.
            df2_data_new, description_final = recode_extend(df2_data, df2_data_new, new_column_name, df2_column_name,
                                                            actions, description_df1)

        elif item == 'copy':
            # Call copy function to modify df2_data and df2_data_new, and update options_updated.
            df2_data_new, options_updated = copy(df2_data, df2_data_new, new_column_name, df2_column_name, options_updated)

        elif item == 'new_options':
            # Call new_options function to modify options_updated.
            options_updated = new_options(actions)

        elif item == 'especial':
            print(f"La variable {df2_column_name} requiere un trato especial")

        else:
            print(f"La accion solicitada {item} no se encuentra")

    code = new_column_name
    description = description_final
    options = str(options_updated)
    categoria = category

    return code, description, options, categoria


def get_distribution2(df2_data, df2_data_new, df2_column_name, new_column_name):
    #Descomentar las siguientes tres lineas si queremos observar la distribucion de los valores.
    conteos_df_new = df2_data_new[new_column_name].value_counts()
    conteos_df_old = df2_data[df2_column_name].value_counts()
    #print(conteos_df_old, conteos_df_new)
    state = all(conteos_df_old.reset_index(drop=True) == conteos_df_new.reset_index(drop=True))
    return state

def get_distribution(df2_data, df2_data_new, df2_column_name, new_column_name, how_recode):
    count_keys, count_values = count_relation(df2_data, df2_data_new, df2_column_name, new_column_name, how_recode)
    print(f'Relacion de recodificación: {how_recode}')
    print(f'Conteo de llaves: {count_keys}')
    print(f'Conteo de valores: {count_values}')
    count_values_actualizado = check_dict_relation(how_recode, count_keys, count_values)
    for valor in count_values_actualizado.values():
        if valor != 0:
            return False
    return True

def count_relation(df2_data, df2_data_new, df2_column_name, new_column_name, how_recode):
    """
    Esta función cuenta el número de apariciones de las llaves del diccionario how_recode en col1 y el número de
    apariciones de los valores de cada llave en how_recode en col2.

    Args:
        df2_data (pandas.DataFrame): The DataFrame that contains the column to modify.
        df2_data_new (pandas.DataFrame): The new DataFrame where the modified column will be added.
        df2_column_name (str): The name of the column in the second DataFrame.
        new_column_name (str): The name of the new column to be created.
        how_recode (dict): Diccionario que relaciona las llaves de df2_column_name con los valores de new_column_name.

    Returns:
        tuple: Retorna una tupla con dos diccionarios. El primer diccionario contiene la cuenta de las llaves en df2_column_name
        y el segundo diccionario contiene la cuenta de los valores de cada llave en new_column_name según how_recode.
    """
    col1 = df2_data[df2_column_name].tolist()
    col2 = df2_data_new[new_column_name].tolist()
    count_keys = {}
    count_values = {}

    # Obtener las llaves únicas presentes en col1
    unique_keys = set(col1)

    for key in unique_keys:
        # Contar el número de apariciones de la llave en col1
        count_keys[key] = col1.count(key)
        if key in how_recode:
            # Obtener el valor asociado con la llave
            value = how_recode[key]
            # Contar el número de apariciones del valor en col2
            count_values[value] = col2.count(value)
        #else:
        #    print(f'Error con la llave {key} en {df2_column_name}')

    return count_keys, count_values

def check_dict_relation(how_recode, count_keys, count_values):
    """
    Esta función comprueba si la relación establecida en how_recode se cumple en los diccionarios count_keys y
    count_values.

    Args:
        how_recode (dict): Diccionario que relaciona las llaves de count_keys con los valores de count_values.
        count_keys (dict): Diccionario con la cuenta de las llaves de how_recode en una columna del dataframe.
        count_values (dict): Diccionario con la cuenta de los valores de how_recode en otra columna del dataframe.

    Returns:
        dict: Retorna el diccionario count_values actualizado después de haber comprobado la relación how_recode.
    """
    for key, value in how_recode.items():
        if key in count_keys:
            count_key = count_keys[key]
            count_values[value] -= count_key
    return count_values