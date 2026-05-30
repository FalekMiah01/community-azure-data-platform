def remove_underscores_from_columns(data: list) -> list:
    """
    Removes underscores from column names in a list.
    
    Parameters:
            data (list): The input list of column names.
            
    Returns:
            list: A new list with modified column names.
    """
    return [col.replace('_', '') if isinstance(col, str) else col for col in data]