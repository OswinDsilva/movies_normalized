from typing import List, Dict, Any

def remove_invalid(data : List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    # Remove records which contain invalid / empty records
    pass

def remove_duplicates(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Remove records that contain duplicate values of Id
    pass

def clean_types(data : List[Dict[str, Any]]) -> List[Dict[str,Any]]:
    # Ensure all the columns have proper typing
    pass

def cleaner(data : List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    no_invalid_data = remove_invalid(data)
    assert len(no_invalid_data) <= len(data), "Removal of Invalids was not executed"

    no_duplicates = remove_duplicates(no_invalid_data)
    assert len(no_duplicates) <= len(no_invalid_data), "Removal of Duplicates was not executed"

    cleaned_data = clean_types(no_duplicates)
    
    return cleaned_data