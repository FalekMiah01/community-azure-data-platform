from dabs_python_wheel import remove_underscores_from_columns

def test_remove_underscores_from_columns():
    assert remove_underscores_from_columns(["first_name", "last_name", "email_address"]) == ["firstname", "lastname", "emailaddress"]
    assert remove_underscores_from_columns(["_leading", "trailing_", "_both_"]) == ["leading", "trailing", "both"]
    assert remove_underscores_from_columns(["nochange"]) == ["nochange"]
    assert remove_underscores_from_columns(["under_score_here"]) == ["underscorehere"]
    assert remove_underscores_from_columns([]) == []