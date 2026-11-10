import pytest

pytest.importorskip("datasette")

from cylc_status import column, column_rules  # noqa: E402


def test_no_rules_no_folding() -> None:
    assert column("sw4_Ina1968", column_rules({})) == "sw4_Ina1968"
    assert column("sw4_Ina1968", column_rules(None)) == "sw4_Ina1968"
    assert column("sw4_Ina1968", column_rules({"columns": []})) == "sw4_Ina1968"


def test_the_first_matching_rule_names_the_column() -> None:
    rules = column_rules({"columns": [{"match": "sw4_.*", "column": "sw4"}, {"match": "sw4_x", "column": "never"}]})
    assert column("sw4_Ina1968", rules) == "sw4"
    assert column("sw4_x", rules) == "sw4"
    assert column("im_calc", rules) == "im_calc"


def test_the_whole_name_must_match() -> None:
    rules = column_rules({"columns": [{"match": "sw4", "column": "solver"}]})
    assert column("sw4_Ina1968", rules) == "sw4_Ina1968"
    assert column("sw4", rules) == "solver"


def test_columns_may_use_the_match_groups() -> None:
    rules = column_rules({"columns": [{"match": r"(?P<stage>\w+?)_member\d+", "column": r"\g<stage>"}]})
    assert column("forecast_member07", rules) == "forecast"
    assert column("post_member12", rules) == "post"
