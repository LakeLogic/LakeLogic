from __future__ import annotations

import lakelogic


def test_package_exports_and_help_topic(capsys):
    assert lakelogic.__version__
    assert lakelogic.DataProcessor is not None
    assert lakelogic.validate_contract is not None

    topic = lakelogic.HelpTopic("demo", "demo text")
    topic.help()
    topic()
    captured = capsys.readouterr().out
    assert captured.count("demo text") == 2


def test_help_index_dispatch_and_attr_error(capsys):
    demo = lakelogic.HelpIndex({"demo": lakelogic.HelpTopic("demo", "demo topic")})

    demo()
    demo("demo")
    demo(full=True)
    captured = capsys.readouterr().out
    assert "LakeLogic Help" in captured
    assert "demo topic" in captured

    try:
        demo.missing
    except AttributeError as exc:
        assert str(exc) == "missing"
    else:
        raise AssertionError("Expected AttributeError for unknown topic")


def test_global_help_topics_print_expected_sections(capsys):
    lakelogic.help("driver")
    lakelogic.help("bootstrap")
    lakelogic.help("import_dbt")
    lakelogic.help("unknown")
    captured = capsys.readouterr().out
    assert "LakeLogic Driver Help" in captured
    assert "LakeLogic Bootstrap Help" in captured
    assert "LakeLogic import-dbt Help" in captured
    assert "LakeLogic Help" in captured