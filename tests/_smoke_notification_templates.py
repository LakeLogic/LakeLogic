from pathlib import Path

import pytest

from lakelogic.notifications.base import render_notification_content


def test_render_notification_content_passthrough_without_templates():
    message, subject = render_notification_content(
        config={},
        message="hello",
        subject="default",
        context={"event": "failure"},
    )
    assert message == "hello"
    assert subject == "default"


def test_render_notification_content_inline_templates():
    config = {
        "subject_template": "[{{ event }}] {{ contract.title }}",
        "message_template": "run={{ run_id }} msg={{ message }}",
    }
    message, subject = render_notification_content(
        config=config,
        message="quality failed",
        subject="fallback",
        context={
            "event": "failure",
            "run_id": "abc-123",
            "contract": {"title": "silver_customers"},
        },
    )
    assert subject == "[failure] silver_customers"
    assert message == "run=abc-123 msg=quality failed"


def test_render_notification_content_file_templates(tmp_path: Path):
    subject_file = tmp_path / "subject.j2"
    message_file = tmp_path / "message.j2"
    subject_file.write_text("{{ contract.title }} :: {{ event }}", encoding="utf-8")
    message_file.write_text("{{ metadata.domain }} -> {{ message }}", encoding="utf-8")

    config = {
        "_base_path": str(tmp_path),
        "subject_template_file": "subject.j2",
        "message_template_file": "message.j2",
    }
    message, subject = render_notification_content(
        config=config,
        message="quarantine detected",
        subject="fallback",
        context={
            "event": "quarantine",
            "contract": {"title": "bronze_orders"},
            "metadata": {"domain": "commerce"},
        },
    )
    assert subject == "bronze_orders :: quarantine"
    assert message == "commerce -> quarantine detected"


def test_render_notification_content_template_context_merges():
    config = {
        "subject_template": "{{ subject }}",
        "body_template": "{{ team }} :: {{ message }}",
        "template_context": {"team": "data-platform"},
    }
    message, subject = render_notification_content(
        config=config,
        message="schema drift",
        subject="alert",
    )
    assert subject == "alert"
    assert message == "data-platform :: schema drift"


def test_render_notification_content_requires_existing_file(tmp_path: Path):
    with pytest.raises(ValueError, match="subject_template_file not found"):
        render_notification_content(
            config={"_base_path": str(tmp_path), "subject_template_file": "missing.j2"},
            message="msg",
            subject="subject",
        )
