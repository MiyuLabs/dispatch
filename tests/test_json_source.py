import json
import tempfile
from pathlib import Path

from dispatch.sources.json_source import JSONFileSource


def _write(tmp_path: Path, data: list) -> Path:
    p = tmp_path / "waitlist.json"
    p.write_text(json.dumps(data))
    return p


def test_parses_valid_entries(tmp_path):
    data = [
        {"id": 1, "email": "A@Example.com", "createdAt": "2026-05-10T00:15:12.931Z", "source": "waitlist"},
    ]
    path = _write(tmp_path, data)
    recipients = list(JSONFileSource(path).fetch())
    assert len(recipients) == 1
    r = recipients[0]
    assert r.email == "a@example.com"  # normalized to lowercase
    assert r.external_id == "1"
    assert r.source == "waitlist"
    assert r.created_at is not None


def test_skips_malformed_entries(tmp_path):
    data = [
        {"id": 1, "email": "good@example.com", "createdAt": "2026-05-10T00:00:00Z"},
        {"id": 2, "email": "not-an-email"},
        {"id": 3},  # missing email entirely
    ]
    path = _write(tmp_path, data)
    recipients = list(JSONFileSource(path).fetch())
    assert [r.email for r in recipients] == ["good@example.com"]


def test_dedupes_within_file(tmp_path):
    data = [
        {"id": 1, "email": "dupe@example.com"},
        {"id": 2, "email": "DUPE@example.com"},
    ]
    path = _write(tmp_path, data)
    recipients = list(JSONFileSource(path).fetch())
    assert len(recipients) == 1


def test_extra_fields_become_metadata(tmp_path):
    data = [{"id": 1, "email": "a@example.com", "name": "Amina", "planInterest": "partner"}]
    path = _write(tmp_path, data)
    recipient = next(iter(JSONFileSource(path).fetch()))
    assert recipient.metadata == {"name": "Amina", "planInterest": "partner"}
