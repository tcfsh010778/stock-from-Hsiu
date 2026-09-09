import json
import pytest
from tools import resolve_verified_session as module


def test_rejects_old_manifest_instead_of_publishing_stale_flow(tmp_path, monkeypatch):
    monkeypatch.setattr(module, "expected_session", lambda **_: "2026-09-09")
    payload = {"dataset_id": "official_adjusted_daily_update", "calendar_basis": "official_twse_tpex", "status": "complete", "data_as_of": "2026-09-09"}
    path = tmp_path / "official_adjusted_update_manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    (tmp_path / "price_refresh_summary.json").write_text(json.dumps({"status": "fresh", "latest_data_date": "2026-09-09"}), encoding="utf-8")
    assert module.resolve(tmp_path) == "2026-09-09"
    payload["data_as_of"] = "2026-09-08"
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ValueError, match="not current"):
        module.resolve(tmp_path)
