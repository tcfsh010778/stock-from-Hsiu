import pytest
from tools.verify_holder_flow import verify

def fixture():
    return {"schema_version":"1.3.0","date":"2026-09-09","data_quality":{"state":"ok"},"institutional_history":{"session_count":20},"supplemental_data":{"retail_200":{"date":"2026-09-04","coverage_count":1900},"holder_metric_count":50}}

def test_weekly_and_daily_dates_are_separate():
    assert verify(fixture(),{"date":"2026-09-04"})["flow_date"]=="2026-09-09"

def test_stale_retail_date_reports_actual_failed_field():
    flow=fixture();flow["supplemental_data"]["retail_200"]["date"]="2026-08-28"
    with pytest.raises(ValueError,match='retail_date.*2026-08-28'):
        verify(flow,{"date":"2026-09-04"})
