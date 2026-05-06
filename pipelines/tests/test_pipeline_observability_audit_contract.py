from inspect import signature

from pipelines.loaders import audit_loader
from pipelines.transforms.common import transform_audit


def test_loader_finish_pipeline_run_accepts_unmatched_count():
    params = signature(audit_loader.finish_pipeline_run).parameters

    assert "unmatched_count" in params


def test_transform_finish_run_accepts_unmatched_count():
    params = signature(transform_audit.finish_transform_run).parameters

    assert "unmatched_count" in params
