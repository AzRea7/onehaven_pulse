from pipelines.common.logging import REDACTED, sanitize_log_payload


def test_pipeline_log_sanitizer_redacts_secret_context():
    payload = sanitize_log_payload(
        {
            "source": "fred",
            "api_key": "secret",
            "metadata": {
                "HUD_USPS_ACCESS_TOKEN": "secret-token",
                "safe": "ok",
            },
        }
    )

    assert payload["api_key"] == REDACTED
    assert payload["metadata"]["HUD_USPS_ACCESS_TOKEN"] == REDACTED
    assert payload["metadata"]["safe"] == "ok"
