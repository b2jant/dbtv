from dbtv.core.redaction import REDACTED, redact


def test_redacts_nested_secrets_and_connection_userinfo() -> None:
    value = {
        "account": "example",
        "password": "canary-password",
        "nested": {"oauth_token": "canary-token"},
        "url": "postgresql://user:pass@example.test/db",
    }
    result = redact(value)
    assert result["password"] == REDACTED
    assert result["nested"]["oauth_token"] == REDACTED
    assert result["url"] == "postgresql://<redacted>@example.test/db"

