from dbtv.core.redaction import REDACTED, redact, redact_text


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


def test_inline_secret_redaction() -> None:
    text = redact_text("password=canary token:other private_key='key-material'")
    assert "canary" not in text
    assert "other" not in text
    assert "key-material" not in text
