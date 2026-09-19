from unittest.mock import patch

from dispatch.models import Recipient
from dispatch.validation import SyntaxOnlyValidator, SyntaxAndMXValidator, ValidationResult


def test_syntax_only_rejects_malformed():
    v = SyntaxOnlyValidator()
    assert v.validate(Recipient(email="not-an-email")).valid is False
    assert v.validate(Recipient(email="a@b.com")).valid is True


def test_mx_validator_rejects_malformed_without_dns_call():
    v = SyntaxAndMXValidator()
    with patch("dispatch.validation.dns.resolver.Resolver") as mock_resolver_cls:
        result = v.validate(Recipient(email="not-an-email"))
        mock_resolver_cls.assert_not_called()
    assert result.valid is False
    assert "malformed" in result.reason


def test_mx_validator_accepts_domain_with_mx_record():
    v = SyntaxAndMXValidator()
    with patch.object(v, "_check_domain", return_value=ValidationResult(valid=True)):
        result = v.validate(Recipient(email="a@example.com"))
    assert result.valid is True


def test_mx_validator_rejects_nxdomain():
    v = SyntaxAndMXValidator()
    with patch.object(v, "_check_domain") as mock_check:
        mock_check.return_value = ValidationResult(valid=False, reason="domain does not exist: nope.invalid")
        result = v.validate(Recipient(email="a@nope.invalid"))
    assert result.valid is False
    assert "does not exist" in result.reason


def test_mx_validator_caches_per_domain():
    v = SyntaxAndMXValidator(ttl_seconds=999)
    with patch.object(v, "_check_domain") as mock_check:
        mock_check.return_value = ValidationResult(valid=True)
        v.validate(Recipient(email="a@example.com"))
    mock_check.assert_called_once()  # second call served from cache


def test_mx_validator_rejects_null_mx():
    import dns.rdata
    import dns.rdataclass
    import dns.rdatatype

    v = SyntaxAndMXValidator()
    null_mx = dns.rdata.from_text(dns.rdataclass.IN, dns.rdatatype.MX, "0 .")

    with patch("dispatch.validation.dns.resolver.Resolver") as mock_resolver_cls:
        instance = mock_resolver_cls.return_value
        instance.resolve.return_value = [null_mx]
        result = v.validate(Recipient(email="test@nomail.example"))

    assert result.valid is False
    assert "RFC 7505 Null MX" in result.reason

