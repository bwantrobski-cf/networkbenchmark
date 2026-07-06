"""Reverse DNS, WHOIS, and ASN lookups for MTR hop annotation.

Lookups are skipped entirely for RFC1918 / loopback / link-local / CGNAT
(100.64.0.0/10) addresses per requirements.
"""

import socket
from functools import lru_cache

from . import addr

_WHOIS_TIMEOUT_SEC = 3.0


def reverse_dns(ip: str) -> str:
    try:
        host, _, _ = socket.gethostbyaddr(ip)
        return host
    except (socket.herror, socket.gaierror, OSError):
        return ""


@lru_cache(maxsize=512)
def _rdap_lookup(ip: str) -> dict:
    try:
        from ipwhois import IPWhois
        obj = IPWhois(ip)
        result = obj.lookup_rdap(depth=1, retry_count=1, rate_limit_timeout=int(_WHOIS_TIMEOUT_SEC))
        return result or {}
    except Exception:
        return {}


def asn_lookup(ip: str) -> dict:
    """Return {'asn': str, 'asn_description': str, 'asn_country_code': str}."""
    result = _rdap_lookup(ip)
    if not result:
        return {"asn": "", "asn_description": "", "asn_country_code": ""}
    return {
        "asn": result.get("asn") or "",
        "asn_description": result.get("asn_description") or "",
        "asn_country_code": result.get("asn_country_code") or "",
    }


def whois_org(ip: str) -> str:
    result = _rdap_lookup(ip)
    if not result:
        return ""
    network = result.get("network") or {}
    name = network.get("name")
    if name:
        return name
    for entity in result.get("objects", {}).values():
        contact = entity.get("contact") or {}
        if contact.get("name"):
            return contact["name"]
    return ""


def get_ip_info(ip: str) -> dict:
    """Combined lookup used by MTR to annotate each hop."""
    info = {
        "ip": ip,
        "reverse_dns": "",
        "asn": "",
        "asn_description": "",
        "whois_org": "",
        "lookup_skipped": False,
    }

    if not ip:
        return info

    if addr.is_rfc1918_or_cgnat(ip):
        info["lookup_skipped"] = True
        info["reverse_dns"] = reverse_dns(ip)
        return info

    info["reverse_dns"] = reverse_dns(ip)
    asn_data = asn_lookup(ip)
    info["asn"] = asn_data["asn"]
    info["asn_description"] = asn_data["asn_description"]
    info["whois_org"] = whois_org(ip)
    return info
