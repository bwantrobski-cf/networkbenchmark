"""Address resolution and RFC1918 / CGNAT classification helpers."""

import ipaddress
import socket

CGNAT_NETWORK = ipaddress.ip_network("100.64.0.0/10")


def resolve_host(host: str) -> str:
    """Resolve a hostname or IP string to an IP address string."""
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        pass

    try:
        return socket.gethostbyname(host)
    except socket.gaierror as exc:
        raise ValueError(f"unable to resolve hostname '{host}': {exc}") from exc


def is_rfc1918_or_cgnat(ip: str) -> bool:
    """True if the IP is RFC1918 private, loopback, link-local, or CGNAT
    (100.64.0.0/10), meaning WHOIS/ASN/reverse lookups should be skipped."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False

    if addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_multicast:
        return True

    if addr.version == 4:
        if addr.is_private:
            return True
        if addr in CGNAT_NETWORK:
            return True
        return False

    # IPv6: unique local / private best-effort
    return bool(addr.is_private)


def is_public_ip(ip: str) -> bool:
    return not is_rfc1918_or_cgnat(ip)


def is_ipv4(ip: str) -> bool:
    try:
        return ipaddress.ip_address(ip).version == 4
    except ValueError:
        return False
