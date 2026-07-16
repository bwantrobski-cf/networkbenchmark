import ipaddress
import socket


def family_for_ipv6(ipv6=False):
    return socket.AF_INET6 if ipv6 else socket.AF_INET


def family_label(family):
    return "IPv6" if family == socket.AF_INET6 else "IPv4"


def ip_version_label(ipv6=False, family=None):
    selected_family = family if family is not None else family_for_ipv6(ipv6)
    return family_label(selected_family)


def resolve_host(host, family=socket.AF_INET):
    try:
        parsed = ipaddress.ip_address(host.split("%", 1)[0])
        if parsed.version == 6 and family != socket.AF_INET6:
            raise ValueError(f"host '{host}' is IPv6 but IPv4 was requested")
        if parsed.version == 4 and family != socket.AF_INET:
            raise ValueError(f"host '{host}' is IPv4 but IPv6 was requested")
    except ValueError as exc:
        if "was requested" in str(exc):
            raise
    try:
        infos = socket.getaddrinfo(host, None, family, socket.SOCK_DGRAM, 0)
    except socket.gaierror as exc:
        raise ValueError(f"unable to resolve hostname '{host}' as {family_label(family)}: {exc}") from exc
    if not infos:
        raise ValueError(f"unable to resolve hostname '{host}' as {family_label(family)}")
    return infos[0][4][0]


def resolve_endpoint(host, port, family=socket.AF_INET, socktype=0):
    try:
        infos = socket.getaddrinfo(host, port, family, socktype, 0)
    except socket.gaierror as exc:
        raise ValueError(f"unable to resolve endpoint '{host}:{port}' as {family_label(family)}: {exc}") from exc
    if not infos:
        raise ValueError(f"unable to resolve endpoint '{host}:{port}' as {family_label(family)}")
    selected = infos[0]
    return selected[0], selected[4]


def sockaddr(host, port, family, scope_id=0):
    if family == socket.AF_INET6:
        return (host, port, 0, scope_id)
    return (host, port)


def bind_targets(bind, port, socktype):
    if bind is None or bind == "" or bind in ("0.0.0.0", "*"):
        return [
            (socket.AF_INET, ("0.0.0.0", port)),
            (socket.AF_INET6, ("::", port, 0, 0)),
        ]
    try:
        parsed = ipaddress.ip_address(bind.split("%", 1)[0])
        family = socket.AF_INET6 if parsed.version == 6 else socket.AF_INET
        return [(family, sockaddr(bind, port, family))]
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(bind, port, socket.AF_UNSPEC, socktype, 0, socket.AI_PASSIVE)
    except socket.gaierror:
        return []
    targets = []
    seen = set()
    for family, _socktype, _proto, _canonname, address in infos:
        key = (family, address)
        if key not in seen:
            seen.add(key)
            targets.append(key)
    return targets


def bind_address(bind, port, family):
    if bind is None or bind == "" or bind in ("0.0.0.0", "*"):
        return ("::", port, 0, 0) if family == socket.AF_INET6 else ("0.0.0.0", port)
    return sockaddr(bind, port, family)


def configure_ipv6_socket(sock):
    if sock.family != socket.AF_INET6:
        return
    option = getattr(socket, "IPV6_V6ONLY", None)
    if option is not None:
        sock.setsockopt(socket.IPPROTO_IPV6, option, 1)


def open_server_sockets(bind, port, socktype, listener=False, log=None, service="socket"):
    sockets = []
    targets = bind_targets(bind, port, socktype)
    if not targets and log:
        log(f"{service}: no usable bind address for {bind}:{port}")
    for family, address in targets:
        label = family_label(family)
        sock = None
        try:
            sock = socket.socket(family, socktype)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            configure_ipv6_socket(sock)
            sock.bind(address)
            if listener:
                sock.listen(64)
            sockets.append(sock)
        except OSError as exc:
            if log:
                log(f"{service}: {label} listener unavailable on {address}: {exc}")
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
    return sockets
