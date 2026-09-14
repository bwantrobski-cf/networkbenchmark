"""Optional Linux UDP batching with a portable, contract-compatible fallback."""

import errno
import operator
import os
import select
import socket
import struct
import time


_VALID_BACKENDS = {"auto", "native", "python"}
REQUESTED_BACKEND = os.environ.get("NPERF_UDP_BACKEND", "auto").strip().lower()
if REQUESTED_BACKEND not in _VALID_BACKENDS:
    raise ValueError(
        "NPERF_UDP_BACKEND must be one of: auto, native, python"
    )

_native = None
_native_import_error = None
_native_runtime_error = None
try:
    from . import _udp_batch as _native
except (ImportError, OSError) as exc:
    _native_import_error = exc

NATIVE_AVAILABLE = _native is not None
NATIVE_IMPORT_ERROR = _native_import_error
if REQUESTED_BACKEND == "native" and not NATIVE_AVAILABLE:
    raise ImportError(
        "NPERF_UDP_BACKEND=native was requested, but the Linux CPython "
        "UDP batch extension is unavailable"
    ) from NATIVE_IMPORT_ERROR

BACKEND = "native" if NATIVE_AVAILABLE and REQUESTED_BACKEND != "python" else "python"
RXQ_OVERFLOW_SUPPORTED = hasattr(socket, "SO_RXQ_OVFL")

_SO_RXQ_OVFL = getattr(socket, "SO_RXQ_OVFL", None)
_MSG_WAITFORONE = getattr(socket, "MSG_WAITFORONE", 0)
_MSG_DONTWAIT = getattr(socket, "MSG_DONTWAIT", 0)
_SYNTHETIC_MSG_TRUNC = 1 << 30
_MSG_TRUNC = getattr(socket, "MSG_TRUNC", 0)
if not _MSG_TRUNC:
    _MSG_TRUNC = _SYNTHETIC_MSG_TRUNC
    # stream_worker checks this socket constant against returned message flags.
    socket.MSG_TRUNC = _MSG_TRUNC
_MSG_CTRUNC = getattr(socket, "MSG_CTRUNC", 0)
_UDP_MAX_DATAGRAM_SIZE = (1 << 16) - 1
_WSAEMSGSIZE = getattr(errno, "WSAEMSGSIZE", 10040)
_ANCILLARY_SIZE = (
    socket.CMSG_SPACE(struct.calcsize("=I"))
    if RXQ_OVERFLOW_SUPPORTED and hasattr(socket, "CMSG_SPACE")
    else 0
)

__all__ = [
    "BACKEND",
    "NATIVE_AVAILABLE",
    "NATIVE_IMPORT_ERROR",
    "REQUESTED_BACKEND",
    "RXQ_OVERFLOW_SUPPORTED",
    "backend_status",
    "aggregate_backend_status",
    "recvmmsg_into",
    "self_test",
    "sendmmsg",
]


def backend_status():
    """Return the requested/selected backend and optional native import error."""
    return {
        "requested": REQUESTED_BACKEND,
        "selected": BACKEND,
        "native_available": NATIVE_AVAILABLE,
        "native_error": (
            None
            if NATIVE_IMPORT_ERROR is None
            else f"{type(NATIVE_IMPORT_ERROR).__name__}: {NATIVE_IMPORT_ERROR}"
        ),
        "native_runtime_error": _native_runtime_error,
        "rxq_overflow_supported": RXQ_OVERFLOW_SUPPORTED,
    }


def aggregate_backend_status(streams, base_status=None, include_base=True):
    """Combine process-local status with final per-stream worker reports."""
    result = dict(base_status or backend_status())
    reports = []
    selected = set()
    if include_base and result.get("selected"):
        selected.add(result["selected"])
    for stream in streams or []:
        status = stream.get("udp_batch") if isinstance(stream, dict) else None
        backend = status.get("selected") if isinstance(status, dict) else None
        if not backend:
            continue
        selected.add(backend)
        reports.append({
            "index": stream.get("index"),
            "selected": backend,
            "native_runtime_error": status.get("native_runtime_error"),
        })
    if len(selected) > 1:
        result["selected"] = "mixed"
    elif selected:
        result["selected"] = next(iter(selected))
    if reports:
        result["stream_backends"] = reports
    if (not result.get("native_runtime_error")
            and any(report["native_runtime_error"] for report in reports)):
        result["native_runtime_error"] = "reported by data worker"
    return result


def _coerce_flags(flags):
    flags = operator.index(flags)
    if flags < -(1 << 31) or flags > (1 << 31) - 1:
        raise OverflowError("flags do not fit in a C int")
    return flags


def _release_views(views):
    for view in views:
        view.release()


def _buffer_views(buffers, writable):
    items = list(buffers)
    views = []
    try:
        for item in items:
            view = memoryview(item)
            try:
                if writable and view.readonly:
                    raise TypeError(
                        "writable_buffers must contain writable buffers"
                    )
                if not view.c_contiguous:
                    raise BufferError("buffers must be C-contiguous")
                if view.format != "B" or view.ndim != 1:
                    byte_view = view.cast("B")
                    view.release()
                    view = byte_view
            except BaseException:
                view.release()
                raise
            views.append(view)
    except BaseException:
        _release_views(views)
        raise
    return views


def _python_sendmmsg(sock, buffers, address=None, flags=0):
    flags = _coerce_flags(flags)
    views = _buffer_views(buffers, writable=False)
    sent = 0
    try:
        for view in views:
            while True:
                try:
                    if address is None:
                        sock.send(view, flags)
                    else:
                        sock.sendto(view, flags, address)
                    break
                except InterruptedError:
                    continue
                except OSError as exc:
                    if exc.errno == errno.EINTR:
                        continue
                    if sent:
                        return sent
                    raise
            sent += 1
        return sent
    finally:
        _release_views(views)


def _parse_rxq_overflow(ancillary, msg_flags=0):
    if _MSG_CTRUNC and msg_flags & _MSG_CTRUNC:
        return None
    if _SO_RXQ_OVFL is None:
        return None
    for level, kind, data in ancillary:
        if (
            level == socket.SOL_SOCKET
            and kind == _SO_RXQ_OVFL
            and len(data) >= struct.calcsize("=I")
        ):
            return struct.unpack_from("=I", data)[0]
    return None


def _python_recvmmsg_into(sock, writable_buffers, flags=0):
    flags = _coerce_flags(flags)
    views = _buffer_views(writable_buffers, writable=True)
    results = []
    recvmsg_into = getattr(sock, "recvmsg_into", None)
    use_recvmsg = callable(recvmsg_into)
    receive_flags = flags
    scratch = None if use_recvmsg else bytearray(_UDP_MAX_DATAGRAM_SIZE)
    try:
        for view in views:
            while True:
                try:
                    if use_recvmsg:
                        length, ancillary, msg_flags, address = recvmsg_into(
                            (view,), _ANCILLARY_SIZE, receive_flags
                        )
                        overflow = _parse_rxq_overflow(ancillary, msg_flags)
                    else:
                        windows_truncated = False
                        try:
                            received_length, address = sock.recvfrom_into(
                                scratch, len(scratch), receive_flags
                            )
                        except OSError as exc:
                            if (
                                exc.errno != _WSAEMSGSIZE
                                and getattr(exc, "winerror", None) != _WSAEMSGSIZE
                            ):
                                raise
                            # Winsock filled and consumed the oversized datagram,
                            # but Python cannot return its length or source address.
                            received_length = len(scratch)
                            address = None
                            windows_truncated = True
                        length = min(received_length, view.nbytes, len(scratch))
                        view[:length] = scratch[:length]
                        msg_flags = (
                            _MSG_TRUNC
                            if windows_truncated or received_length > length
                            else 0
                        )
                        overflow = None
                    break
                except InterruptedError:
                    continue
                except OSError as exc:
                    if exc.errno == errno.EINTR:
                        continue
                    if results:
                        return results
                    raise
            results.append((length, address, msg_flags, overflow))
            if _MSG_WAITFORONE and flags & _MSG_WAITFORONE:
                receive_flags |= _MSG_DONTWAIT
        return results
    finally:
        _release_views(views)


def _has_positive_timeout(sock):
    gettimeout = getattr(sock, "gettimeout", None)
    if not callable(gettimeout):
        return False
    timeout = gettimeout()
    return timeout is not None and timeout > 0


def _downgrade_native(exc):
    global BACKEND, _native_runtime_error
    _native_runtime_error = f"{type(exc).__name__}: {exc}"
    BACKEND = "python"


def sendmmsg(sock, buffers, address=None, flags=0):
    """Send one datagram per buffer and return the completed message count."""
    if BACKEND == "native" and _has_positive_timeout(sock):
        return _python_sendmmsg(sock, buffers, address, flags)
    if BACKEND == "native":
        try:
            return _native.sendmmsg(sock, buffers, address, flags)
        except OSError as exc:
            if (REQUESTED_BACKEND != "auto"
                    or exc.errno not in (errno.ENOSYS, errno.EPERM, errno.EOPNOTSUPP)):
                raise
            _downgrade_native(exc)
            return _python_sendmmsg(sock, buffers, address, flags)
    return _python_sendmmsg(sock, buffers, address, flags)


def recvmmsg_into(sock, writable_buffers, flags=0):
    """Receive datagrams into buffers and return per-message metadata tuples."""
    if BACKEND == "native" and _has_positive_timeout(sock):
        return _python_recvmmsg_into(sock, writable_buffers, flags)
    if BACKEND == "native":
        try:
            return _native.recvmmsg_into(sock, writable_buffers, flags)
        except OSError as exc:
            if (REQUESTED_BACKEND != "auto"
                    or exc.errno not in (errno.ENOSYS, errno.EPERM, errno.EOPNOTSUPP)):
                raise
            _downgrade_native(exc)
            return _python_recvmmsg_into(sock, writable_buffers, flags)
    return _python_recvmmsg_into(sock, writable_buffers, flags)


def self_test():
    """Exercise the selected backend over each available address family."""
    addresses = [(socket.AF_INET, "127.0.0.1", "IPv4")]
    if socket.has_ipv6:
        addresses.append((socket.AF_INET6, "::1", "IPv6"))
    families = []
    unavailable_families = []
    message_count = 0
    for family, host, label in addresses:
        receiver = None
        sender = None
        payloads = (b"nperf-native-one", b"nperf-native-two")
        try:
            receiver = socket.socket(family, socket.SOCK_DGRAM)
            sender = socket.socket(family, socket.SOCK_DGRAM)
            receiver.bind((host, 0))
            receiver.setblocking(False)
            if sendmmsg(sender, payloads, receiver.getsockname()) != len(payloads):
                raise RuntimeError(f"UDP batch {label} self-test sent a partial batch")
            deadline = time.monotonic() + 2.0
            decoded = []
            while len(decoded) < len(payloads):
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError(
                        f"UDP batch {label} self-test receive timed out")
                readable, _, _ = select.select([receiver], [], [], remaining)
                if not readable:
                    raise RuntimeError(
                        f"UDP batch {label} self-test receive timed out")
                buffers = [bytearray(32)
                           for _ in range(len(payloads) - len(decoded))]
                try:
                    received = recvmmsg_into(receiver, buffers)
                except BlockingIOError:
                    continue
                decoded.extend(bytes(buffer[:metadata[0]])
                               for buffer, metadata in zip(buffers, received))
            if tuple(decoded) != payloads:
                raise RuntimeError(
                    f"UDP batch {label} self-test payload mismatch: {decoded!r}")
            families.append(label)
            message_count += len(payloads)
        except OSError as exc:
            if (family == socket.AF_INET6
                    and exc.errno in (errno.EAFNOSUPPORT, errno.EADDRNOTAVAIL,
                                      errno.EPROTONOSUPPORT)):
                unavailable_families.append(label)
                continue
            raise
        finally:
            if sender is not None:
                sender.close()
            if receiver is not None:
                receiver.close()
    return {
        "ok": True,
        "families": families,
        "unavailable_families": unavailable_families,
        "messages": message_count,
        **backend_status(),
    }
