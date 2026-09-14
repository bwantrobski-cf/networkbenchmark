#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#ifdef __linux__

#include <arpa/inet.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#define UDP_BATCH_CONTROL_SIZE 256
#define UDP_BATCH_MAX_MESSAGES 1024
#define UDP_BATCH_KEYWORD_METHOD(function) \
    ((PyCFunction)(void (*)(void))(function))

typedef union {
    struct cmsghdr alignment;
    unsigned char bytes[UDP_BATCH_CONTROL_SIZE];
} UdpBatchControl;

static int
udp_socket_family(int fd, int *family)
{
    struct sockaddr_storage address;
    socklen_t address_len = sizeof(address);

    if (getsockname(fd, (struct sockaddr *)&address, &address_len) < 0) {
        PyErr_SetFromErrno(PyExc_OSError);
        return -1;
    }
    if (address.ss_family != AF_INET && address.ss_family != AF_INET6) {
        PyErr_SetString(PyExc_ValueError,
                        "only IPv4 and IPv6 sockets are supported");
        return -1;
    }
    *family = address.ss_family;
    return 0;
}

static int
udp_parse_uint32(PyObject *value, const char *name, uint32_t *result)
{
    unsigned long parsed = PyLong_AsUnsignedLong(value);

    if (parsed == (unsigned long)-1 && PyErr_Occurred()) {
        return -1;
    }
    if (parsed > UINT32_MAX) {
        PyErr_Format(PyExc_OverflowError, "%s does not fit in 32 bits", name);
        return -1;
    }
    *result = (uint32_t)parsed;
    return 0;
}

static int
udp_parse_address(int fd, PyObject *address, struct sockaddr_storage *storage,
                  socklen_t *storage_len)
{
    int family;
    Py_ssize_t item_count;
    PyObject *host_object;
    PyObject *port_object;
    const char *host;
    char *bytes_host;
    Py_ssize_t host_len;
    long port;

    if (udp_socket_family(fd, &family) < 0) {
        return -1;
    }
    if (!PyTuple_Check(address)) {
        PyErr_SetString(PyExc_TypeError, "address must be a socket address tuple");
        return -1;
    }

    item_count = PyTuple_GET_SIZE(address);
    if ((family == AF_INET && item_count != 2) ||
        (family == AF_INET6 && (item_count < 2 || item_count > 4))) {
        PyErr_SetString(PyExc_TypeError,
                        family == AF_INET
                            ? "IPv4 address must be a (host, port) tuple"
                            : "IPv6 address must be a (host, port[, flowinfo[, scope_id]]) tuple");
        return -1;
    }

    host_object = PyTuple_GET_ITEM(address, 0);
    port_object = PyTuple_GET_ITEM(address, 1);
    if (PyUnicode_Check(host_object)) {
        host = PyUnicode_AsUTF8AndSize(host_object, &host_len);
        if (host == NULL) {
            return -1;
        }
    } else if (PyBytes_Check(host_object)) {
        if (PyBytes_AsStringAndSize(host_object, &bytes_host, &host_len) < 0) {
            return -1;
        }
        host = bytes_host;
    } else {
        PyErr_SetString(PyExc_TypeError, "address host must be str or bytes");
        return -1;
    }
    if (memchr(host, '\0', (size_t)host_len) != NULL) {
        PyErr_SetString(PyExc_ValueError, "address host contains a null byte");
        return -1;
    }

    port = PyLong_AsLong(port_object);
    if (port == -1 && PyErr_Occurred()) {
        return -1;
    }
    if (port < 0 || port > 65535) {
        PyErr_SetString(PyExc_OverflowError, "address port must be 0-65535");
        return -1;
    }

    memset(storage, 0, sizeof(*storage));
    if (family == AF_INET) {
        struct sockaddr_in *ipv4 = (struct sockaddr_in *)storage;
        int parsed = inet_pton(AF_INET, host, &ipv4->sin_addr);

        if (parsed != 1) {
            PyErr_SetString(PyExc_ValueError,
                            "native UDP destination must be a numeric IPv4 address");
            return -1;
        }
        ipv4->sin_family = AF_INET;
        ipv4->sin_port = htons((uint16_t)port);
        *storage_len = sizeof(*ipv4);
    } else {
        struct sockaddr_in6 *ipv6 = (struct sockaddr_in6 *)storage;
        uint32_t parsed;
        int address_parsed = inet_pton(AF_INET6, host, &ipv6->sin6_addr);

        if (address_parsed != 1) {
            PyErr_SetString(PyExc_ValueError,
                            "native UDP destination must be a numeric IPv6 address");
            return -1;
        }
        ipv6->sin6_family = AF_INET6;
        ipv6->sin6_port = htons((uint16_t)port);
        *storage_len = sizeof(*ipv6);

        if (item_count >= 3) {
            if (udp_parse_uint32(PyTuple_GET_ITEM(address, 2), "flowinfo", &parsed) < 0) {
                return -1;
            }
            ipv6->sin6_flowinfo = htonl(parsed);
        }
        if (item_count >= 4) {
            if (udp_parse_uint32(PyTuple_GET_ITEM(address, 3), "scope_id", &parsed) < 0) {
                return -1;
            }
            ipv6->sin6_scope_id = parsed;
        }
    }
    return 0;
}

static PyObject *
udp_address_to_python(const struct sockaddr_storage *storage, socklen_t address_len)
{
    char host[INET6_ADDRSTRLEN];

    if (address_len == 0) {
        Py_RETURN_NONE;
    }
    if (storage->ss_family == AF_INET) {
        const struct sockaddr_in *ipv4 = (const struct sockaddr_in *)storage;

        if (address_len < sizeof(*ipv4)) {
            PyErr_SetString(PyExc_OSError, "received a truncated IPv4 address");
            return NULL;
        }
        if (inet_ntop(AF_INET, &ipv4->sin_addr, host, sizeof(host)) == NULL) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        return Py_BuildValue("(si)", host, (int)ntohs(ipv4->sin_port));
    }
    if (storage->ss_family == AF_INET6) {
        const struct sockaddr_in6 *ipv6 = (const struct sockaddr_in6 *)storage;

        if (address_len < sizeof(*ipv6)) {
            PyErr_SetString(PyExc_OSError, "received a truncated IPv6 address");
            return NULL;
        }
        if (inet_ntop(AF_INET6, &ipv6->sin6_addr, host, sizeof(host)) == NULL) {
            return PyErr_SetFromErrno(PyExc_OSError);
        }
        return Py_BuildValue("(sikk)", host, (int)ntohs(ipv6->sin6_port),
                             (unsigned long)ntohl(ipv6->sin6_flowinfo),
                             (unsigned long)ipv6->sin6_scope_id);
    }

    PyErr_SetString(PyExc_OSError, "received an unsupported socket address");
    return NULL;
}

static PyObject *
udp_rxq_overflow(const struct msghdr *message)
{
#ifdef MSG_CTRUNC
    if (message->msg_flags & MSG_CTRUNC) {
        Py_RETURN_NONE;
    }
#endif
#ifdef SO_RXQ_OVFL
    struct cmsghdr *control;

    for (control = CMSG_FIRSTHDR(message); control != NULL;
         control = CMSG_NXTHDR((struct msghdr *)message, control)) {
        if (control->cmsg_level == SOL_SOCKET &&
            control->cmsg_type == SO_RXQ_OVFL &&
            control->cmsg_len >= CMSG_LEN(sizeof(uint32_t))) {
            uint32_t overflow;

            memcpy(&overflow, CMSG_DATA(control), sizeof(overflow));
            return PyLong_FromUnsignedLong((unsigned long)overflow);
        }
    }
#else
    (void)message;
#endif
    Py_RETURN_NONE;
}

static void
udp_release_buffers(Py_buffer *views, Py_ssize_t acquired)
{
    Py_ssize_t index;

    for (index = 0; index < acquired; index++) {
        PyBuffer_Release(&views[index]);
    }
}

static PyObject *
udp_sendmmsg(PyObject *self, PyObject *args, PyObject *kwargs)
{
    static char *keywords[] = {"sock", "buffers", "address", "flags", NULL};
    PyObject *socket_object;
    PyObject *buffers_object;
    PyObject *address_object = Py_None;
    int flags = 0;
    PyObject *buffers = NULL;
    Py_buffer *views = NULL;
    struct iovec *iovecs = NULL;
    struct mmsghdr *messages = NULL;
    struct sockaddr_storage address;
    socklen_t address_len = 0;
    Py_ssize_t count;
    Py_ssize_t acquired = 0;
    Py_ssize_t index;
    int fd;
    int sent;
    int saved_errno;
    PyObject *result = NULL;

    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|Oi:sendmmsg", keywords,
                                     &socket_object, &buffers_object,
                                     &address_object, &flags)) {
        return NULL;
    }
    buffers = PySequence_Fast(buffers_object, "buffers must be iterable");
    if (buffers == NULL) {
        return NULL;
    }
    count = PySequence_Fast_GET_SIZE(buffers);
    if (count > UDP_BATCH_MAX_MESSAGES) {
        PyErr_Format(PyExc_OverflowError,
                     "UDP batch is limited to %d datagrams",
                     UDP_BATCH_MAX_MESSAGES);
        goto done;
    }
    if (count == 0) {
        result = PyLong_FromLong(0);
        goto done;
    }
    fd = PyObject_AsFileDescriptor(socket_object);
    if (fd < 0) {
        goto done;
    }
    if (address_object != Py_None &&
        udp_parse_address(fd, address_object, &address, &address_len) < 0) {
        goto done;
    }

    views = PyMem_Calloc((size_t)count, sizeof(*views));
    iovecs = PyMem_Calloc((size_t)count, sizeof(*iovecs));
    messages = PyMem_Calloc((size_t)count, sizeof(*messages));
    if (views == NULL || iovecs == NULL || messages == NULL) {
        PyErr_NoMemory();
        goto done;
    }
    for (index = 0; index < count; index++) {
        PyObject *item = PySequence_Fast_GET_ITEM(buffers, index);

        if (PyObject_GetBuffer(item, &views[index], PyBUF_SIMPLE) < 0) {
            goto done;
        }
        acquired++;
        iovecs[index].iov_base = views[index].buf;
        iovecs[index].iov_len = (size_t)views[index].len;
        messages[index].msg_hdr.msg_iov = &iovecs[index];
        messages[index].msg_hdr.msg_iovlen = 1;
        if (address_object != Py_None) {
            messages[index].msg_hdr.msg_name = &address;
            messages[index].msg_hdr.msg_namelen = address_len;
        }
    }

    for (;;) {
        Py_BEGIN_ALLOW_THREADS
        sent = sendmmsg(fd, messages, (unsigned int)count, flags);
        saved_errno = errno;
        Py_END_ALLOW_THREADS
        if (sent >= 0) {
            break;
        }
        if (saved_errno != EINTR) {
            errno = saved_errno;
            PyErr_SetFromErrno(PyExc_OSError);
            goto done;
        }
        if (PyErr_CheckSignals() < 0) {
            goto done;
        }
    }
    result = PyLong_FromLong(sent);

done:
    if (views != NULL) {
        udp_release_buffers(views, acquired);
    }
    PyMem_Free(messages);
    PyMem_Free(iovecs);
    PyMem_Free(views);
    Py_XDECREF(buffers);
    return result;
}

static PyObject *
udp_recvmmsg_into(PyObject *self, PyObject *args, PyObject *kwargs)
{
    static char *keywords[] = {"sock", "writable_buffers", "flags", NULL};
    PyObject *socket_object;
    PyObject *buffers_object;
    int flags = 0;
    PyObject *buffers = NULL;
    Py_buffer *views = NULL;
    struct iovec *iovecs = NULL;
    struct mmsghdr *messages = NULL;
    struct sockaddr_storage *addresses = NULL;
    UdpBatchControl *controls = NULL;
    Py_ssize_t count;
    Py_ssize_t acquired = 0;
    Py_ssize_t index;
    int fd;
    int family;
    int received;
    int saved_errno;
    PyObject *result = NULL;

    (void)self;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|i:recvmmsg_into", keywords,
                                     &socket_object, &buffers_object, &flags)) {
        return NULL;
    }
    buffers = PySequence_Fast(buffers_object, "writable_buffers must be iterable");
    if (buffers == NULL) {
        return NULL;
    }
    count = PySequence_Fast_GET_SIZE(buffers);
    if (count > UDP_BATCH_MAX_MESSAGES) {
        PyErr_Format(PyExc_OverflowError,
                     "UDP batch is limited to %d datagrams",
                     UDP_BATCH_MAX_MESSAGES);
        goto done;
    }
    if (count == 0) {
        result = PyList_New(0);
        goto done;
    }
    fd = PyObject_AsFileDescriptor(socket_object);
    if (fd < 0 || udp_socket_family(fd, &family) < 0) {
        goto done;
    }
    (void)family;

    views = PyMem_Calloc((size_t)count, sizeof(*views));
    iovecs = PyMem_Calloc((size_t)count, sizeof(*iovecs));
    messages = PyMem_Calloc((size_t)count, sizeof(*messages));
    addresses = PyMem_Calloc((size_t)count, sizeof(*addresses));
    controls = PyMem_Calloc((size_t)count, sizeof(*controls));
    if (views == NULL || iovecs == NULL || messages == NULL ||
        addresses == NULL || controls == NULL) {
        PyErr_NoMemory();
        goto done;
    }
    for (index = 0; index < count; index++) {
        PyObject *item = PySequence_Fast_GET_ITEM(buffers, index);

        if (PyObject_GetBuffer(item, &views[index],
                               PyBUF_WRITABLE | PyBUF_SIMPLE) < 0) {
            goto done;
        }
        acquired++;
        iovecs[index].iov_base = views[index].buf;
        iovecs[index].iov_len = (size_t)views[index].len;
        messages[index].msg_hdr.msg_name = &addresses[index];
        messages[index].msg_hdr.msg_namelen = sizeof(addresses[index]);
        messages[index].msg_hdr.msg_iov = &iovecs[index];
        messages[index].msg_hdr.msg_iovlen = 1;
        messages[index].msg_hdr.msg_control = controls[index].bytes;
        messages[index].msg_hdr.msg_controllen = sizeof(controls[index].bytes);
    }

    for (;;) {
        Py_BEGIN_ALLOW_THREADS
        received = recvmmsg(fd, messages, (unsigned int)count, flags, NULL);
        saved_errno = errno;
        Py_END_ALLOW_THREADS
        if (received >= 0) {
            break;
        }
        if (saved_errno != EINTR) {
            errno = saved_errno;
            PyErr_SetFromErrno(PyExc_OSError);
            goto done;
        }
        if (PyErr_CheckSignals() < 0) {
            goto done;
        }
    }

    result = PyList_New(received);
    if (result == NULL) {
        goto done;
    }
    for (index = 0; index < received; index++) {
        PyObject *length_object = NULL;
        PyObject *address_object = NULL;
        PyObject *flags_object = NULL;
        PyObject *overflow_object = NULL;
        PyObject *item = NULL;

        length_object = PyLong_FromUnsignedLong(messages[index].msg_len);
        address_object = udp_address_to_python(&addresses[index],
                                               messages[index].msg_hdr.msg_namelen);
        flags_object = PyLong_FromLong(messages[index].msg_hdr.msg_flags);
        overflow_object = udp_rxq_overflow(&messages[index].msg_hdr);
        if (length_object == NULL || address_object == NULL || flags_object == NULL ||
            overflow_object == NULL) {
            Py_XDECREF(length_object);
            Py_XDECREF(address_object);
            Py_XDECREF(flags_object);
            Py_XDECREF(overflow_object);
            Py_CLEAR(result);
            goto done;
        }
        item = PyTuple_Pack(4, length_object, address_object, flags_object,
                            overflow_object);
        Py_DECREF(length_object);
        Py_DECREF(address_object);
        Py_DECREF(flags_object);
        Py_DECREF(overflow_object);
        if (item == NULL) {
            Py_CLEAR(result);
            goto done;
        }
        PyList_SET_ITEM(result, index, item);
    }

done:
    if (views != NULL) {
        udp_release_buffers(views, acquired);
    }
    PyMem_Free(controls);
    PyMem_Free(addresses);
    PyMem_Free(messages);
    PyMem_Free(iovecs);
    PyMem_Free(views);
    Py_XDECREF(buffers);
    return result;
}

PyDoc_STRVAR(
    udp_sendmmsg_doc,
    "sendmmsg(sock, buffers, address=None, flags=0) -> count\n\n"
    "Send one datagram per buffer with the Linux sendmmsg syscall.");

PyDoc_STRVAR(
    udp_recvmmsg_into_doc,
    "recvmmsg_into(sock, writable_buffers, flags=0) -> list\n\n"
    "Receive one datagram into each buffer with the Linux recvmmsg syscall.");

static PyMethodDef udp_batch_methods[] = {
    {"sendmmsg", UDP_BATCH_KEYWORD_METHOD(udp_sendmmsg),
     METH_VARARGS | METH_KEYWORDS,
     udp_sendmmsg_doc},
    {"recvmmsg_into", UDP_BATCH_KEYWORD_METHOD(udp_recvmmsg_into),
     METH_VARARGS | METH_KEYWORDS, udp_recvmmsg_into_doc},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef udp_batch_module = {
    PyModuleDef_HEAD_INIT,
    "_udp_batch",
    "Linux UDP batch syscall adapter.",
    -1,
    udp_batch_methods,
    NULL,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC
PyInit__udp_batch(void)
{
    return PyModule_Create(&udp_batch_module);
}

#else
#error "nperf.transport._udp_batch is supported only on Linux"
#endif
