"""
pytest tests for the s3_rdma Python bindings — mock provider.

No NVIDIA hardware is required; all tests use the shared-memory mock.
"""

import pytest
import s3_rdma

S3_KEY = b"test-bucket/test-object.bin"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def both():
    """Mock provider with both client and server roles."""
    return s3_rdma.MockRdmaProvider()


@pytest.fixture
def client():
    return s3_rdma.RdmaProvider(provider="mock", role="client")


@pytest.fixture
def server():
    return s3_rdma.RdmaProvider(provider="mock", role="server")


# ── Info ──────────────────────────────────────────────────────────────────────

def test_name(both):
    assert both.name() == "MockRdmaProvider"


def test_protocol(both):
    assert both.protocol() == "mock"


def test_supports_client_both(both):
    assert both.supports_client()


def test_supports_server_both(both):
    assert both.supports_server()


def test_supports_client_only(client):
    assert client.supports_client()


def test_supports_server_only(server):
    assert server.supports_server()


def test_repr(both):
    r = repr(both)
    assert "MockRdmaProvider" in r
    assert "mock" in r


# ── RdmaProvider constructor ──────────────────────────────────────────────────

def test_create_generic():
    ep = s3_rdma.RdmaProvider(provider="mock", role="both")
    assert ep.supports_client()
    assert ep.supports_server()


def test_create_invalid_role():
    with pytest.raises(RuntimeError, match="unknown role"):
        s3_rdma.RdmaProvider(provider="mock", role="invalid")


# ── PUT round-trip (both roles, bytes buffer) ─────────────────────────────────

def test_put_roundtrip_bytes(both):
    data = bytes(range(256)) * 4  # 1024 bytes
    both.register_memory(data)
    token = both.prepare_put_token(data, 0, len(data), S3_KEY)
    assert token
    received = both.handle_put(S3_KEY, token, size=len(data))
    assert received == data
    both.deregister_memory(data)


# ── PUT round-trip (both roles, bytearray buffer) ────────────────────────────

def test_put_roundtrip_bytearray(both):
    data = bytearray(b"\xAB" * 512)
    both.register_memory(data)
    token = both.prepare_put_token(data, 0, len(data), S3_KEY)
    received = both.handle_put(S3_KEY, token, size=len(data))
    assert received == bytes(data)
    both.deregister_memory(data)


# ── PUT round-trip (both roles, memoryview) ───────────────────────────────────

def test_put_roundtrip_memoryview(both):
    raw = bytearray(b"\xCC" * 256)
    mv = memoryview(raw)
    both.register_memory(mv)
    token = both.prepare_put_token(mv, 0, len(mv), S3_KEY)
    received = both.handle_put(S3_KEY, token, size=len(mv))
    assert received == bytes(raw)
    both.deregister_memory(mv)


# ── GET round-trip (both roles) ───────────────────────────────────────────────

def test_get_roundtrip(both):
    payload = b"\xDE\xAD\xBE\xEF" * 64  # 256 bytes

    buf = bytearray(len(payload))
    both.register_memory(buf)
    get_token = both.prepare_get_token(buf, 0, len(buf), S3_KEY)

    written = both.handle_get(S3_KEY, payload, get_token)
    assert written == len(payload)

    completed = []
    both.process_rdma_reply(
        200, get_token, len(payload),
        lambda code, reply: completed.append((code, reply)),
    )
    assert completed == [(0, b"")]
    both.deregister_memory(buf)


# ── PUT with offset ───────────────────────────────────────────────────────────

def test_put_with_offset(both):
    prefix = b"\x00" * 64
    payload = b"\xFF" * 64
    data = bytearray(prefix + payload)
    both.register_memory(data)
    token = both.prepare_put_token(data, 64, 64, S3_KEY)
    received = both.handle_put(S3_KEY, token, offset=0, size=64)
    assert received == payload
    both.deregister_memory(data)


# ── Client / server separated ─────────────────────────────────────────────────

def test_client_server_separated(client, server):
    data = bytes(b"\x42" * 128)
    client.register_memory(data)
    token = client.prepare_put_token(data, 0, len(data), S3_KEY)
    received = server.handle_put(S3_KEY, token, size=len(data))
    assert received == data
    client.deregister_memory(data)


# ── process_rdma_reply callback ───────────────────────────────────────────────

def test_process_rdma_reply_callback(both):
    data = b"callback test"
    both.register_memory(data)
    token = both.prepare_put_token(data, 0, len(data), S3_KEY)
    results = []
    both.process_rdma_reply(
        200, token, len(data),
        lambda code, reply: results.append((code, bytes(reply))),
    )
    assert len(results) == 1
    code, reply = results[0]
    assert code == 0
    both.deregister_memory(data)


# ── Error cases ───────────────────────────────────────────────────────────────

def test_handle_put_server_only_provider(server):
    """Server can handle a PUT without being the one to register memory."""
    client_ep = s3_rdma.RdmaProvider(provider="mock", role="client")
    data = b"\x11" * 64
    client_ep.register_memory(data)
    token = client_ep.prepare_put_token(data, 0, len(data), S3_KEY)
    received = server.handle_put(S3_KEY, token, size=len(data))
    assert received == data
    client_ep.deregister_memory(data)


def test_buf_size(both):
    bs = both.buf_size()
    assert isinstance(bs, int)
    assert bs > 0


# ── CuObjProvider fallback ────────────────────────────────────────────────────

def test_cuobj_raises_without_plugin():
    with pytest.raises(RuntimeError):
        s3_rdma.CuObjProvider(role="client")
