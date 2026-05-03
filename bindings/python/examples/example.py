"""
s3-rdma Python binding example.

Demonstrates the unified ``RdmaEndpoint`` API with both mock and cuObj
providers for client and server roles — no NVIDIA hardware required for
the mock provider.

Run after building the extension with:
    cd bindings/python && maturin develop
    python3 bindings/python/examples/example.py
"""

import s3_rdma


def demo_mock_both_roles() -> None:
    """Mock provider, both roles in the same process (typical for testing)."""
    print("=== Mock endpoint — both roles ===\n")

    ep = s3_rdma.MockRdmaEndpoint(debug=True)
    print(f"  endpoint : {ep!r}")
    print(f"  client?  : {ep.supports_client()}")
    print(f"  server?  : {ep.supports_server()}")

    data  = bytearray(b"\x42" * 64 * 1024)  # 64 KiB
    s3key = b"my-bucket/my-object.bin"

    ep.register_memory(bytes(data))

    # Client: generate a PUT token (data is copied to SHM)
    put_token = ep.prepare_put_token(bytes(data), 0, len(data), s3key)
    print(f"\n  put_token : {put_token.decode()[:60]}…")

    # Server: read the data back via the token
    received = ep.handle_put(s3key, put_token, size=len(data))
    assert received == bytes(data), "PUT round-trip mismatch"
    print(f"  handle_put: got {len(received)} bytes ✓")

    # Client: generate a GET token (server will fill SHM)
    get_token = ep.prepare_get_token(bytes(data), 0, len(data), s3key)
    print(f"\n  get_token : {get_token.decode()[:60]}…")

    # Server: write data back via the token
    payload = b"\xde\xad\xbe\xef" * (len(data) // 4)
    written = ep.handle_get(s3key, payload, get_token)
    print(f"  handle_get: wrote {written} bytes ✓")

    # Client: process the server's reply (copies SHM back into buffer)
    def on_complete(error_code: int, reply: bytes) -> None:
        print(f"  callback  : error_code={error_code}")

    ep.process_rdma_reply(200, get_token, written, on_complete)
    ep.deregister_memory(bytes(data))
    print()


def demo_mock_client_role() -> None:
    """Mock provider, client role only."""
    print("=== Mock endpoint — client role only ===\n")

    ep = s3_rdma.RdmaEndpoint(provider="mock", role="client", debug=True)
    print(f"  endpoint : {ep!r}")

    data  = bytes(b"\x11" * 1024)
    s3key = b"bucket/upload.bin"

    ep.register_memory(data)
    token = ep.prepare_put_token(data, 0, len(data), s3key)
    print(f"  token    : {token.decode()[:60]}…")

    ep.process_rdma_reply(200, token, len(data), lambda c, r: None)
    ep.deregister_memory(data)
    print()


def demo_mock_server_role() -> None:
    """Mock provider, server role only — token produced by a separate client ep."""
    print("=== Mock endpoint — server role only ===\n")

    client = s3_rdma.RdmaEndpoint(provider="mock", role="client")
    server = s3_rdma.RdmaEndpoint(provider="mock", role="server")
    print(f"  server endpoint : {server!r}")

    data  = bytes(b"\x22" * 1024)
    s3key = b"bucket/download.bin"

    client.register_memory(data)
    put_token = client.prepare_put_token(data, 0, len(data), s3key)

    received = server.handle_put(s3key, put_token, size=len(data))
    assert received == data, "Server PUT data mismatch"
    print(f"  handle_put: received {len(received)} bytes ✓")

    get_token = client.prepare_get_token(data, 0, len(data), s3key)
    written = server.handle_get(s3key, b"\xff" * len(data), get_token)
    print(f"  handle_get: wrote {written} bytes ✓")
    client.deregister_memory(data)
    print()


def demo_cuobj_auto() -> None:
    """Auto-selection: tries cuObj first, falls back to mock gracefully."""
    print("=== Auto endpoint (cuobj → mock fallback) ===\n")

    ep = s3_rdma.RdmaEndpoint(provider="auto", role="both", debug=True)
    print(f"  endpoint : {ep!r}")
    print(f"  protocol : {ep.protocol()}")
    print()


def main() -> None:
    print("=== s3-rdma Python example ===\n")
    print(f"Provider info: {s3_rdma.rdma_provider_info()}\n")

    demo_mock_both_roles()
    demo_mock_client_role()
    demo_mock_server_role()
    demo_cuobj_auto()

    # CuObjEndpoint convenience class: raises RuntimeError when the plugin is absent.
    print("=== CuObjEndpoint (client) — expecting RuntimeError without plugin ===")
    try:
        ep = s3_rdma.CuObjEndpoint(role="client")
        print(f"  cuobj available: {ep!r}")
    except RuntimeError as exc:
        print(f"  cuObj unavailable (expected): {exc}")

    print("\nDone.")


if __name__ == "__main__":
    main()
