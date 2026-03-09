"""
s3-rdma Python binding example.

Demonstrates MockRdmaProvider: register memory, generate PUT/GET tokens,
and process a server reply token — all without NVIDIA hardware.

Run after building the extension with:
    cd python && python3 -m maturin develop
    python3 python/examples/example.py
"""

import s3_rdma


def main() -> None:
    print("=== s3-rdma Python example ===\n")

    provider = s3_rdma.MockRdmaProvider(debug=True)
    print(f"Provider : {provider.name()}")
    print(f"Version  : {provider.version()}")

    data  = bytes(b"\x42" * 1024 * 1024)  # 1 MiB of data
    s3key = b"my-bucket/my-object.bin"

    print("\n[is_memory_suitable]")
    import ctypes
    ptr = ctypes.addressof((ctypes.c_char * len(data)).from_buffer_copy(data))
    print(f"  suitable (ptr): {provider.is_memory_suitable(ptr, len(data))}")

    print("\n[register_memory]")
    provider.register_memory(data, )

    print("\n[prepare_put_token]  (S3 PUT / upload)")
    put_token = provider.prepare_put_token(data, 0, len(data), s3key)
    print(f"  token: {put_token.decode()}")

    print("\n[prepare_get_token]  (S3 GET / download)")
    get_token = provider.prepare_get_token(data, 0, len(data), s3key)
    print(f"  token: {get_token.decode()}")

    print("\n[process_reply_token]")
    def on_complete(error_code: int, reply: bytes) -> None:
        print(f"  callback: error_code={error_code}, reply={reply.decode()}")

    provider.process_reply_token(b"mock-server-reply-ok", on_complete)

    print("\n[deregister_memory]")
    provider.deregister_memory(data)

    print(f"\nProvider info: {s3_rdma.rdma_provider_info()}")
    print("\nDone.")


if __name__ == "__main__":
    main()
