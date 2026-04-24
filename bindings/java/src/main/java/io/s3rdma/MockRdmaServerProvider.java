package io.s3rdma;

/**
 * Mock RDMA server provider — no NVIDIA hardware required.
 *
 * <p>Delegates to the Rust {@code MockRdmaServerProvider} via JNI.  Uses
 * {@code /dev/shm} as the transfer medium; the client and server must share
 * the same filesystem path (same host, or Docker with {@code ipc: host}).</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (MockRdmaServerProvider srv = new MockRdmaServerProvider(true, "/dev/shm", 128 * 1024 * 1024)) {
 *     // On PUT: read bytes from the client's SHM buffer
 *     byte[] data = srv.handlePut(key, token, 0, 0);
 *
 *     // On GET: write object bytes into the client's SHM buffer
 *     long written = srv.handleGet(key, objectData, token, 0);
 * }
 * }</pre>
 */
public final class MockRdmaServerProvider extends RdmaServerProvider {

    static {
        System.loadLibrary("s3_rdma_jni");
    }

    private final long nativeHandle;

    /**
     * Create a mock server provider.
     *
     * @param debug   enable verbose trace output from the Rust layer
     * @param shmPath directory for shared-memory files (typically {@code "/dev/shm"})
     * @param bufSize capacity of each pool buffer in bytes
     */
    public MockRdmaServerProvider(boolean debug, String shmPath, long bufSize) {
        this.nativeHandle = nativeCreate(debug, shmPath, bufSize);
    }

    // ── RdmaServerProvider implementation ─────────────────────────────────

    @Override public String name()    { return nativeName(nativeHandle); }
    @Override public long  bufSize()  { return nativeBufSize(nativeHandle); }

    @Override
    public byte[] handlePut(byte[] s3Key, byte[] token, long offset, long size)
            throws RdmaException {
        return nativeHandlePut(nativeHandle, s3Key, token, offset, size);
    }

    @Override
    public long handleGet(byte[] s3Key, byte[] data, byte[] token, long offset)
            throws RdmaException {
        return nativeHandleGet(nativeHandle, s3Key, data, token, offset);
    }

    @Override
    public void close() {
        nativeDestroy(nativeHandle);
    }

    // ── Native methods ─────────────────────────────────────────────────────

    private native long   nativeCreate(boolean debug, String shmPath, long bufSize);
    private native void   nativeDestroy(long handle);
    private native String nativeName(long handle);
    private native long   nativeBufSize(long handle);
    private native byte[] nativeHandlePut(long handle, byte[] s3Key, byte[] token, long offset, long size);
    private native long   nativeHandleGet(long handle, byte[] s3Key, byte[] data, byte[] token, long offset);
}
