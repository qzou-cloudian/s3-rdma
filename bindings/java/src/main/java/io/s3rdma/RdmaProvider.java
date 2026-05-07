package io.s3rdma;

import java.nio.ByteBuffer;

/**
 * Unified RDMA provider supporting client, server, or both roles.
 *
 * <p>Wraps the Rust {@code RdmaProvider} trait via JNI and supports all
 * provider types ({@code "mock"}, {@code "cuobj"}, {@code "auto"}) and roles
 * ({@code "client"}, {@code "server"}, {@code "both"}).</p>
 *
 * <h2>Quick start — client role</h2>
 * <pre>{@code
 * ByteBuffer buf = ByteBuffer.allocateDirect(4096);
 * buf.put(data);  // fill with object data
 * try (RdmaProvider ep = RdmaProvider.create("mock", "client", RdmaProviderConfig.defaults())) {
 *     ep.registerMemory(buf);
 *     byte[] token = ep.preparePutToken(key, buf, 0, buf.capacity());
 *     // Attach token as "x-amz-rdma-token" header on the S3 PUT request.
 *     // After receiving "x-amz-rdma-reply" from the server:
 *     ep.processRdmaReply(200, token, buf.capacity(), (code, reply) -> { ... });
 *     ep.deregisterMemory(buf);
 * }
 * }</pre>
 *
 * <h2>Quick start — server role</h2>
 * <pre>{@code
 * RdmaProviderConfig cfg = RdmaProviderConfig.defaults();
 * cfg.listenAddr = "192.168.1.1";
 * cfg.listenPort = 4791;
 * try (RdmaProvider ep = RdmaProvider.create("cuobj", "server", cfg)) {
 *     byte[] data   = ep.handlePut(key, putToken, 0, 0);
 *     long written  = ep.handleGet(key, data, getToken, 0);
 * }
 * }</pre>
 *
 * <h2>Quick start — both roles (mock, same process)</h2>
 * <pre>{@code
 * ByteBuffer buf = ByteBuffer.allocateDirect(4096);
 * try (RdmaProvider ep = RdmaProvider.create("mock", "both", RdmaProviderConfig.defaults())) {
 *     ep.registerMemory(buf);
 *     byte[] token = ep.preparePutToken(key, buf, 0, buf.capacity());
 *     byte[] data  = ep.handlePut(key, token, 0, 0);   // server reads from SHM
 *     ep.processRdmaReply(200, token, buf.capacity(), (code, reply) -> { });
 *     ep.deregisterMemory(buf);
 * }
 * }</pre>
 */
public class RdmaProvider implements AutoCloseable {

    static {
        System.loadLibrary("s3rdmajni");
    }

    private long nativeHandle;

    /** Package-private: allows convenience subclasses (e.g. {@link CuObjProvider}). */
    RdmaProvider(long handle) {
        this.nativeHandle = handle;
    }

    // ── Package-private factory helper ─────────────────────────────────────────

    /**
     * Creates the native handle for the given provider/role/config.
     * Separated from the constructor so subclasses can call it via {@code super(...)}.
     */
    static long createHandle(String provider, String role, RdmaProviderConfig cfg)
            throws RdmaException {
        long h = nativeCreate(
                provider, role, cfg.debug,
                cfg.shmPath, cfg.listenAddr, cfg.listenPort,
                cfg.poolCount, cfg.bufSize);
        if (h == 0) {
            throw new RdmaException(
                "failed to create RdmaProvider (provider=" + provider + ", role=" + role + ")");
        }
        return h;
    }

    // ── Factory ────────────────────────────────────────────────────────────────

    /**
     * Create an RDMA provider.
     *
     * @param provider {@code "mock"}, {@code "cuobj"}, or {@code "auto"}
     *                 ({@code "auto"} tries cuobj first, falls back to mock)
     * @param role     {@code "client"}, {@code "server"}, or {@code "both"}
     * @param cfg      provider configuration; use {@link RdmaProviderConfig#defaults()}
     *                 for sensible defaults
     * @throws RdmaException if the provider is unavailable or fails to initialise
     */
    public static RdmaProvider create(String provider, String role, RdmaProviderConfig cfg)
            throws RdmaException {
        return new RdmaProvider(createHandle(provider, role, cfg));
    }

    // ── Info ───────────────────────────────────────────────────────────────────

    /** Human-readable provider name (e.g. {@code "MockRdmaProvider"}). */
    public String  name()            { return nativeName(nativeHandle); }
    /** Transport protocol string: {@code "mock"} or {@code "cuobj"}. */
    public String  protocol()        { return nativeProtocol(nativeHandle); }
    /** {@code true} if this provider supports client-side operations. */
    public boolean supportsClient()  { return nativeSupportsClient(nativeHandle); }
    /** {@code true} if this provider supports server-side operations. */
    public boolean supportsServer()  { return nativeSupportsServer(nativeHandle); }

    // ── Client methods ─────────────────────────────────────────────────────────

    /**
     * Returns {@code true} if the memory region starting at {@code ptr} for
     * {@code size} bytes is suitable for RDMA (e.g. pinned GPU / host memory).
     */
    public boolean isMemorySuitable(long ptr, long size) {
        return nativeIsMemorySuitable(nativeHandle, ptr, size);
    }

    /**
     * Pins the direct buffer {@code buf} with the RDMA subsystem.
     *
     * <p>{@code buf} <strong>must</strong> be a direct {@code ByteBuffer} created with
     * {@link ByteBuffer#allocateDirect(int)} (or {@link RdmaBuffer#buffer()}).
     * The buffer must remain reachable (not garbage-collected) until
     * {@link #deregisterMemory(ByteBuffer)} is called.</p>
     *
     * @param buf  a direct {@code ByteBuffer}; must not be a heap-backed array buffer
     * @throws RdmaException         if registration fails
     * @throws IllegalArgumentException if {@code buf} is not a direct buffer
     */
    public void registerMemory(ByteBuffer buf) throws RdmaException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException(
                "buf must be a direct ByteBuffer (use ByteBuffer.allocateDirect)");
        }
        nativeRegisterMemory(nativeHandle, buf);
    }

    /**
     * Unpins a previously registered direct buffer.
     *
     * @param buf  the same direct {@code ByteBuffer} passed to {@link #registerMemory(ByteBuffer)}
     * @throws RdmaException         if deregistration fails
     * @throws IllegalArgumentException if {@code buf} is not a direct buffer
     */
    public void deregisterMemory(ByteBuffer buf) throws RdmaException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException(
                "buf must be a direct ByteBuffer (use ByteBuffer.allocateDirect)");
        }
        nativeDeregisterMemory(nativeHandle, buf);
    }

    /**
     * Generates the {@code x-amz-rdma-token} header value for an S3 PUT.
     *
     * @param s3Key  the S3 object key
     * @param buf    direct {@code ByteBuffer} containing the data to upload;
     *               must have been passed to {@link #registerMemory(ByteBuffer)}
     * @param offset byte offset within {@code buf} for this sub-request
     * @param size   number of bytes to transfer
     * @return opaque token bytes to attach as an HTTP header
     * @throws RdmaException if token generation fails
     * @throws IllegalArgumentException if {@code buf} is not a direct buffer
     */
    public byte[] preparePutToken(byte[] s3Key, ByteBuffer buf, long offset, long size)
            throws RdmaException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException(
                "buf must be a direct ByteBuffer (use ByteBuffer.allocateDirect)");
        }
        return nativePreparePutToken(nativeHandle, s3Key, buf, offset, size);
    }

    /**
     * Generates the {@code x-amz-rdma-token} header value for an S3 GET.
     *
     * @param s3Key  the S3 object key
     * @param buf    direct {@code ByteBuffer} that will receive the downloaded data;
     *               must have been passed to {@link #registerMemory(ByteBuffer)}
     * @param offset byte offset within {@code buf} for this sub-request
     * @param size   number of bytes to transfer
     * @return opaque token bytes to attach as an HTTP header
     * @throws RdmaException if token generation fails
     * @throws IllegalArgumentException if {@code buf} is not a direct buffer
     */
    public byte[] prepareGetToken(byte[] s3Key, ByteBuffer buf, long offset, long size)
            throws RdmaException {
        if (!buf.isDirect()) {
            throw new IllegalArgumentException(
                "buf must be a direct ByteBuffer (use ByteBuffer.allocateDirect)");
        }
        return nativePrepareGetToken(nativeHandle, s3Key, buf, offset, size);
    }

    /**
     * Processes the {@code x-amz-rdma-reply} status from the S3 server and
     * invokes {@code callback} with the result.
     *
     * @param rdmaStatus   numeric status (200/204/206 = success, 501 = fallback,
     *                     0 = header absent)
     * @param requestToken the token originally sent with the PUT/GET request
     * @param transferred  bytes transferred as reported by {@code x-amz-rdma-bytes}
     *                     (pass 0 if the header was absent)
     * @param callback     invoked with {@code (errorCode, replyToken)} on completion
     * @throws RdmaException if reply processing fails
     */
    public void processRdmaReply(int rdmaStatus, byte[] requestToken, long transferred,
                                 RdmaCompletionCallback callback) throws RdmaException {
        nativeProcessRdmaReply(nativeHandle, rdmaStatus, requestToken, transferred, callback);
    }

    // ── Server methods ─────────────────────────────────────────────────────────

    /** Capacity in bytes of the server's per-slot transfer buffer. */
    public long bufSize() { return nativeBufSize(nativeHandle); }

    /**
     * Handles an RDMA PUT from a client: reads {@code size} bytes starting at
     * {@code offset} from the client buffer described by {@code token} and
     * returns the transferred bytes.
     *
     * @param s3Key  the S3 object key
     * @param token  the RDMA descriptor token from {@code x-amz-rdma-token}
     * @param offset byte offset into the object
     * @param size   bytes to transfer (0 = use full buffer capacity from token)
     * @return bytes received from the client buffer
     * @throws RdmaException on transfer failure
     */
    public byte[] handlePut(byte[] s3Key, byte[] token, long offset, long size)
            throws RdmaException {
        return nativeHandlePut(nativeHandle, s3Key, token, offset, size);
    }

    /**
     * Handles an RDMA GET to a client: writes {@code data} into the client
     * buffer described by {@code token} and returns the number of bytes written.
     *
     * @param s3Key  the S3 object key
     * @param data   the object payload to send to the client
     * @param token  the RDMA descriptor token from {@code x-amz-rdma-token}
     * @param offset byte offset into the client buffer
     * @return number of bytes written to the client buffer
     * @throws RdmaException on transfer failure
     */
    public long handleGet(byte[] s3Key, byte[] data, byte[] token, long offset)
            throws RdmaException {
        return nativeHandleGet(nativeHandle, s3Key, data, token, offset);
    }

    // ── Lifecycle ──────────────────────────────────────────────────────────────

    @Override
    public void close() {
        if (nativeHandle != 0) {
            nativeDestroy(nativeHandle);
            nativeHandle = 0;
        }
    }

    @Override
    public String toString() {
        return "RdmaProvider{name=" + name() + ", protocol=" + protocol() + "}";
    }

    // ── Native methods ─────────────────────────────────────────────────────────

    private static native long    nativeCreate(String provider, String role, boolean debug,
                                               String shmPath, String listenAddr, int listenPort,
                                               int poolCount, long bufSize);
    private native void           nativeDestroy(long handle);
    private native String         nativeName(long handle);
    private native String         nativeProtocol(long handle);
    private native boolean        nativeSupportsClient(long handle);
    private native boolean        nativeSupportsServer(long handle);
    private native boolean        nativeIsMemorySuitable(long handle, long ptr, long size);
    private native void           nativeRegisterMemory(long handle, ByteBuffer buf);
    private native void           nativeDeregisterMemory(long handle, ByteBuffer buf);
    private native byte[]         nativePreparePutToken(long handle, byte[] s3Key, ByteBuffer buf,
                                                        long offset, long size);
    private native byte[]         nativePrepareGetToken(long handle, byte[] s3Key, ByteBuffer buf,
                                                        long offset, long size);
    private native void           nativeProcessRdmaReply(long handle, int rdmaStatus,
                                                         byte[] requestToken, long transferred,
                                                         RdmaCompletionCallback callback);
    private native long           nativeBufSize(long handle);
    private native byte[]         nativeHandlePut(long handle, byte[] s3Key, byte[] token,
                                                  long offset, long size);
    private native long           nativeHandleGet(long handle, byte[] s3Key, byte[] data,
                                                  byte[] token, long offset);
}
