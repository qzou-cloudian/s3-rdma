package io.s3rdma;

/**
 * Mock RDMA provider — no NVIDIA hardware required.
 *
 * <p>Delegates to the Rust {@code MockRdmaProvider} via JNI.  The native
 * library {@code libs3_rdma_jni.so} must be on {@code java.library.path}.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (MockRdmaProvider p = new MockRdmaProvider(true)) {
 *     byte[] token = p.preparePutToken(key, buf, 0, buf.length);
 *     p.processReplyToken(reply, (code, r) -> System.out.println(code));
 * }
 * }</pre>
 */
public final class MockRdmaProvider implements AutoCloseable {

    static {
        System.loadLibrary("s3_rdma_jni");
    }

    private final long nativeHandle;

    /** Create a mock provider.  Set {@code debug=true} for trace output. */
    public MockRdmaProvider(boolean debug) {
        this.nativeHandle = nativeCreate(debug);
    }

    


    public boolean isMemorySuitable(long ptr, long size) {
        return nativeIsMemorySuitable(nativeHandle, ptr, size);
    }

    public void registerMemory(byte[] buf) throws RdmaException {
        nativeRegisterMemory(nativeHandle, buf);
    }

    public void deregisterMemory(byte[] buf) throws RdmaException {
        nativeDeregisterMemory(nativeHandle, buf);
    }

    public byte[] preparePutToken(byte[] s3Key, byte[] buffer, long offset, long size)
            throws RdmaException {
        return nativePreparePutToken(nativeHandle, s3Key, buffer, offset, size);
    }

    public byte[] prepareGetToken(byte[] s3Key, byte[] buffer, long offset, long size)
            throws RdmaException {
        return nativePrepareGetToken(nativeHandle, s3Key, buffer, offset, size);
    }

    public void processReplyToken(byte[] replyToken, RdmaCompletionCallback callback)
            throws RdmaException {
        nativeProcessReplyToken(nativeHandle, replyToken, callback);
    }

    public void close() {
        nativeDestroy(nativeHandle);
    }

    // ── Native methods ─────────────────────────────────────────────────────

    private native long    nativeCreate(boolean debug);
    private native void    nativeDestroy(long handle);
    private native String  nativeName(long handle);
    private native String  nativeVersion(long handle);
    private native boolean nativeIsMemorySuitable(long handle, long ptr, long size);
    private native void    nativeRegisterMemory(long handle, byte[] buf);
    private native void    nativeDeregisterMemory(long handle, byte[] buf);
    private native byte[]  nativePreparePutToken(long handle, byte[] s3Key, byte[] buf, long offset, long size);
    private native byte[]  nativePrepareGetToken(long handle, byte[] s3Key, byte[] buf, long offset, long size);
    private native void    nativeProcessReplyToken(long handle, byte[] replyToken, RdmaCompletionCallback callback);
}
