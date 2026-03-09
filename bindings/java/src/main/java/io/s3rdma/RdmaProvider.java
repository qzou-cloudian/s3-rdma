package io.s3rdma;

/**
 * GPU-Direct RDMA provider for S3 object storage.
 *
 * <p>Concrete implementations hold a native Rust handle and delegate all
 * operations to the Rust {@code s3-rdma} library.</p>
 *
 * <h2>Quick start</h2>
 * <pre>{@code
 * try (MockRdmaProvider provider = new MockRdmaProvider(true)) {
 *     byte[] token = provider.preparePutToken(key, buffer, 0, buffer.length);
 *     // Attach token as "x-amz-rdma-token" header in S3 PUT
 *     provider.processReplyToken(replyBytes, (errorCode, reply) -> { ... });
 * }
 * }</pre>
 */
public abstract class RdmaProvider implements AutoCloseable {

    // ── Provider API ───────────────────────────────────────────────────────

    public abstract String name();
    public abstract String version();

    /**
     * Returns {@code true} if the memory region starting at {@code ptr} with
     * the given {@code size} is suitable for RDMA (e.g. pinned GPU memory).
     */
    public abstract boolean isMemorySuitable(long ptr, long size);

    public abstract void registerMemory(byte[] buf) throws RdmaException;
    public abstract void deregisterMemory(byte[] buf) throws RdmaException;

    /**
     * Generates the {@code x-amz-rdma-token} header value for an S3 PUT.
     *
     * @param s3Key  the S3 object key
     * @param buffer byte array that will be transferred
     * @param offset offset into {@code buffer}
     * @param size   number of bytes to transfer
     * @return opaque token bytes to attach as an HTTP header
     */
    public abstract byte[] preparePutToken(byte[] s3Key, byte[] buffer, long offset, long size)
        throws RdmaException;

    /** Generates the {@code x-amz-rdma-token} header value for an S3 GET. */
    public abstract byte[] prepareGetToken(byte[] s3Key, byte[] buffer, long offset, long size)
        throws RdmaException;

    /**
     * Processes the {@code x-amz-rdma-reply} response header after an S3
     * PUT/GET completes.  Invokes {@code callback} with the result.
     */
    public abstract void processReplyToken(byte[] replyToken, RdmaCompletionCallback callback)
        throws RdmaException;

    @Override
    public abstract void close();
}

