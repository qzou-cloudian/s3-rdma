package io.s3rdma;

/**
 * Server-side RDMA provider for the S3 gateway.
 *
 * <p>Implementations handle incoming RDMA PUT and GET requests from S3 clients.
 * The server maintains an internal buffer pool of pre-registered DMA slots;
 * each call acquires a slot, performs the RDMA transfer, and releases it.</p>
 *
 * <h2>PUT flow</h2>
 * <pre>
 *   S3 client sends x-amz-rdma-token header with PUT request.
 *   Gateway calls handlePut(key, token, offset, size).
 *   Server performs RDMA_READ from the client's registered buffer.
 *   Returns the transferred bytes.
 * </pre>
 *
 * <h2>GET flow</h2>
 * <pre>
 *   S3 client sends x-amz-rdma-token header with GET request.
 *   Gateway calls handleGet(key, data, token, offset).
 *   Server copies data into a pool slot and performs RDMA_WRITE to client.
 *   Returns the number of bytes written.
 * </pre>
 */
public abstract class RdmaServerProvider implements AutoCloseable {

    /** Human-readable provider name (e.g. {@code "MockRdmaServerProvider"}). */
    public abstract String name();

    /** Capacity of each buffer in the internal pool, in bytes. */
    public abstract long bufSize();

    /**
     * Handle an incoming RDMA PUT: reads {@code size} bytes starting at
     * {@code offset} from the client buffer described by {@code token}.
     *
     * @param s3Key  the S3 object key
     * @param token  the RDMA descriptor token from {@code x-amz-rdma-token}
     * @param offset byte offset into the object
     * @param size   number of bytes to transfer (0 = use pool buffer capacity)
     * @return the bytes received from the client's buffer
     * @throws RdmaException on transfer failure
     */
    public abstract byte[] handlePut(byte[] s3Key, byte[] token, long offset, long size)
            throws RdmaException;

    /**
     * Handle an incoming RDMA GET: writes {@code data} into the client buffer
     * described by {@code token}.
     *
     * @param s3Key  the S3 object key
     * @param data   the object payload to write to the client
     * @param token  the RDMA descriptor token from {@code x-amz-rdma-token}
     * @param offset byte offset into the object
     * @return the number of bytes written to the client buffer
     * @throws RdmaException on transfer failure
     */
    public abstract long handleGet(byte[] s3Key, byte[] data, byte[] token, long offset)
            throws RdmaException;

    @Override
    public abstract void close();
}
