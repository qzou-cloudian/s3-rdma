package io.s3rdma;

import java.nio.ByteBuffer;

/**
 * A direct-memory buffer suitable for RDMA operations.
 *
 * <p>Wraps {@link ByteBuffer#allocateDirect(int)} and provides helper methods
 * to convert content to/from {@code byte[]} without exposing the {@code ByteBuffer}
 * positioning state.</p>
 *
 * <pre>{@code
 * byte[] content = "hello RDMA".getBytes();
 * try (RdmaBuffer buf = new RdmaBuffer(content.length)) {
 *     buf.put(content, 0, content.length);
 *     ep.registerMemory(buf.buffer());
 *     byte[] token = ep.preparePutToken(key, buf.buffer(), 0, content.length);
 *     // ...
 *     ep.deregisterMemory(buf.buffer());
 * }
 * }</pre>
 */
public final class RdmaBuffer implements AutoCloseable {

    private final ByteBuffer buf;

    /**
     * Allocates a direct-memory buffer of {@code capacity} bytes.
     *
     * @param capacity buffer size in bytes
     */
    public RdmaBuffer(int capacity) {
        this.buf = ByteBuffer.allocateDirect(capacity);
    }

    /** Returns the underlying direct {@link ByteBuffer}. */
    public ByteBuffer buffer() { return buf; }

    /** Returns the capacity of this buffer in bytes. */
    public int capacity() { return buf.capacity(); }

    /**
     * Copies {@code src[0..len]} into this buffer starting at absolute position
     * {@code offset}.
     *
     * @param src    source byte array
     * @param offset absolute position in this buffer to write to
     * @param len    number of bytes to copy from {@code src}
     */
    public void put(byte[] src, int offset, int len) {
        ByteBuffer slice = buf.duplicate();
        slice.position(offset);
        slice.put(src, 0, len);
    }

    /**
     * Copies {@code len} bytes from this buffer starting at absolute position
     * {@code offset} into a newly allocated {@code byte[]}.
     *
     * @param offset absolute position in this buffer to read from
     * @param len    number of bytes to copy
     * @return a new {@code byte[]} containing the copied data
     */
    public byte[] get(int offset, int len) {
        byte[] dst = new byte[len];
        ByteBuffer slice = buf.duplicate();
        slice.position(offset);
        slice.get(dst);
        return dst;
    }

    /**
     * No-op — the JVM GC will reclaim the off-heap direct memory when this
     * object is garbage-collected.
     */
    @Override
    public void close() { /* GC handles direct ByteBuffer cleanup */ }
}
