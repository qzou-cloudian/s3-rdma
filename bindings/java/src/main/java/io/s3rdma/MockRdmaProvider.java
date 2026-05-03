package io.s3rdma;

/**
 * Convenience subclass of {@link RdmaProvider} pre-configured for the mock
 * shared-memory provider — no NVIDIA hardware required.
 *
 * <p>Equivalent to {@code RdmaProvider.create("mock", role, RdmaProviderConfig.defaults())}
 * with a simpler constructor.</p>
 *
 * <pre>{@code
 * byte[] content = "hello RDMA".getBytes();
 * java.nio.ByteBuffer buf = java.nio.ByteBuffer.allocateDirect(content.length);
 * buf.put(content);
 *
 * try (MockRdmaProvider ep = new MockRdmaProvider()) {
 *     ep.registerMemory(buf);
 *     byte[] token = ep.preparePutToken(key, buf, 0, buf.capacity());
 *     byte[] received = ep.handlePut(key, token, 0, buf.capacity());
 *     ep.deregisterMemory(buf);
 * }
 * }</pre>
 */
public final class MockRdmaProvider extends RdmaProvider {

    /**
     * Creates a mock provider supporting both client and server operations.
     *
     * @throws RdmaException if initialisation fails (should not occur for mock)
     */
    public MockRdmaProvider() throws RdmaException {
        super(createHandle("mock", "both", RdmaProviderConfig.defaults()));
    }

    /**
     * Creates a mock provider for the specified role.
     *
     * @param role {@code "client"}, {@code "server"}, or {@code "both"}
     * @throws RdmaException if initialisation fails (should not occur for mock)
     */
    public MockRdmaProvider(String role) throws RdmaException {
        super(createHandle("mock", role, RdmaProviderConfig.defaults()));
    }
}
