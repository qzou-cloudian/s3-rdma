package io.s3rdma;

/**
 * CuObj RDMA endpoint — requires NVIDIA hardware and the cuObject SDK plugin.
 *
 * <p>Convenience subclass of {@link RdmaProvider} that pre-selects
 * {@code provider="cuobj"} and supports client, server, or both roles.
 * The native library ({@code libs3rdmacuobjclient.so} and/or
 * {@code libs3rdmacuobjserver.so}) is loaded at runtime via
 * {@code dlopen}; no compile-time NVIDIA SDK dependency is required.</p>
 *
 * <p>Raises {@link RdmaException} if the required cuObj plugin cannot be
 * found at runtime.  Use {@link RdmaProvider#create(String, String, RdmaProviderConfig)
 * RdmaProvider.create("auto", role, cfg)} for a transparent fallback to the mock
 * provider when cuObj is unavailable.</p>
 *
 * <h2>Quick start — client role</h2>
 * <pre>{@code
 * try (CuObjProvider ep = new CuObjProvider("client", RdmaProviderConfig.defaults())) {
 *     ep.registerMemory(buf);
 *     byte[] token = ep.preparePutToken(key, buf, 0, buf.length);
 *     // Attach token as "x-amz-rdma-token" header on the S3 PUT request.
 *     ep.processRdmaReply(200, token, buf.length, (code, reply) -> { ... });
 *     ep.deregisterMemory(buf);
 * }
 * }</pre>
 *
 * <h2>Quick start — server role</h2>
 * <pre>{@code
 * RdmaProviderConfig cfg = RdmaProviderConfig.defaults();
 * cfg.listenAddr = "192.168.1.1";
 * cfg.listenPort = 4791;
 * try (CuObjProvider ep = new CuObjProvider("server", cfg)) {
 *     byte[] data  = ep.handlePut(key, putToken, 0, 0);
 *     long written = ep.handleGet(key, data, getToken, 0);
 * }
 * }</pre>
 *
 * <h2>Quick start — both roles</h2>
 * <pre>{@code
 * try (CuObjProvider ep = new CuObjProvider("both", cfg)) {
 *     ep.registerMemory(buf);
 *     byte[] token = ep.preparePutToken(key, buf, 0, buf.length);
 *     byte[] data  = ep.handlePut(key, token, 0, 0);
 *     ep.processRdmaReply(200, token, buf.length, (code, reply) -> { });
 *     ep.deregisterMemory(buf);
 * }
 * }</pre>
 */
public final class CuObjProvider extends RdmaProvider {

    /**
     * Create a cuObj RDMA endpoint.
     *
     * @param role {@code "client"}, {@code "server"}, or {@code "both"}
     * @param cfg  provider configuration; for the server role set
     *             {@link RdmaProviderConfig#listenAddr} and
     *             {@link RdmaProviderConfig#listenPort}
     * @throws RdmaException if the cuObj plugin is unavailable or fails to initialise
     */
    public CuObjProvider(String role, RdmaProviderConfig cfg) throws RdmaException {
        super(RdmaProvider.createHandle("cuobj", role, cfg));
    }
}
