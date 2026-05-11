package io.s3rdma;

/**
 * Configuration for constructing an {@link RdmaProvider}.
 *
 * <p>Fields irrelevant to the chosen provider are silently ignored:</p>
 *
 * <table border="1">
 *   <tr><th>Field</th><th>mock</th><th>cuobj</th></tr>
 *   <tr><td>{@link #shmPath}</td><td>&#x2713;</td><td>&#x2013;</td></tr>
 *   <tr><td>{@link #listenAddr}</td><td>&#x2013;</td><td>&#x2713;</td></tr>
 *   <tr><td>{@link #listenPort}</td><td>&#x2013;</td><td>&#x2713;</td></tr>
 *   <tr><td>{@link #bufSize}</td><td>&#x2013;</td><td>&#x2713;</td></tr>
 * </table>
 *
 * <p>Obtain defaults via {@link #defaults()}:</p>
 * <pre>{@code
 * RdmaProviderConfig cfg = RdmaProviderConfig.defaults();
 * cfg.listenAddr = "192.168.1.1";
 * cfg.listenPort = 4791;
 * RdmaProvider ep = RdmaProvider.create("cuobj", "server", cfg);
 * }</pre>
 */
public final class RdmaProviderConfig {

    /** Emit debug trace lines to stderr. */
    public boolean debug = false;

    /** Directory for POSIX shared-memory files (mock provider). Default: {@code "/dev/shm"}. */
    public String shmPath = "/dev/shm";

    /** RDMA listen address for the cuObj server provider (e.g. {@code "192.168.1.1"}). */
    public String listenAddr = "";

    /** RDMA listen port for the cuObj server provider. */
    public int listenPort = 0;

    /**
     * @deprecated Pool management has moved to hsg-rdma; this field is ignored.
     *             Will be removed in a future release.
     */
    @Deprecated
    public int poolCount = 0;

    /** Size in bytes of each buffer pool slot (cuObj server). Default: 128 MiB. */
    public long bufSize = 128L * 1024 * 1024;

    /** Returns a default configuration suitable for the mock provider. */
    public static RdmaProviderConfig defaults() {
        return new RdmaProviderConfig();
    }
}
