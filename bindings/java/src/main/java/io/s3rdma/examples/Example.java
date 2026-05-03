package io.s3rdma.examples;

import io.s3rdma.CuObjProvider;
import io.s3rdma.RdmaProvider;
import io.s3rdma.RdmaProviderConfig;
import io.s3rdma.RdmaException;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * s3-rdma Java binding example.
 *
 * <p>Demonstrates the unified {@link RdmaProvider} API with both mock and
 * cuObj providers for client and server roles — no NVIDIA hardware required
 * for the mock provider.</p>
 *
 * <p>Run after building the JNI library:</p>
 * <pre>
 *   cargo build --manifest-path bindings/java/Cargo.toml
 *   mvn compile -f bindings/java/pom.xml
 *   java -Djava.library.path=bindings/java/target/debug \
 *        -cp bindings/java/target/classes \
 *        io.s3rdma.examples.Example
 * </pre>
 */
public class Example {

    static final byte[] S3_KEY = "my-bucket/my-object.bin".getBytes(StandardCharsets.UTF_8);

    public static void main(String[] args) throws Exception {
        System.out.println("=== s3-rdma Java example ===\n");

        demoMockBothRoles();
        demoMockClientRole();
        demoMockServerRole();
        demoCuObjAutoFallback();
        demoCuObjClientExpectFailure();

        System.out.println("Done.");
    }

    /** Mock provider — both client and server roles in the same process. */
    static void demoMockBothRoles() throws RdmaException {
        System.out.println("=== Mock endpoint — both roles ===\n");

        try (RdmaProvider ep = RdmaProvider.create("mock", "both", RdmaProviderConfig.defaults())) {
            System.out.println("  endpoint : " + ep);
            System.out.println("  client?  : " + ep.supportsClient());
            System.out.println("  server?  : " + ep.supportsServer());

            byte[] data = new byte[64 * 1024];
            Arrays.fill(data, (byte) 0x42);

            ep.registerMemory(data);

            // Client: generate a PUT token (data is copied to SHM)
            byte[] putToken = ep.preparePutToken(S3_KEY, data, 0, data.length);
            System.out.println("\n  put_token : " + abbrev(putToken));

            // Server: read back via the PUT token
            byte[] received = ep.handlePut(S3_KEY, putToken, 0, data.length);
            assert Arrays.equals(received, data) : "PUT round-trip mismatch";
            System.out.println("  handle_put: got " + received.length + " bytes ✓");

            // Client: generate a GET token (server will write SHM)
            byte[] getToken = ep.prepareGetToken(S3_KEY, data, 0, data.length);
            System.out.println("\n  get_token : " + abbrev(getToken));

            // Server: write data via the GET token
            byte[] payload = new byte[data.length];
            Arrays.fill(payload, (byte) 0xde);
            long written = ep.handleGet(S3_KEY, payload, getToken, 0);
            System.out.println("  handle_get: wrote " + written + " bytes ✓");

            // Client: process server reply
            ep.processRdmaReply(200, getToken, (long) data.length,
                    (code, reply) -> System.out.println("  callback  : error_code=" + code));

            ep.deregisterMemory(data);
        }
        System.out.println();
    }

    /** Mock provider — client role only. */
    static void demoMockClientRole() throws RdmaException {
        System.out.println("=== Mock endpoint — client role only ===\n");

        try (RdmaProvider ep = RdmaProvider.create("mock", "client", RdmaProviderConfig.defaults())) {
            System.out.println("  endpoint : " + ep);

            byte[] data = new byte[1024];
            Arrays.fill(data, (byte) 0x11);

            ep.registerMemory(data);
            byte[] token = ep.preparePutToken(S3_KEY, data, 0, data.length);
            System.out.println("  token    : " + abbrev(token));

            ep.processRdmaReply(200, token, data.length, (code, r) -> {});
            ep.deregisterMemory(data);
        }
        System.out.println();
    }

    /** Mock provider — server role only, token produced by a separate client endpoint. */
    static void demoMockServerRole() throws RdmaException {
        System.out.println("=== Mock endpoint — server role only ===\n");

        try (RdmaProvider client = RdmaProvider.create("mock", "client", RdmaProviderConfig.defaults());
             RdmaProvider server = RdmaProvider.create("mock", "server", RdmaProviderConfig.defaults())) {

            System.out.println("  server endpoint : " + server);

            byte[] data = new byte[1024];
            Arrays.fill(data, (byte) 0x22);

            client.registerMemory(data);
            byte[] putToken = client.preparePutToken(S3_KEY, data, 0, data.length);

            byte[] received = server.handlePut(S3_KEY, putToken, 0, data.length);
            assert Arrays.equals(received, data) : "Server PUT data mismatch";
            System.out.println("  handle_put: received " + received.length + " bytes ✓");

            byte[] getToken = client.prepareGetToken(S3_KEY, data, 0, data.length);
            byte[] payload = new byte[data.length];
            Arrays.fill(payload, (byte) 0xff);
            long written = server.handleGet(S3_KEY, payload, getToken, 0);
            System.out.println("  handle_get: wrote " + written + " bytes ✓");

            client.deregisterMemory(data);
        }
        System.out.println();
    }

    /** Auto-selection: tries cuObj first, falls back to mock transparently. */
    static void demoCuObjAutoFallback() throws RdmaException {
        System.out.println("=== Auto endpoint (cuobj → mock fallback) ===\n");

        try (RdmaProvider ep = RdmaProvider.create("auto", "both", RdmaProviderConfig.defaults())) {
            System.out.println("  endpoint : " + ep);
            System.out.println("  protocol : " + ep.protocol());
        }
        System.out.println();
    }

    /** CuObjProvider convenience class: raises RdmaException when the plugin is absent. */
    static void demoCuObjClientExpectFailure() {
        System.out.println("=== CuObjProvider (client) — expecting RdmaException without plugin ===\n");

        try {
            try (CuObjProvider ep = new CuObjProvider("client", RdmaProviderConfig.defaults())) {
                System.out.println("  cuObj available: " + ep);
            }
        } catch (RdmaException e) {
            System.out.println("  cuObj unavailable (expected): " + e.getMessage());
        }
        System.out.println();
    }

    private static String abbrev(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);
        return s.length() > 60 ? s.substring(0, 60) + "…" : s;
    }
}
