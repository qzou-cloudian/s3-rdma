package io.s3rdma.examples;

import io.s3rdma.MockRdmaProvider;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * s3-rdma Java binding example.
 *
 * <p>Demonstrates MockRdmaProvider: register memory, generate PUT/GET tokens,
 * and process a server reply token — all without NVIDIA hardware.</p>
 *
 * <p>Run after building the JNI library:</p>
 * <pre>
 *   cargo build --manifest-path s3-rdma-jni/Cargo.toml
 *   mvn compile -f java/pom.xml
 *   java -Djava.library.path=target/debug \
 *        -cp java/target/classes \
 *        io.s3rdma.examples.Example
 * </pre>
 */
public class Example {

    public static void main(String[] args) throws Exception {
        System.out.println("=== s3-rdma Java example ===\n");

        try (MockRdmaProvider provider = new MockRdmaProvider(true)) {
            System.out.println("Provider : " + provider.name());
            System.out.println("Version  : " + provider.version());

            byte[] data  = new byte[1024 * 1024]; // 1 MiB
            Arrays.fill(data, (byte) 0x42);
            byte[] s3Key = "my-bucket/my-object.bin".getBytes(StandardCharsets.UTF_8);

            System.out.println("\n[isMemorySuitable]");
            System.out.println("  suitable: " + provider.isMemorySuitable(0L, data.length));

            System.out.println("\n[registerMemory]");
            provider.registerMemory(data);

            System.out.println("\n[preparePutToken]  (S3 PUT / upload)");
            byte[] putToken = provider.preparePutToken(s3Key, data, 0, data.length);
            System.out.println("  token: " + new String(putToken, StandardCharsets.UTF_8));

            System.out.println("\n[prepareGetToken]  (S3 GET / download)");
            byte[] getToken = provider.prepareGetToken(s3Key, data, 0, data.length);
            System.out.println("  token: " + new String(getToken, StandardCharsets.UTF_8));

            System.out.println("\n[processReplyToken]");
            provider.processReplyToken(
                "mock-server-reply-ok".getBytes(StandardCharsets.UTF_8),
                (errorCode, reply) -> System.out.println(
                    "  callback: error_code=" + errorCode +
                    ", reply=" + new String(reply, StandardCharsets.UTF_8))
            );

            System.out.println("\n[deregisterMemory]");
            provider.deregisterMemory(data);
        }

        System.out.println("\nDone.");
    }
}
