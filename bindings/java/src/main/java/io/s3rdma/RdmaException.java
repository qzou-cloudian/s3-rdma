package io.s3rdma;

/** Thrown when an RDMA operation fails. */
public class RdmaException extends Exception {
    public RdmaException(String message) {
        super(message);
    }

    public RdmaException(String message, Throwable cause) {
        super(message, cause);
    }
}
