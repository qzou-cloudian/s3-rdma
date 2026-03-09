package io.s3rdma;

/** Callback invoked when an RDMA operation completes. */
@FunctionalInterface
public interface RdmaCompletionCallback {
    /**
     * @param errorCode  0 on success, non-zero on failure
     * @param replyToken the reply token bytes from the S3 response header
     *                   (may be empty on error)
     */
    void onComplete(int errorCode, byte[] replyToken);
}
