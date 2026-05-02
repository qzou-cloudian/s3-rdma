//! JNI bindings for `s3-rdma` — exposes `MockRdmaEndpoint` to Java.
//!
//! The Java classes `io.s3rdma.MockRdmaProvider` and
//! `io.s3rdma.MockRdmaServerProvider` both hold a `nativeHandle` (long)
//! that points to a boxed `MockRdmaEndpoint`.  The unified endpoint supports
//! both client and server roles, so both Java classes share the same Rust type.

use jni::objects::{JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jbyteArray, jlong, JNI_TRUE};
use jni::JNIEnv;

use s3_rdma::MockRdmaEndpoint;
use s3_rdma::endpoint::RdmaEndpoint;
use s3_rdma::provider::{RdmaCompletionCallback, RdmaCompletionResult};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// # Safety
/// `handle` must be a non-null `jlong` obtained from `nativeCreate` and not yet
/// passed to `nativeDestroy`.  The caller must not call `nativeDestroy` while
/// any reference produced by this function is still live.
unsafe fn endpoint_ref(handle: jlong) -> &'static MockRdmaEndpoint {
    unsafe { &*(handle as *const MockRdmaEndpoint) }
}

fn throw(env: &mut JNIEnv<'_>, msg: &str) {
    let _ = env.throw_new("io/s3rdma/RdmaException", msg);
}

// ══════════════════════════════════════════════════════════════════════════════
// MockRdmaProvider JNI  (client role — backed by MockRdmaEndpoint)
// ══════════════════════════════════════════════════════════════════════════════

/// `MockRdmaProvider.nativeCreate(boolean debug) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeCreate(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    debug: jboolean,
) -> jlong {
    let ep = MockRdmaEndpoint::with_debug(debug == JNI_TRUE);
    Box::into_raw(Box::new(ep)) as jlong
}

/// `MockRdmaProvider.nativeDestroy(long handle)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeDestroy(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
) {
    if handle != 0 {
        let _ = unsafe { Box::from_raw(handle as *mut MockRdmaEndpoint) };
    }
}

/// `MockRdmaProvider.nativeName(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeName<'a>(
    env: JNIEnv<'a>,
    _obj: JObject<'a>,
    handle: jlong,
) -> JString<'a> {
    let name = unsafe { endpoint_ref(handle) }.name();
    env.new_string(name).unwrap_or_else(|_| JString::default())
}

/// `MockRdmaProvider.nativeVersion(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeVersion<'a>(
    env: JNIEnv<'a>,
    _obj: JObject<'a>,
    _handle: jlong,
) -> JString<'a> {
    env.new_string("1").unwrap_or_else(|_| JString::default())
}

/// `MockRdmaProvider.nativeIsMemorySuitable(long handle, long ptr, long size) -> boolean`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeIsMemorySuitable(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    ptr: jlong,
    size: jlong,
) -> jboolean {
    let suitable = unsafe { endpoint_ref(handle) }
        .is_memory_suitable(ptr as *const u8, size as usize);
    suitable as jboolean
}

/// `MockRdmaProvider.nativeRegisterMemory(long handle, byte[] buf) -> void`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeRegisterMemory(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    buf: JByteArray<'_>,
) {
    let len = env.get_array_length(&buf).unwrap_or(0) as usize;
    let base_ptr = buf.as_raw() as *mut u8;
    if let Err(e) = unsafe { endpoint_ref(handle) }.register_memory(base_ptr, len) {
        throw(&mut env, &e.to_string());
    }
}

/// `MockRdmaProvider.nativeDeregisterMemory(long handle, byte[] buf) -> void`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeDeregisterMemory(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    buf: JByteArray<'_>,
) {
    let base_ptr = buf.as_raw() as *mut u8;
    if let Err(e) = unsafe { endpoint_ref(handle) }.deregister_memory(base_ptr) {
        throw(&mut env, &e.to_string());
    }
}

/// `MockRdmaProvider.nativePreparePutToken(long, byte[] s3Key, byte[] buf, long offset, long size) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativePreparePutToken(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    s3_key: JByteArray<'_>,
    buf: JByteArray<'_>,
    offset: jlong,
    size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let buf_ptr = buf.as_raw() as *const u8;
    match unsafe { endpoint_ref(handle) }.prepare_put_token(
        &key_bytes, buf_ptr, size as usize, offset as usize,
    ) {
        Ok(token) => env.byte_array_from_slice(&token)
            .map(|a| a.into_raw())
            .unwrap_or(std::ptr::null_mut()),
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

/// `MockRdmaProvider.nativePrepareGetToken(long, byte[] s3Key, byte[] buf, long offset, long size) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativePrepareGetToken(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    s3_key: JByteArray<'_>,
    buf: JByteArray<'_>,
    offset: jlong,
    size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let buf_ptr = buf.as_raw() as *mut u8;
    match unsafe { endpoint_ref(handle) }.prepare_get_token(
        &key_bytes, buf_ptr, size as usize, offset as usize,
    ) {
        Ok(token) => env.byte_array_from_slice(&token)
            .map(|a| a.into_raw())
            .unwrap_or(std::ptr::null_mut()),
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

/// `MockRdmaProvider.nativeProcessReplyToken(long, byte[] replyToken, RdmaCompletionCallback) -> void`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeProcessReplyToken(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    reply_token: JByteArray<'_>,
    callback: JObject<'_>,
) {
    let reply_bytes = match env.convert_byte_array(&reply_token) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return; }
    };

    let jvm = env.get_java_vm().expect("get_java_vm");
    let cb_global = match env.new_global_ref(&callback) {
        Ok(r) => r,
        Err(e) => { throw(&mut env, &e.to_string()); return; }
    };

    let cb: RdmaCompletionCallback = Box::new(move |result: RdmaCompletionResult| {
        let mut env = jvm.attach_current_thread().expect("attach_current_thread");
        let reply_arr = env
            .byte_array_from_slice(&result.reply_token)
            .expect("byte_array_from_slice");
        let _ = env.call_method(
            cb_global.as_obj(),
            "onComplete",
            "(I[B)V",
            &[
                JValue::Int(result.error_code),
                JValue::Object(&reply_arr),
            ],
        );
    });

    // Use process_rdma_reply with status=200 and usize::MAX transferred so the
    // mock copies all available GET data (matching old process_reply_token behavior).
    if let Err(e) = unsafe { endpoint_ref(handle) }
        .process_rdma_reply(200, &reply_bytes, usize::MAX, cb)
    {
        throw(&mut env, &e.to_string());
    }
}

// ══════════════════════════════════════════════════════════════════════════════
// MockRdmaServerProvider JNI  (server role — backed by MockRdmaEndpoint)
// ══════════════════════════════════════════════════════════════════════════════

/// `MockRdmaServerProvider.nativeCreate(boolean debug, String shmPath, long bufSize) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeCreate(
    mut env: JNIEnv<'_>,
    _class: JClass<'_>,
    debug: jboolean,
    shm_path: JString<'_>,
    buf_size: jlong,
) -> jlong {
    let path: String = match env.get_string(&shm_path) {
        Ok(s) => s.into(),
        Err(e) => {
            let _ = env.throw_new("io/s3rdma/RdmaException", e.to_string());
            return 0;
        }
    };
    let ep = MockRdmaEndpoint::new(debug == JNI_TRUE, path, buf_size as usize);
    Box::into_raw(Box::new(ep)) as jlong
}

/// `MockRdmaServerProvider.nativeDestroy(long handle)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeDestroy(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
) {
    if handle != 0 {
        let _ = unsafe { Box::from_raw(handle as *mut MockRdmaEndpoint) };
    }
}

/// `MockRdmaServerProvider.nativeName(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeName<'a>(
    env: JNIEnv<'a>,
    _obj: JObject<'a>,
    handle: jlong,
) -> JString<'a> {
    let name = unsafe { endpoint_ref(handle) }.name();
    env.new_string(name).unwrap_or_else(|_| JString::default())
}

/// `MockRdmaServerProvider.nativeBufSize(long handle) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeBufSize(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
) -> jlong {
    unsafe { endpoint_ref(handle) }.buf_size() as jlong
}

/// `MockRdmaServerProvider.nativeHandlePut(long, byte[] s3Key, byte[] token, long offset, long size) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeHandlePut(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    s3_key: JByteArray<'_>,
    token: JByteArray<'_>,
    offset: jlong,
    size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let token_bytes = match env.convert_byte_array(&token) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let ep = unsafe { endpoint_ref(handle) };
    let buf_cap = if size > 0 { size as usize } else {
        ep.buf_size().min(128 * 1024 * 1024)
    };
    let mut buf = vec![0u8; buf_cap];
    match ep.handle_put(&key_bytes, &token_bytes, &mut buf, offset as usize, size as usize) {
        Ok(n) => {
            buf.truncate(n);
            env.byte_array_from_slice(&buf)
                .map(|a| a.into_raw())
                .unwrap_or(std::ptr::null_mut())
        }
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

/// `MockRdmaServerProvider.nativeHandleGet(long, byte[] s3Key, byte[] data, byte[] token, long offset) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaServerProvider_nativeHandleGet(
    mut env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    s3_key: JByteArray<'_>,
    data: JByteArray<'_>,
    token: JByteArray<'_>,
    offset: jlong,
) -> jlong {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    let data_bytes = match env.convert_byte_array(&data) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    let token_bytes = match env.convert_byte_array(&token) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    match unsafe { endpoint_ref(handle) }.handle_get(
        &key_bytes, &data_bytes, &token_bytes, offset as usize,
    ) {
        Ok(written) => written as jlong,
        Err(e) => { throw(&mut env, &e.to_string()); -1 }
    }
}
