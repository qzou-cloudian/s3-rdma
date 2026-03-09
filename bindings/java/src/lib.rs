//! JNI bindings for `s3-rdma` — exposes `MockRdmaProvider` to Java.
//!
//! The Java class `io.s3rdma.MockRdmaProvider` holds a `nativeHandle` (long)
//! which is a heap pointer to a boxed `MockRdmaProvider`.  Every JNI call
//! borrows the provider from that pointer without dropping it; `nativeDestroy`
//! drops it.

use jni::objects::{JByteArray, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jbyteArray, jlong, JNI_TRUE};
use jni::JNIEnv;

use s3_rdma::mock::MockRdmaProvider;
use s3_rdma::provider::{RdmaCompletionCallback, RdmaCompletionResult, RdmaProvider};

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Borrow a `MockRdmaProvider` from an opaque `jlong` handle without dropping.
///
/// # Safety
/// `handle` must be a valid pointer obtained from `native_create`.
unsafe fn provider_ref(handle: jlong) -> &'static MockRdmaProvider {
    unsafe { &*(handle as *const MockRdmaProvider) }
}

fn throw(env: &mut JNIEnv<'_>, msg: &str) {
    let _ = env.throw_new("io/s3rdma/RdmaException", msg);
}

// ── Factory ───────────────────────────────────────────────────────────────────

/// `MockRdmaProvider.nativeCreate(boolean debug) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeCreate(
    _env: JNIEnv<'_>,
    _class: JClass<'_>,
    debug: jboolean,
) -> jlong {
    let provider = MockRdmaProvider::new(debug == JNI_TRUE);
    Box::into_raw(Box::new(provider)) as jlong
}

/// `MockRdmaProvider.nativeDestroy(long handle)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeDestroy(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
) {
    if handle != 0 {
        // SAFETY: handle was created by `nativeCreate` and must not be used again.
        let _ = unsafe { Box::from_raw(handle as *mut MockRdmaProvider) };
    }
}

// ── Provider metadata ─────────────────────────────────────────────────────────

/// `MockRdmaProvider.nativeName(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeName<'a>(
    mut env: JNIEnv<'a>,
    _obj: JObject<'a>,
    handle: jlong,
) -> JString<'a> {
    // SAFETY: handle is valid for the lifetime of the call.
    let name = unsafe { provider_ref(handle) }.name();
    env.new_string(name).unwrap_or_else(|_| JString::default())
}

/// `MockRdmaProvider.nativeVersion(long handle) -> String`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeVersion<'a>(
    mut env: JNIEnv<'a>,
    _obj: JObject<'a>,
    handle: jlong,
) -> JString<'a> {
    let version = unsafe { provider_ref(handle) }.version();
    env.new_string(version.to_string())
        .unwrap_or_else(|_| JString::default())
}

// ── Memory management ─────────────────────────────────────────────────────────

/// `MockRdmaProvider.nativeIsMemorySuitable(long handle, long ptr, long size) -> boolean`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_MockRdmaProvider_nativeIsMemorySuitable(
    _env: JNIEnv<'_>,
    _obj: JObject<'_>,
    handle: jlong,
    ptr: jlong,
    size: jlong,
) -> jboolean {
    let suitable = unsafe { provider_ref(handle) }
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
    // We store the Java array's base address in the mock's registration map.
    // The pointer is only used as a key; MockRdmaProvider never dereferences it.
    let base_ptr = buf.as_raw() as *mut u8;
    if let Err(e) = unsafe { provider_ref(handle) }.register_memory(base_ptr, len) {
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
    if let Err(e) = unsafe { provider_ref(handle) }.deregister_memory(base_ptr) {
        throw(&mut env, &e.to_string());
    }
}

// ── Token generation ──────────────────────────────────────────────────────────

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
    // Use the Java array jobject address as the RDMA buffer pointer (mock only).
    let buf_ptr = buf.as_raw() as *const u8;
    match unsafe { provider_ref(handle) }.prepare_put_token(
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
    match unsafe { provider_ref(handle) }.prepare_get_token(
        &key_bytes, buf_ptr, size as usize, offset as usize,
    ) {
        Ok(token) => env.byte_array_from_slice(&token)
            .map(|a| a.into_raw())
            .unwrap_or(std::ptr::null_mut()),
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

// ── Reply processing ──────────────────────────────────────────────────────────

/// `MockRdmaProvider.nativeProcessReplyToken(long, byte[] replyToken, RdmaCompletionCallback) -> void`
///
/// Calls `callback.onComplete(errorCode, replyToken)` synchronously from Rust.
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

    // Capture a global ref to the callback so the closure can own it.
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

    if let Err(e) = unsafe { provider_ref(handle) }.process_reply_token(&reply_bytes, cb) {
        throw(&mut env, &e.to_string());
    }
}
