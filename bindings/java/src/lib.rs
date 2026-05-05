//! JNI bindings for `s3-rdma`.
//!
//! Exposes the unified `RdmaProvider` Java class backed by
//! `Box<Arc<dyn RdmaProvider>>`, supporting client, server, and both roles
//! with any provider (`"mock"`, `"rdma"`, `"auto"`).

use std::sync::Arc;

use jni::objects::{JByteArray, JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jbyteArray, jlong, JNI_TRUE};
use jni::JNIEnv;

use s3_rdma::TelemLevel;
use s3_rdma::provider::{RdmaRole, RdmaProvider, RdmaClientProvider};
use s3_rdma::provider::{RdmaCompletionCallback, RdmaCompletionResult, RdmaServerConfig, DEFAULT_RDMA_BUF_SIZE};
use s3_rdma::provider::TelemetryConfig;

fn throw(env: &mut JNIEnv<'_>, msg: &str) {
    let _ = env.throw_new("io/s3rdma/RdmaException", msg);
}

// ══════════════════════════════════════════════════════════════════════════════
// RdmaProvider JNI  (unified client+server, backed by Arc<dyn RdmaProvider>)
// ══════════════════════════════════════════════════════════════════════════════

// Handle type for the unified RdmaProvider class.
// The jlong stores a raw pointer to a heap-allocated Arc<dyn RdmaProvider>.
// We box the Arc so we get a single-word (thin) pointer suitable for a jlong.
type ArcProvider = Arc<dyn RdmaProvider>;

/// # Safety
/// `handle` must be a non-null `jlong` produced by
/// `Java_io_s3rdma_RdmaProvider_nativeCreate` and not yet passed to
/// `Java_io_s3rdma_RdmaProvider_nativeDestroy`.
unsafe fn arc_provider_ref(handle: jlong) -> &'static dyn RdmaProvider {
    if handle == 0 {
        eprintln!("[s3-rdma-jni] FATAL: null handle — nativeCreate likely returned 0 (error)");
        std::process::abort();
    }
    let arc = unsafe { &*(handle as *const ArcProvider) };
    arc.as_ref()
}

fn parse_role(role: &str) -> Option<RdmaRole> {
    match role {
        "client" => Some(RdmaRole::Client),
        "server" => Some(RdmaRole::Server),
        "both"   => Some(RdmaRole::Both),
        _ => None,
    }
}

// ── Factory ───────────────────────────────────────────────────────────────────

/// `RdmaProvider.nativeCreate(provider, role, debug, shmPath, listenAddr,
///                             listenPort, poolCount, bufSize) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeCreate(
    mut env:        JNIEnv<'_>,
    _class:         JClass<'_>,
    provider:       JString<'_>,
    role:           JString<'_>,
    debug:          jboolean,
    shm_path:       JString<'_>,
    listen_addr:    JString<'_>,
    listen_port:    jni::sys::jint,
    pool_count:     jni::sys::jint,
    buf_size:       jlong,
) -> jlong {
    macro_rules! jstr {
        ($s:expr) => {
            match env.get_string(&$s) {
                Ok(s) => String::from(s),
                Err(e) => { throw(&mut env, &e.to_string()); return 0; }
            }
        };
    }
    let provider_str  = jstr!(provider);
    let role_str      = jstr!(role);
    let shm_path_str  = jstr!(shm_path);
    let listen_str    = jstr!(listen_addr);

    let provider_role = match parse_role(&role_str) {
        Some(r) => r,
        None => {
            throw(&mut env, &format!(
                "unknown role '{}' (valid: client, server, both)", role_str
            ));
            return 0;
        }
    };
    let cfg = RdmaServerConfig {
        shm_path:       shm_path_str,
        listen_addr:    listen_str,
        listen_port:    listen_port as u16,
        pool_count:     pool_count as usize,
        buf_pool_count: 0,
        buf_size:       buf_size as usize,
    };

    match s3_rdma::create_provider(&provider_str, debug == JNI_TRUE, provider_role, &cfg) {
        Ok(arc) => Box::into_raw(Box::new(arc)) as jlong,
        Err(e)  => { throw(&mut env, &e.to_string()); 0 }
    }
}

/// `RdmaProvider.nativeDestroy(long handle)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeDestroy(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) {
    if handle != 0 {
        let _ = unsafe { Box::from_raw(handle as *mut ArcProvider) };
    }
}

// ── Info ──────────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeName<'a>(
    env: JNIEnv<'a>, _obj: JObject<'a>, handle: jlong,
) -> JString<'a> {
    let s = unsafe { arc_provider_ref(handle) }.name();
    env.new_string(s).unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeProtocol<'a>(
    env: JNIEnv<'a>, _obj: JObject<'a>, handle: jlong,
) -> JString<'a> {
    let s = unsafe { arc_provider_ref(handle) }.protocol();
    env.new_string(s).unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsClient(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    unsafe { arc_provider_ref(handle) }.supports_client() as jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsServer(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    unsafe { arc_provider_ref(handle) }.supports_server() as jboolean
}

// ── Client methods ────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeIsMemorySuitable(
    _env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, ptr: jlong, size: jlong,
) -> jboolean {
    unsafe { arc_provider_ref(handle) }
        .is_memory_suitable(ptr as *const u8, size as usize) as jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeRegisterMemory(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, buf: JByteBuffer<'_>,
) {
    let ptr = match env.get_direct_buffer_address(&buf) {
        Ok(p) => p,
        Err(e) => { throw(&mut env, &format!("buf must be a direct ByteBuffer: {e}")); return; }
    };
    let len = match env.get_direct_buffer_capacity(&buf) {
        Ok(c) => c,
        Err(e) => { throw(&mut env, &e.to_string()); return; }
    };
    if let Err(e) = unsafe { arc_provider_ref(handle) }.register_memory(ptr, len) {
        throw(&mut env, &e.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeDeregisterMemory(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, buf: JByteBuffer<'_>,
) {
    let ptr = match env.get_direct_buffer_address(&buf) {
        Ok(p) => p,
        Err(e) => { throw(&mut env, &format!("buf must be a direct ByteBuffer: {e}")); return; }
    };
    if let Err(e) = unsafe { arc_provider_ref(handle) }.deregister_memory(ptr) {
        throw(&mut env, &e.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativePreparePutToken(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, s3_key: JByteArray<'_>, buf: JByteBuffer<'_>,
    offset: jlong, size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let buf_ptr = match env.get_direct_buffer_address(&buf) {
        Ok(p) => p as *const u8,
        Err(e) => { throw(&mut env, &format!("buf must be a direct ByteBuffer: {e}")); return std::ptr::null_mut(); }
    };
    match unsafe { arc_provider_ref(handle) }
        .prepare_put_token(&key_bytes, buf_ptr, size as usize, offset as usize)
    {
        Ok(tok) => env.byte_array_from_slice(&tok)
            .map(|a| a.into_raw()).unwrap_or(std::ptr::null_mut()),
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativePrepareGetToken(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, s3_key: JByteArray<'_>, buf: JByteBuffer<'_>,
    offset: jlong, size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let buf_ptr = match env.get_direct_buffer_address(&buf) {
        Ok(p) => p,
        Err(e) => { throw(&mut env, &format!("buf must be a direct ByteBuffer: {e}")); return std::ptr::null_mut(); }
    };
    match unsafe { arc_provider_ref(handle) }
        .prepare_get_token(&key_bytes, buf_ptr, size as usize, offset as usize)
    {
        Ok(tok) => env.byte_array_from_slice(&tok)
            .map(|a| a.into_raw()).unwrap_or(std::ptr::null_mut()),
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

/// `RdmaProvider.nativeProcessRdmaReply(handle, rdmaStatus, requestToken,
///                                       transferred, callback)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeProcessRdmaReply(
    mut env:       JNIEnv<'_>,
    _obj:          JObject<'_>,
    handle:        jlong,
    rdma_status:   jni::sys::jint,
    request_token: JByteArray<'_>,
    transferred:   jlong,
    callback:      JObject<'_>,
) {
    let req_bytes = match env.convert_byte_array(&request_token) {
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
            cb_global.as_obj(), "onComplete", "(I[B)V",
            &[JValue::Int(result.error_code), JValue::Object(&reply_arr)],
        );
    });

    if let Err(e) = unsafe { arc_provider_ref(handle) }
        .process_rdma_reply(rdma_status as u16, &req_bytes, transferred as usize, cb)
    {
        throw(&mut env, &e.to_string());
    }
}

// ── Server methods ────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeBufSize(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jlong {
    unsafe { arc_provider_ref(handle) }.buf_size() as jlong
}

/// `RdmaProvider.nativeHandlePut(handle, s3Key, token, offset, size) -> byte[]`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeHandlePut(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, s3_key: JByteArray<'_>, token: JByteArray<'_>,
    offset: jlong, size: jlong,
) -> jbyteArray {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b, Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let tok_bytes = match env.convert_byte_array(&token) {
        Ok(b) => b, Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };
    let ep = unsafe { arc_provider_ref(handle) };
    let raw_cap = ep.buf_size();
    let buf_cap = if size > 0 {
        size as usize
    } else {
        // buf_size() returns usize::MAX for mock when unconfigured; cap to avoid OOM.
        raw_cap.clamp(4096, DEFAULT_RDMA_BUF_SIZE)
    };
    let mut buf = vec![0u8; buf_cap];
    match ep.handle_put(&key_bytes, &tok_bytes, &mut buf, offset as usize, size as usize) {
        Ok(n) => {
            buf.truncate(n);
            env.byte_array_from_slice(&buf)
                .map(|a| a.into_raw()).unwrap_or(std::ptr::null_mut())
        }
        Err(e) => { throw(&mut env, &e.to_string()); std::ptr::null_mut() }
    }
}

/// `RdmaProvider.nativeHandleGet(handle, s3Key, data, token, offset) -> long`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeHandleGet(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, s3_key: JByteArray<'_>, data: JByteArray<'_>,
    token: JByteArray<'_>, offset: jlong,
) -> jlong {
    let key_bytes = match env.convert_byte_array(&s3_key) {
        Ok(b) => b, Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    let data_bytes = match env.convert_byte_array(&data) {
        Ok(b) => b, Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    let tok_bytes = match env.convert_byte_array(&token) {
        Ok(b) => b, Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };
    match unsafe { arc_provider_ref(handle) }
        .handle_get(&key_bytes, &data_bytes, &tok_bytes, offset as usize)
    {
        Ok(n)  => n as jlong,
        Err(e) => { throw(&mut env, &e.to_string()); -1 }
    }
}

// ── Telemetry ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsTelemetry(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    RdmaClientProvider::supports_telemetry(unsafe { arc_provider_ref(handle) }) as jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSetupTelemetry(
    mut env: JNIEnv<'_>, _obj: JObject<'_>,
    handle: jlong, use_otel: jboolean, log_file: JString<'_>, level: jni::sys::jint,
) {
    let log_file = if log_file.is_null() {
        None
    } else {
        match env.get_string(&log_file) {
            Ok(s) => Some(String::from(s)),
            Err(e) => { throw(&mut env, &e.to_string()); return; }
        }
    };
    let lvl = match level {
        0 => TelemLevel::Off,
        1 => TelemLevel::Error,
        2 => TelemLevel::Info,
        _ => TelemLevel::Debug,
    };
    let cfg = TelemetryConfig {
        use_otel: use_otel == JNI_TRUE,
        log_file,
        level: lvl,
    };
    if let Err(e) = RdmaClientProvider::setup_telemetry(
        unsafe { arc_provider_ref(handle) }, &cfg,
    ) {
        throw(&mut env, &e.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeShutdownTelemetry(
    mut env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) {
    if let Err(e) = RdmaClientProvider::shutdown_telemetry(unsafe { arc_provider_ref(handle) }) {
        throw(&mut env, &e.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSetTelemLevel(
    mut env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong, level: jni::sys::jint,
) {
    let lvl = match level {
        0 => TelemLevel::Off,
        1 => TelemLevel::Error,
        2 => TelemLevel::Info,
        _ => TelemLevel::Debug,
    };
    if let Err(e) = RdmaClientProvider::set_telem_level(
        unsafe { arc_provider_ref(handle) }, lvl,
    ) {
        throw(&mut env, &e.to_string());
    }
}
