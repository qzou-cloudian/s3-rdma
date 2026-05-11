//! JNI bindings for `s3-rdma`.
//!
//! Exposes the unified `RdmaProvider` Java class backed by a `ProviderState`
//! that wraps an `Arc<dyn RdmaProvider>` and a per-provider registry of
//! in-flight RDMA transfers.  Supports client, server, and both roles with
//! any provider (`"mock"`, `"cuobj"`, `"auto"`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jni::objects::{JByteArray, JByteBuffer, JClass, JObject, JString, JValue};
use jni::sys::{jboolean, jbyteArray, jlong, JNI_TRUE};
use jni::JNIEnv;

use s3_rdma::TelemLevel;
use s3_rdma::provider::{
    InflightTransfers, PendingTransfer,
    RdmaRole, RdmaServerChannelExt,
};
use s3_rdma::provider::{RdmaServerConfig, DEFAULT_RDMA_BUF_SIZE};
use s3_rdma::provider::TelemetryConfig;

fn throw(env: &mut JNIEnv<'_>, msg: &str) {
    let _ = env.throw_new("io/s3rdma/RdmaException", msg);
}

// ══════════════════════════════════════════════════════════════════════════════
// Provider state  (unified client+server, backed by Arc<dyn RdmaProvider>)
// ══════════════════════════════════════════════════════════════════════════════

// ── ProviderState ─────────────────────────────────────────────────────────────

/// Per-provider state stored behind the `nativeHandle` jlong.
struct ProviderState {
    provider: Arc<dyn s3_rdma::provider::RdmaProvider>,
    /// In-flight RDMA transfers, keyed by token bytes.
    inflight:    InflightTransfers,
    /// Per-token destination buffer pointers for GET operations.
    get_buffers: Mutex<HashMap<Vec<u8>, (*mut u8, usize)>>,
}

/// # Safety
/// `handle` must be a non-null `jlong` produced by
/// `Java_io_s3rdma_RdmaProvider_nativeCreate` and not yet passed to
/// `Java_io_s3rdma_RdmaProvider_nativeDestroy`.
unsafe fn provider_state_ref(handle: jlong) -> &'static ProviderState {
    if handle == 0 {
        eprintln!("[s3-rdma-jni] FATAL: null handle — nativeCreate likely returned 0 (error)");
        std::process::abort();
    }
    &*(handle as *const ProviderState)
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
    // Pool management moved to hsg-rdma; accepted for Java ABI stability, unused.
    _pool_count:    jni::sys::jint,
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
        buf_size:       buf_size as usize,
    };

    match s3_rdma::create_provider(&provider_str, debug == JNI_TRUE, provider_role, Some(&cfg)) {
        Ok(arc) => {
            let state = Box::new(ProviderState {
                provider:    arc,
                inflight:    InflightTransfers::new(),
                get_buffers: Mutex::new(HashMap::new()),
            });
            Box::into_raw(state) as jlong
        }
        Err(e)  => { throw(&mut env, &e.to_string()); 0 }
    }
}

/// `RdmaProvider.nativeDestroy(long handle)`
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeDestroy(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) {
    if handle != 0 {
        let _ = unsafe { Box::from_raw(handle as *mut ProviderState) };
    }
}

// ── Info ──────────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeName<'a>(
    env: JNIEnv<'a>, _obj: JObject<'a>, handle: jlong,
) -> JString<'a> {
    let s = unsafe { provider_state_ref(handle) }.provider.name();
    env.new_string(s).unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeProtocol<'a>(
    env: JNIEnv<'a>, _obj: JObject<'a>, handle: jlong,
) -> JString<'a> {
    let s = unsafe { provider_state_ref(handle) }.provider.protocol();
    env.new_string(s).unwrap_or_else(|_| JString::default())
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsClient(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    unsafe { provider_state_ref(handle) }.provider.supports_client() as jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsServer(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    unsafe { provider_state_ref(handle) }.provider.supports_server() as jboolean
}

// ── Client methods ────────────────────────────────────────────────────────────

/// Always returns `true` — suitability is validated at `bind` time.
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeIsMemorySuitable(
    _env: JNIEnv<'_>, _obj: JObject<'_>,
    _handle: jlong, _ptr: jlong, _size: jlong,
) -> jboolean {
    JNI_TRUE
}

/// No-op — memory registration is handled internally by `nativePreparePutToken`
/// and `nativePrepareGetToken` when they allocate and bind a channel buffer.
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeRegisterMemory(
    _env: JNIEnv<'_>, _obj: JObject<'_>,
    _handle: jlong, _buf: JByteBuffer<'_>,
) {
    // No-op: channel API handles registration at prepare time.
}

/// No-op — channel resources are released when `nativeProcessRdmaReply`
/// consumes the pending transfer entry.
#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeDeregisterMemory(
    _env: JNIEnv<'_>, _obj: JObject<'_>,
    _handle: jlong, _buf: JByteBuffer<'_>,
) {
    // No-op: the channel is dropped when the pending transfer is removed.
}

/// `RdmaProvider.nativePreparePutToken(handle, s3Key, buf, offset, size) -> byte[]`
///
/// Allocates an RDMA channel, copies the Java direct buffer into it, prepares a
/// PUT token, and stores the pending transfer in the per-provider registry.
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
    let buf_cap = match env.get_direct_buffer_capacity(&buf) {
        Ok(c) => c as usize,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    let state = unsafe { provider_state_ref(handle) };

    let ch = match state.provider.bind(buf_cap, &key_bytes) {
        Ok(ch) => ch,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    // Copy the Java direct buffer into the channel's registered buffer.
    let copy_len = buf_cap.min(ch.size());
    if copy_len > 0 {
        unsafe { std::ptr::copy_nonoverlapping(buf_ptr, ch.ptr(), copy_len); }
    }

    let put_size = (size as usize).min(copy_len.saturating_sub(offset as usize));
    let put_handle = match ch.prepare_put(offset as usize, put_size) {
        Ok(h) => h,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    let token = put_handle.token().to_vec();
    state.inflight.insert(token.clone(), PendingTransfer::new(ch, put_handle));

    env.byte_array_from_slice(&token)
        .map(|a| a.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// `RdmaProvider.nativePrepareGetToken(handle, s3Key, buf, offset, size) -> byte[]`
///
/// Allocates an RDMA channel, prepares a GET token, and stores the pending
/// transfer (including the destination buffer pointer) in the per-provider
/// registry.  After the server fills the SHM buffer, `nativeProcessRdmaReply`
/// copies the data back into the Java direct ByteBuffer.
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
    let buf_cap = match env.get_direct_buffer_capacity(&buf) {
        Ok(c) => c as usize,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    let state = unsafe { provider_state_ref(handle) };

    let ch = match state.provider.bind(buf_cap, &key_bytes) {
        Ok(ch) => ch,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    let get_size = (size as usize).min(buf_cap.saturating_sub(offset as usize));
    let get_handle = match ch.prepare_get(offset as usize, get_size) {
        Ok(h) => h,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    let token = get_handle.token().to_vec();
    state.inflight.insert(token.clone(), PendingTransfer::new(ch, get_handle));
    state.get_buffers.lock().unwrap().insert(token.clone(), (buf_ptr, buf_cap));

    env.byte_array_from_slice(&token)
        .map(|a| a.into_raw())
        .unwrap_or(std::ptr::null_mut())
}

/// `RdmaProvider.nativeProcessRdmaReply(handle, rdmaStatus, requestToken,
///                                       transferred, callback)`
///
/// Looks up the pending transfer by token, completes it, and (for GET)
/// copies the received data back into the Java direct ByteBuffer.
/// Calls `callback.onComplete(errorCode, replyToken)` synchronously.
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

    let state = unsafe { provider_state_ref(handle) };

    let transfer = match state.inflight.remove(&req_bytes) {
        Some(p) => p,
        None => {
            throw(&mut env, "no pending transfer for the given token; \
                  call preparePutToken or prepareGetToken first");
            return;
        }
    };

    let transferred_n = transferred as usize;
    let status = rdma_status as u16;
    let get_buf = state.get_buffers.lock().unwrap().remove(&req_bytes);

    let result = if let Some((dst_ptr, dst_cap)) = get_buf {
        let chan_ptr  = transfer.channel().ptr();
        let chan_size = transfer.channel().size();
        let r = transfer.complete_get(status, transferred_n);
        if r.is_ok() {
            let n = transferred_n.min(dst_cap).min(chan_size);
            if n > 0 {
                unsafe { std::ptr::copy_nonoverlapping(chan_ptr, dst_ptr, n); }
            }
        }
        r
    } else {
        transfer.complete_put(status, transferred_n)
    };

    if let Err(e) = result {
        throw(&mut env, &e.to_string());
        return;
    }

    // Invoke callback.onComplete(0, new byte[0])
    let reply_arr = match env.byte_array_from_slice(b"") {
        Ok(a) => a,
        Err(_) => return,
    };
    let _ = env.call_method(
        &callback, "onComplete", "(I[B)V",
        &[JValue::Int(0), JValue::Object(&reply_arr)],
    );
}

// ── Server methods ────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeBufSize(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jlong {
    unsafe { provider_state_ref(handle) }.provider.buf_size() as jlong
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

    let state = unsafe { provider_state_ref(handle) };
    let buf_cap = if size > 0 {
        size as usize
    } else {
        // buf_size() may be usize::MAX for mock; cap to avoid OOM.
        state.provider.buf_size().clamp(4096, DEFAULT_RDMA_BUF_SIZE)
    };
    let mut received = Vec::<u8>::with_capacity(buf_cap);

    let ch = match state.provider.open_channel(&key_bytes, &tok_bytes) {
        Ok(ch) => ch,
        Err(e) => { throw(&mut env, &e.to_string()); return std::ptr::null_mut(); }
    };

    match ch.put(offset as usize, buf_cap, |data: &[u8]| {
        received.extend_from_slice(data);
        Ok(data.len())
    }) {
        Ok(_) => env.byte_array_from_slice(&received)
                    .map(|a| a.into_raw())
                    .unwrap_or(std::ptr::null_mut()),
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

    let state = unsafe { provider_state_ref(handle) };
    let data_len = data_bytes.len();

    let ch = match state.provider.open_channel(&key_bytes, &tok_bytes) {
        Ok(ch) => ch,
        Err(e) => { throw(&mut env, &e.to_string()); return -1; }
    };

    match ch.get(offset as usize, data_len, move |buf: &mut [u8]| {
        let n = data_len.min(buf.len());
        buf[..n].copy_from_slice(&data_bytes[..n]);
        Ok(n)
    }) {
        Ok(n)  => n as jlong,
        Err(e) => { throw(&mut env, &e.to_string()); -1 }
    }
}

// ── Telemetry ──────────────────────────────────────────────────────────────────

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeSupportsTelemetry(
    _env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) -> jboolean {
    unsafe { provider_state_ref(handle) }.provider.supports_telemetry() as jboolean
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
    if let Err(e) = unsafe { provider_state_ref(handle) }.provider.setup_telemetry(&cfg) {
        throw(&mut env, &e.to_string());
    }
}

#[no_mangle]
pub extern "system" fn Java_io_s3rdma_RdmaProvider_nativeShutdownTelemetry(
    mut env: JNIEnv<'_>, _obj: JObject<'_>, handle: jlong,
) {
    if let Err(e) = unsafe { provider_state_ref(handle) }.provider.shutdown_telemetry() {
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
    if let Err(e) = unsafe { provider_state_ref(handle) }.provider.set_telem_level(lvl) {
        throw(&mut env, &e.to_string());
    }
}

