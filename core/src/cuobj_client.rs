//! NVIDIA cuObject RDMA client provider via lazy runtime loading.
//!
//! Enabled by the `cuobj-client` Cargo feature.  At runtime `hsc` searches for
//! the provider plugin (`libs3_rdma_cuobj_client.so`) and loads it on demand.
//! The main `hsc` binary therefore has **no compile-time or ELF-level
//! dependency** on `libcuobjclient.so` — the library is pulled in transitively
//! when the plugin is dlopened.
//!
//! Search order for `libs3_rdma_cuobj_client.so`:
//! 1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache).
//! 2. Directory containing the current executable.
//! 3. `/lib64/`, `/usr/lib64/` (common CUDA/cuObject install locations).

#[cfg(feature = "cuobj-client")]
mod real {
    use std::ffi::c_void;

    use libloading::{Library, Symbol};

    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, GetVtableFn, RdmaCompletionFn, RdmaProviderVtable};
    use crate::provider::{
        RdmaCompletionCallback, RdmaCompletionResult, RdmaProvider,
        rdma_status_to_error_code,
    };

    // ── Library search ────────────────────────────────────────────────────────

    /// Combined single-file provider (preferred).
    const LIB_NAME_COMBINED: &str = "libs3rdmacuobj.so";
    /// Legacy split client-only plugin (fallback).
    const LIB_NAME: &str = "libs3_rdma_cuobj_client.so";

    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<Library> {
        unsafe { Library::new(path) }.ok()
    }

    fn find_library() -> Option<Library> {
        // Prefer the combined provider, fall back to the legacy client-only plugin.
        // For each candidate name, search:
        //   1. Standard linker paths (LD_LIBRARY_PATH, RPATH, ldconfig)
        //   2. Directory alongside the running executable
        //   3. Common CUDA/cuObject install locations
        for name in &[LIB_NAME_COMBINED, LIB_NAME] {
            if let Some(lib) = try_load(name) {
                return Some(lib);
            }
            if let Ok(exe) = std::env::current_exe() {
                if let Some(dir) = exe.parent() {
                    if let Some(lib) = try_load(dir.join(name)) {
                        return Some(lib);
                    }
                }
            }
            for dir in &["/lib64", "/usr/lib64"] {
                if let Some(lib) = try_load(format!("{dir}/{name}")) {
                    return Some(lib);
                }
            }
        }
        None
    }

    // ── Completion trampoline ─────────────────────────────────────────────────

    /// Static C callback used with `process_reply_token`.
    ///
    /// The provider implementations call this synchronously, so `user_data`
    /// points to a live `Box<RdmaCompletionCallback>` on the Rust side.
    unsafe extern "C" fn completion_trampoline(
        user_data:   *mut c_void,
        error_code:  i32,
        reply_token: *const ByteCursor,
    ) {
        let cb = unsafe { Box::from_raw(user_data as *mut RdmaCompletionCallback) };
        let token = if reply_token.is_null() {
            vec![]
        } else {
            unsafe { (*reply_token).as_slice().to_vec() }
        };
        cb(RdmaCompletionResult { error_code, reply_token: token });
    }

    // ── CuObjClientProvider ───────────────────────────────────────────────────

    /// RDMA client provider that loads `libs3_rdma_cuobj_client.so` at runtime
    /// and calls through the exported [`RdmaProviderVtable`].
    pub struct CuObjClientProvider {
        // Order matters: ctx/vtable must be dropped (logically) before _lib
        // unloads, so cleanup() is called in Drop::drop() before _lib drops.
        vtable: *const RdmaProviderVtable,
        ctx:    *mut c_void,
        _lib:   Library,  // keep the .so mapped; drops last
    }

    unsafe impl Send for CuObjClientProvider {}
    unsafe impl Sync for CuObjClientProvider {}

    impl std::fmt::Debug for CuObjClientProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CuObjClientProvider(vtable)")
        }
    }

    impl CuObjClientProvider {
        /// Try to load the provider plugin and initialise it.
        ///
        /// Returns `Err` if the plugin is not found, fails to load (e.g.
        /// `libcuobjclient.so` is missing), or the vtable's `init` fails.
        pub fn new() -> Result<Self, RdmaError> {
            let lib = find_library().ok_or_else(|| {
                RdmaError::CuObjConnectionFailed(
                    format!("{LIB_NAME_COMBINED} (or {LIB_NAME}) not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64)")
                )
            })?;

            let vtable: *const RdmaProviderVtable = unsafe {
                let get_vtable: Symbol<GetVtableFn> = lib
                    .get(b"s3_rdma_provider_get_vtable\0")
                    .map_err(|e| RdmaError::CuObjConnectionFailed(e.to_string()))?;
                let ptr = get_vtable();
                if ptr.is_null() {
                    return Err(RdmaError::CuObjConnectionFailed(
                        "s3_rdma_provider_get_vtable returned null".into()
                    ));
                }
                ptr
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(std::ptr::null_mut(), &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::CuObjConnectionFailed(
                        format!("vtable init() returned {rc}")
                    ));
                }
            }

            Ok(Self { vtable, ctx, _lib: lib })
        }

        fn vtable(&self) -> &RdmaProviderVtable {
            // SAFETY: vtable lives in the library's data segment which is
            // mapped for the lifetime of `self` (kept alive by `_lib`).
            unsafe { &*self.vtable }
        }
    }

    impl Drop for CuObjClientProvider {
        fn drop(&mut self) {
            // Call cleanup before `_lib` drops and unloads the .so.
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe { cleanup(self.ctx); }
            }
        }
    }

    // ── RdmaProvider impl ─────────────────────────────────────────────────────

    impl RdmaProvider for CuObjClientProvider {
        fn name(&self)    -> &str { "CuObjClientProvider" }
        fn version(&self) -> u32  { unsafe { (*self.vtable).provider_version } }

        fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
            let Some(f) = self.vtable().is_memory_suitable else { return false; };
            unsafe { f(self.ctx, ptr as *const c_void, size) }
        }

        fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().register_memory else { return Ok(()); };
            let rc = unsafe { f(self.ctx, ptr as *mut c_void, size) };
            if rc != 0 {
                Err(RdmaError::MemoryRegistrationFailed {
                    ptr: ptr as usize, size,
                    reason: format!("vtable register_memory returned {rc}"),
                })
            } else { Ok(()) }
        }

        fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().deregister_memory else { return Ok(()); };
            let rc = unsafe { f(self.ctx, ptr as *mut c_void) };
            if rc != 0 {
                Err(RdmaError::MemoryDeregistrationFailed {
                    ptr: ptr as usize,
                    reason: format!("vtable deregister_memory returned {rc}"),
                })
            } else { Ok(()) }
        }

        fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
            let Some(f) = self.vtable().get_max_transfer_size else { return usize::MAX; };
            let v = unsafe { f(self.ctx, ptr as *const c_void) };
            if v == 0 { usize::MAX } else { v }
        }

        fn prepare_put_token(
            &self, s3_key: &[u8], buffer: *const u8, size: usize, offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let Some(f) = self.vtable().prepare_put_token else {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_put_token not implemented".into(),
                });
            };
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor, buffer as *const c_void, size, offset, &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("vtable prepare_put_token returned {rc}"),
                });
            }
            // SAFETY: the cdylib allocates the token bytes with Box::into_raw.
            let token = unsafe {
                let slice = std::slice::from_raw_parts(out.ptr, out.len);
                let v = slice.to_vec();
                // Reclaim the box the cdylib leaked.
                drop(Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(out.ptr as *mut u8, out.len)
                ));
                v
            };
            Ok(token)
        }

        fn prepare_get_token(
            &self, s3_key: &[u8], buffer: *mut u8, size: usize, offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let Some(f) = self.vtable().prepare_get_token else {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_get_token not implemented".into(),
                });
            };
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor, buffer as *mut c_void, size, offset, &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("vtable prepare_get_token returned {rc}"),
                });
            }
            let token = unsafe {
                let slice = std::slice::from_raw_parts(out.ptr, out.len);
                let v = slice.to_vec();
                drop(Box::from_raw(
                    std::ptr::slice_from_raw_parts_mut(out.ptr as *mut u8, out.len)
                ));
                v
            };
            Ok(token)
        }

        /// Override the default `process_rdma_reply` to deregister the MR via
        /// `c_process_reply_token(request_token)` regardless of RDMA status.
        ///
        /// The default trait implementation only uses `rdma_status` to decide
        /// success/failure; it never touches the C vtable's `process_reply_token`
        /// function, so the cuobj pending map entry (keyed by the token bytes)
        /// would never be removed.  This override passes the **request** token
        /// (not the reply header value) to `c_process_reply_token` so the
        /// `RegisteredBuffer` RAII guard is dropped and the MR is deregistered
        /// when the server replies — whether successfully or with an error.
        fn process_rdma_reply(
            &self,
            rdma_status: u16,
            request_token: &[u8],
            _transferred: usize,
            callback: RdmaCompletionCallback,
        ) -> Result<(), RdmaError> {
            // Always deregister the MR, whether the server reports success or error.
            if let Some(f) = self.vtable().process_reply_token {
                let cursor = ByteCursor::from_slice(request_token);
                // Pass null user_data and no callback — we only need the cleanup.
                unsafe { f(self.ctx, &cursor, std::ptr::null_mut(), None); }
            }
            callback(RdmaCompletionResult {
                error_code: rdma_status_to_error_code(rdma_status),
                reply_token: vec![],
            });
            Ok(())
        }

        #[allow(deprecated)]
        fn process_reply_token(
            &self, reply_token: &[u8], callback: RdmaCompletionCallback,
        ) -> Result<(), RdmaError> {
            let Some(f) = self.vtable().process_reply_token else {
                // No-op: just fire the callback with success.
                callback(RdmaCompletionResult { error_code: 0, reply_token: reply_token.to_vec() });
                return Ok(());
            };
            // Box the callback and pass it as user_data to the trampoline.
            // The trampoline reclaims the Box; if rc != 0 we reclaim it here.
            let cb_ptr = Box::into_raw(Box::new(callback));
            let reply_cursor = ByteCursor::from_slice(reply_token);
            let rc = unsafe {
                f(self.ctx, &reply_cursor, cb_ptr as *mut c_void,
                  Some(completion_trampoline as RdmaCompletionFn))
            };
            if rc != 0 {
                // Callback was not invoked; reclaim the box.
                unsafe { drop(Box::from_raw(cb_ptr)); }
                Err(RdmaError::ReplyTokenProcessingFailed(
                    format!("vtable process_reply_token returned {rc}")
                ))
            } else {
                Ok(())
            }
        }

        fn rdma_token_header_name(&self) -> &[u8] {
            let Some(f) = self.vtable().rdma_token_header_name else {
                return crate::provider::RDMA_TOKEN_HEADER.as_bytes();
            };
            let cursor = unsafe { f(self.ctx) };
            if cursor.ptr.is_null() {
                return crate::provider::RDMA_TOKEN_HEADER.as_bytes();
            }
            // SAFETY: returned ByteCursor points into static string in the .so.
            unsafe { std::slice::from_raw_parts(cursor.ptr, cursor.len) }
        }

        fn rdma_reply_header_name(&self) -> &[u8] {
            let Some(f) = self.vtable().rdma_reply_header_name else {
                return crate::provider::RDMA_REPLY_HEADER.as_bytes();
            };
            let cursor = unsafe { f(self.ctx) };
            if cursor.ptr.is_null() {
                return crate::provider::RDMA_REPLY_HEADER.as_bytes();
            }
            unsafe { std::slice::from_raw_parts(cursor.ptr, cursor.len) }
        }
    }

    // ── Availability probe ────────────────────────────────────────────────────

    /// Returns `true` if the cuObject client provider plugin can be loaded
    /// (i.e., `libs3_rdma_cuobj_client.so` and `libcuobjclient.so` are installed).
    pub fn is_available() -> bool {
        CuObjClientProvider::new().is_ok()
    }
}

#[cfg(feature = "cuobj-client")]
pub use real::CuObjClientProvider;
#[cfg(feature = "cuobj-client")]
pub use real::is_available as cuobj_available;

// ── CuObjEndpoint ─────────────────────────────────────────────────────────────

/// Unified cuObject RDMA endpoint that may carry both client and server roles.
///
/// Each role is loaded on demand from the corresponding plugin:
/// - Client: `libs3rdmacuobj.so` / `libs3_rdma_cuobj_client.so`  
///   (requires `cuobj-client` feature)
/// - Server: `libs3rdmacuobj.so` / `libs3_rdma_cuobj_server.so`  
///   (requires `cuobj-server` feature)
///
/// Either or both roles may be absent (plugin not found, hardware unavailable).
pub struct CuObjEndpoint {
    #[cfg(feature = "cuobj-client")]
    client: Option<real::CuObjClientProvider>,
    #[cfg(feature = "cuobj-server")]
    server: Option<crate::cuobj_server::CuObjServerProvider>,
    buf_size: usize,
}

unsafe impl Send for CuObjEndpoint {}
unsafe impl Sync for CuObjEndpoint {}

impl std::fmt::Debug for CuObjEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CuObjEndpoint")
            .field("buf_size", &self.buf_size)
            .finish()
    }
}

impl CuObjEndpoint {
    /// Create a client-only endpoint.
    ///
    /// Returns `Err` if the `cuobj-client` feature is not compiled in or the
    /// plugin cannot be loaded.
    pub fn new_client() -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        {
            let client = real::CuObjClientProvider::new()?;
            Ok(Self {
                client: Some(client),
                #[cfg(feature = "cuobj-server")]
                server: None,
                buf_size: 0,
            })
        }
        #[cfg(not(feature = "cuobj-client"))]
        {
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "cuobj".into(),
                reason: "compiled without the `cuobj-client` feature".into(),
            })
        }
    }

    /// Create a server-only endpoint.
    pub fn new_server(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "cuobj-server")]
        {
            let server = crate::cuobj_server::real::CuObjServerProvider::new(cfg)?;
            Ok(Self {
                #[cfg(feature = "cuobj-client")]
                client: None,
                server: Some(server),
                buf_size: cfg.buf_size,
            })
        }
        #[cfg(not(feature = "cuobj-server"))]
        {
            let _ = cfg;
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "cuobj".into(),
                reason: "compiled without the `cuobj-server` feature".into(),
            })
        }
    }

    /// Create an endpoint carrying both client and server roles.
    ///
    /// If the client plugin fails to load, `supports_client()` will return
    /// `false` (not a hard error); the server role is still available.
    /// If the server plugin fails, `Err` is returned.
    pub fn new_both(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        let mut ep = Self::new_server(cfg)?;
        #[cfg(feature = "cuobj-client")]
        {
            ep.client = real::CuObjClientProvider::new().ok();
        }
        Ok(ep)
    }
}

impl crate::endpoint::RdmaEndpoint for CuObjEndpoint {
    fn name(&self) -> &str { "CuObjEndpoint" }
    fn protocol(&self) -> &str { "cuobj" }

    fn supports_client(&self) -> bool {
        #[cfg(feature = "cuobj-client")]
        { self.client.is_some() }
        #[cfg(not(feature = "cuobj-client"))]
        { false }
    }

    fn supports_server(&self) -> bool {
        #[cfg(feature = "cuobj-server")]
        { self.server.is_some() }
        #[cfg(not(feature = "cuobj-server"))]
        { false }
    }

    // ── Client methods — delegate to inner CuObjClientProvider ─────────────

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.is_memory_suitable(ptr, size);
        }
        let _ = (ptr, size);
        false
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.register_memory(ptr, size);
        }
        let _ = (ptr, size);
        Err(crate::RdmaError::OperationNotSupported("cuobj client not loaded".into()))
    }

    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.deregister_memory(ptr);
        }
        let _ = ptr;
        Err(crate::RdmaError::OperationNotSupported("cuobj client not loaded".into()))
    }

    fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.get_max_transfer_size(ptr);
        }
        let _ = ptr;
        usize::MAX
    }

    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.prepare_put_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("cuobj client not loaded".into()))
    }

    fn prepare_get_token(
        &self,
        s3_key: &[u8],
        buffer: *mut u8,
        size: usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.prepare_get_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("cuobj client not loaded".into()))
    }

    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: crate::provider::RdmaCompletionCallback,
    ) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.process_rdma_reply(rdma_status, request_token, transferred, callback);
        }
        let _ = (rdma_status, request_token, transferred);
        // Fallback: fire callback based on status
        use crate::provider::{RdmaCompletionResult, rdma_status_to_error_code};
        callback(RdmaCompletionResult {
            error_code: rdma_status_to_error_code(rdma_status),
            reply_token: vec![],
        });
        Ok(())
    }

    fn rdma_token_header_name(&self) -> &[u8] {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.rdma_token_header_name();
        }
        crate::provider::RDMA_TOKEN_HEADER.as_bytes()
    }

    fn rdma_reply_header_name(&self) -> &[u8] {
        #[cfg(feature = "cuobj-client")]
        if let Some(c) = &self.client {
            use crate::provider::RdmaProvider;
            return c.rdma_reply_header_name();
        }
        crate::provider::RDMA_REPLY_HEADER.as_bytes()
    }

    // ── Server methods — delegate to inner CuObjServerProvider ─────────────

    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn handle_put(
        &self,
        s3_key: &[u8],
        token: &[u8],
        buf: &mut [u8],
        offset: usize,
        size: usize,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "cuobj-server")]
        if let Some(s) = &self.server {
            use crate::provider::RdmaServerProvider;
            return s.handle_put(s3_key, token, buf, offset, size);
        }
        let _ = (s3_key, token, buf, offset, size);
        Err(crate::RdmaError::OperationNotSupported("cuobj server not loaded".into()))
    }

    fn handle_get(
        &self,
        s3_key: &[u8],
        data: &[u8],
        token: &[u8],
        offset: usize,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "cuobj-server")]
        if let Some(s) = &self.server {
            use crate::provider::RdmaServerProvider;
            return s.handle_get(s3_key, data, token, offset);
        }
        let _ = (s3_key, data, token, offset);
        Err(crate::RdmaError::OperationNotSupported("cuobj server not loaded".into()))
    }
}

