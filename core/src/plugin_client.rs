//! Generic RDMA client provider via lazy runtime loading.
//!
//! Enabled by the `plugin-client` Cargo feature.  At runtime `hsc` searches
//! for the provider plugin (`libs3rdmaclient.so`) and loads it on demand.
//! The main `hsc` binary therefore has **no compile-time or ELF-level
//! dependency** on any RDMA SDK — the library is pulled in transitively when
//! the plugin is dlopened.
//!
//! Search order for `libs3rdmaclient.so`:
//! 1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache).
//! 2. Directory containing the current executable.
//! 3. `/lib64/`, `/usr/lib64/` (common CUDA/cuObject install locations).

#[cfg(feature = "plugin-client")]
mod real {
    use std::ffi::{c_void, CString};

    use libloading::{Library, Symbol};

    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, GetNamedClientVtableFn, RdmaCompletionFn, RdmaProviderVtable};
    use crate::provider::{
        RdmaCompletionCallback, RdmaCompletionResult,
        rdma_status_to_error_code,
    };

    // ── Library search ────────────────────────────────────────────────────────

    const LIB_NAME: &str = "libs3rdmaclient.so";

    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<Library> {
        unsafe { Library::new(path) }.ok()
    }

    fn find_library() -> Option<Library> {
        // Search order:
        //   1. Standard linker paths (LD_LIBRARY_PATH, RPATH, ldconfig)
        //   2. Directory alongside the running executable
        //   3. Common RDMA SDK install locations
        if let Some(lib) = try_load(LIB_NAME) {
            return Some(lib);
        }
        if let Ok(exe) = std::env::current_exe() {
            if let Some(dir) = exe.parent() {
                if let Some(lib) = try_load(dir.join(LIB_NAME)) {
                    return Some(lib);
                }
            }
        }
        for dir in &["/lib64", "/usr/lib64"] {
            if let Some(lib) = try_load(format!("{dir}/{LIB_NAME}")) {
                return Some(lib);
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

    // ── PluginClientProvider ──────────────────────────────────────────────────

    /// RDMA client provider that loads `libs3rdmaclient.so` at runtime and
    /// calls through the exported [`RdmaProviderVtable`].
    ///
    /// The plugin is queried for the `"plugin"` backend name; the `.so` routes
    /// this to whichever hardware backend it was compiled with (currently cuObj).
    pub struct PluginClientProvider {
        // Order matters: ctx/vtable must be dropped (logically) before _lib
        // unloads, so cleanup() is called in Drop::drop() before _lib drops.
        vtable: *const RdmaProviderVtable,
        ctx:    *mut c_void,
        _lib:   Library,  // keep the .so mapped; drops last
    }

    unsafe impl Send for PluginClientProvider {}
    unsafe impl Sync for PluginClientProvider {}

    impl std::fmt::Debug for PluginClientProvider {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "PluginClientProvider(vtable)")
        }
    }

    impl PluginClientProvider {
        /// Try to load the provider plugin and initialise it.
        ///
        /// Returns `Err` if the plugin is not found, fails to load (e.g.
        /// the underlying RDMA SDK `.so` is missing), or the vtable's
        /// `init` fails.
        pub fn new() -> Result<Self, RdmaError> {
            let lib = find_library().ok_or_else(|| {
                RdmaError::PluginLoadFailed(
                    format!("{LIB_NAME} not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64)")
                )
            })?;

            let provider_name = CString::new("plugin").unwrap();
            let vtable: *const RdmaProviderVtable = unsafe {
                let get_vtable: Symbol<GetNamedClientVtableFn> = lib
                    .get(b"s3_rdma_get_client_vtable\0")
                    .map_err(|e| RdmaError::PluginLoadFailed(e.to_string()))?;
                let ptr = get_vtable(provider_name.as_ptr());
                if ptr.is_null() {
                    return Err(RdmaError::PluginLoadFailed(
                        format!("s3_rdma_get_client_vtable(\"plugin\") returned null")
                    ));
                }
                ptr
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(std::ptr::null_mut(), &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::PluginLoadFailed(
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

    impl Drop for PluginClientProvider {
        fn drop(&mut self) {
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe { cleanup(self.ctx); }
            }
        }
    }

    impl PluginClientProvider {
        pub fn name(&self)    -> &str { "PluginClientProvider" }
        pub fn version(&self) -> u32  { unsafe { (*self.vtable).provider_version } }

        pub fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
            if let Some(f) = self.vtable().is_memory_suitable {
                unsafe { f(self.ctx, ptr as *const std::ffi::c_void, size) }
            } else {
                true
            }
        }

        pub fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), RdmaError> {
            if let Some(f) = self.vtable().register_memory {
                let rc = unsafe { f(self.ctx, ptr as *mut std::ffi::c_void, size) };
                if rc != 0 {
                    return Err(RdmaError::MemoryRegistrationFailed {
                        ptr: ptr as usize, size,
                        reason: format!("vtable register_memory returned {rc}"),
                    });
                }
            }
            Ok(())
        }

        pub fn deregister_memory(&self, ptr: *mut u8) -> Result<(), RdmaError> {
            if let Some(f) = self.vtable().deregister_memory {
                let rc = unsafe { f(self.ctx, ptr as *mut std::ffi::c_void) };
                if rc != 0 {
                    return Err(RdmaError::MemoryDeregistrationFailed {
                        ptr: ptr as usize,
                        reason: format!("vtable deregister_memory returned {rc}"),
                    });
                }
            }
            Ok(())
        }

        pub fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
            if let Some(f) = self.vtable().get_max_transfer_size {
                unsafe { f(self.ctx, ptr as *const std::ffi::c_void) }
            } else {
                usize::MAX
            }
        }

        pub fn prepare_put_token(
            &self,
            s3_key: &[u8],
            buffer: *const u8,
            size:   usize,
            offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let f = self.vtable().prepare_put_token.ok_or_else(|| {
                RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_put_token not implemented in plugin".into(),
                }
            })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor,
                  buffer as *const std::ffi::c_void, size, offset,
                  &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("prepare_put_token returned {rc}"),
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

        pub fn prepare_get_token(
            &self,
            s3_key: &[u8],
            buffer: *mut u8,
            size:   usize,
            offset: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let f = self.vtable().prepare_get_token.ok_or_else(|| {
                RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: "prepare_get_token not implemented in plugin".into(),
                }
            })?;
            let key_cursor = ByteCursor::from_slice(s3_key);
            let mut out = ByteCursor::null();
            let rc = unsafe {
                f(self.ctx, &key_cursor,
                  buffer as *mut std::ffi::c_void, size, offset,
                  &mut out)
            };
            if rc != 0 {
                return Err(RdmaError::TokenGenerationFailed {
                    key: String::from_utf8_lossy(s3_key).into_owned(),
                    reason: format!("prepare_get_token returned {rc}"),
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

        /// Process the `x-amz-rdma-reply` status code returned by the S3 server.
        ///
        /// Always deregisters the MR via `process_reply_token(request_token)`
        /// regardless of RDMA status, so the plugin's pending map entry is
        /// cleaned up when the server replies.
        pub fn process_rdma_reply(
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

        pub fn rdma_token_header_name(&self) -> &[u8] {
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

        pub fn rdma_reply_header_name(&self) -> &[u8] {
            let Some(f) = self.vtable().rdma_reply_header_name else {
                return crate::provider::RDMA_REPLY_HEADER.as_bytes();
            };
            let cursor = unsafe { f(self.ctx) };
            if cursor.ptr.is_null() {
                return crate::provider::RDMA_REPLY_HEADER.as_bytes();
            }
            unsafe { std::slice::from_raw_parts(cursor.ptr, cursor.len) }
        }

        pub fn rdma_bytes_header_name(&self) -> &[u8] {
            let Some(f) = self.vtable().rdma_bytes_header_name else {
                return crate::provider::RDMA_BYTES_HEADER.as_bytes();
            };
            let cursor = unsafe { f(self.ctx) };
            if cursor.ptr.is_null() {
                return crate::provider::RDMA_BYTES_HEADER.as_bytes();
            }
            unsafe { std::slice::from_raw_parts(cursor.ptr, cursor.len) }
        }
    }

    // ── Availability probe ────────────────────────────────────────────────────

    /// Returns `true` if the RDMA client provider plugin can be loaded
    /// (i.e., `libs3rdmaclient.so` and its SDK dependencies are installed).
    pub fn is_available() -> bool {
        PluginClientProvider::new().is_ok()
    }
}

#[cfg(feature = "plugin-client")]
pub use real::PluginClientProvider;
#[cfg(feature = "plugin-client")]
pub use real::is_available as rdma_plugin_available;

// ── PluginProvider ────────────────────────────────────────────────────────────

/// Unified RDMA plugin provider that may carry both client and server roles.
///
/// Each role is loaded on demand from the corresponding plugin:
/// - Client: `libs3rdmaclient.so`
///   (requires `plugin-client` feature)
/// - Server: `libs3rdmaserver.so`
///   (requires `plugin-server` feature)
///
/// Either or both roles may be absent (plugin not found, hardware unavailable).
pub struct PluginProvider {
    #[cfg(feature = "plugin-client")]
    client: Option<real::PluginClientProvider>,
    #[cfg(feature = "plugin-server")]
    server: Option<crate::plugin_server::PluginServerProvider>,
    buf_size: usize,
}

unsafe impl Send for PluginProvider {}
unsafe impl Sync for PluginProvider {}

impl std::fmt::Debug for PluginProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginProvider")
            .field("buf_size", &self.buf_size)
            .finish()
    }
}

impl PluginProvider {
    /// Create a client-only provider.
    ///
    /// Returns `Err` if the `plugin-client` feature is not compiled in or the
    /// plugin cannot be loaded.
    pub fn new_client() -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        {
            let client = real::PluginClientProvider::new()?;
            Ok(Self {
                client: Some(client),
                #[cfg(feature = "plugin-server")]
                server: None,
                buf_size: 0,
            })
        }
        #[cfg(not(feature = "plugin-client"))]
        {
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "plugin".into(),
                reason: "compiled without the `plugin-client` feature".into(),
            })
        }
    }

    /// Create a server-only provider.
    pub fn new_server(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        #[cfg(feature = "plugin-server")]
        {
            let server = crate::plugin_server::PluginServerProvider::new(cfg)?;
            Ok(Self {
                #[cfg(feature = "plugin-client")]
                client: None,
                server: Some(server),
                buf_size: cfg.buf_size,
            })
        }
        #[cfg(not(feature = "plugin-server"))]
        {
            let _ = cfg;
            Err(crate::RdmaError::ProviderUnavailable {
                provider: "plugin".into(),
                reason: "compiled without the `plugin-server` feature".into(),
            })
        }
    }

    /// Create a provider carrying both client and server roles.
    ///
    /// If the client plugin fails to load, `supports_client()` returns `false`
    /// (not a hard error); the server role is still available.
    /// If the server plugin fails, `Err` is returned.
    pub fn new_both(cfg: &crate::provider::RdmaServerConfig) -> Result<Self, crate::RdmaError> {
        #[allow(unused_mut)]
        let mut ep = Self::new_server(cfg)?;
        #[cfg(feature = "plugin-client")]
        {
            ep.client = real::PluginClientProvider::new().ok();
        }
        Ok(ep)
    }
}

impl crate::provider::RdmaClientProvider for PluginProvider {
    fn name(&self) -> &str { "PluginProvider" }
    fn protocol(&self) -> &str { "plugin" }

    fn is_memory_suitable(&self, ptr: *const u8, size: usize) -> bool {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.is_memory_suitable(ptr, size);
        }
        let _ = (ptr, size);
        false
    }

    fn register_memory(&self, ptr: *mut u8, size: usize) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.register_memory(ptr, size);
        }
        let _ = (ptr, size);
        Err(crate::RdmaError::OperationNotSupported("plugin client not loaded".into()))
    }

    fn deregister_memory(&self, ptr: *mut u8) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.deregister_memory(ptr);
        }
        let _ = ptr;
        Err(crate::RdmaError::OperationNotSupported("plugin client not loaded".into()))
    }

    fn get_max_transfer_size(&self, ptr: *const u8) -> usize {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.get_max_transfer_size(ptr);
        }
        let _ = ptr;
        0
    }

    fn prepare_put_token(
        &self,
        s3_key: &[u8],
        buffer: *const u8,
        size:   usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.prepare_put_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("plugin client not loaded".into()))
    }

    fn prepare_get_token(
        &self,
        s3_key: &[u8],
        buffer: *mut u8,
        size:   usize,
        offset: usize,
    ) -> Result<Vec<u8>, crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.prepare_get_token(s3_key, buffer, size, offset);
        }
        let _ = (s3_key, buffer, size, offset);
        Err(crate::RdmaError::OperationNotSupported("plugin client not loaded".into()))
    }

    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: crate::provider::RdmaCompletionCallback,
    ) -> Result<(), crate::RdmaError> {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.process_rdma_reply(rdma_status, request_token, transferred, callback);
        }
        let _ = (rdma_status, request_token, transferred, callback);
        Err(crate::RdmaError::OperationNotSupported("plugin client not loaded".into()))
    }

    fn rdma_token_header_name(&self) -> &[u8] {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.rdma_token_header_name();
        }
        crate::provider::RDMA_TOKEN_HEADER.as_bytes()
    }

    fn rdma_reply_header_name(&self) -> &[u8] {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.rdma_reply_header_name();
        }
        crate::provider::RDMA_REPLY_HEADER.as_bytes()
    }

    fn rdma_bytes_header_name(&self) -> &[u8] {
        #[cfg(feature = "plugin-client")]
        if let Some(c) = &self.client {
            return c.rdma_bytes_header_name();
        }
        crate::provider::RDMA_BYTES_HEADER.as_bytes()
    }
}

impl crate::provider::RdmaServerProvider for PluginProvider {
    fn buf_size(&self) -> usize {
        self.buf_size
    }

    fn handle_put(
        &self,
        s3_key: &[u8],
        token:  &[u8],
        buf:    &mut [u8],
        offset: usize,
        size:   usize,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "plugin-server")]
        if let Some(s) = &self.server {
            return s.handle_put(s3_key, token, buf, offset, size);
        }
        let _ = (s3_key, token, buf, offset, size);
        Err(crate::RdmaError::OperationNotSupported("plugin server not loaded".into()))
    }

    fn handle_get(
        &self,
        s3_key: &[u8],
        data:   &[u8],
        token:  &[u8],
        offset: usize,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "plugin-server")]
        if let Some(s) = &self.server {
            return s.handle_get(s3_key, data, token, offset);
        }
        let _ = (s3_key, data, token, offset);
        Err(crate::RdmaError::OperationNotSupported("plugin server not loaded".into()))
    }

    unsafe fn handle_put_zc(
        &self,
        s3_key:        &[u8],
        token:         &[u8],
        offset:        usize,
        size:          usize,
        write_fn:      crate::ffi::RdmaIoWriteFn,
        write_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "plugin-server")]
        if let Some(s) = &self.server {
            return unsafe { s.handle_put_zc(s3_key, token, offset, size, write_fn, write_fn_data) };
        }
        let _ = (s3_key, token, offset, size, write_fn, write_fn_data);
        Err(crate::RdmaError::OperationNotSupported("plugin server not loaded".into()))
    }

    unsafe fn handle_get_zc(
        &self,
        s3_key:       &[u8],
        token:        &[u8],
        offset:       usize,
        read_fn:      crate::ffi::RdmaIoReadFn,
        read_fn_data: *mut std::ffi::c_void,
    ) -> Result<usize, crate::RdmaError> {
        #[cfg(feature = "plugin-server")]
        if let Some(s) = &self.server {
            return unsafe { s.handle_get_zc(s3_key, token, offset, read_fn, read_fn_data) };
        }
        let _ = (s3_key, token, offset, read_fn, read_fn_data);
        Err(crate::RdmaError::OperationNotSupported("plugin server not loaded".into()))
    }
}

impl crate::provider::RdmaProvider for PluginProvider {
    fn supports_client(&self) -> bool {
        #[cfg(feature = "plugin-client")]
        { self.client.is_some() }
        #[cfg(not(feature = "plugin-client"))]
        { false }
    }

    fn supports_server(&self) -> bool {
        #[cfg(feature = "plugin-server")]
        { self.server.is_some() }
        #[cfg(not(feature = "plugin-server"))]
        { false }
    }
}
