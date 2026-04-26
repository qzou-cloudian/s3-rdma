//! cuObject RDMA **server** provider via lazy runtime loading.
//!
//! Enabled by the `cuobj-server` Cargo feature.  At runtime the gateway
//! searches for the server plugin (`libs3_rdma_cuobj_server.so`) and
//! loads it on demand, so the main binary has **no compile-time dependency**
//! on `libcuobjclient.so` or `hsg-io`.
//!
//! Search order for `libs3_rdma_cuobj_server.so`:
//! 1. Standard dynamic-linker paths (`LD_LIBRARY_PATH`, RPATH, ldconfig cache).
//! 2. Directory containing the current executable.
//! 3. `/lib64/`, `/usr/lib64/` (common CUDA/cuObject install locations).

pub use crate::provider::RdmaServerConfig;

#[cfg(feature = "cuobj-server")]
mod real {
    use std::ffi::{c_void, CString};
    use libloading::{Library, Symbol};
    use crate::error::RdmaError;
    use crate::ffi::{ByteCursor, GetServerVtableFn, RdmaServerInitParams, RdmaServerVtable};
    use crate::provider::RdmaServerProvider;
    use super::RdmaServerConfig;
    use libc;

    const LIB_NAME_COMBINED: &str = "libs3rdmacuobj.so";
    const LIB_NAME: &str = "libs3_rdma_cuobj_server.so";

    fn try_load(path: impl AsRef<std::ffi::OsStr>) -> Option<Library> {
        unsafe { Library::new(path) }.ok()
    }

    fn find_library() -> Option<Library> {
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

    // ── CuObjServerProvider ───────────────────────────────────────────────────

    /// RDMA server provider that loads `libs3_rdma_cuobj_server.so` at runtime.
    ///
    /// Wraps a `CuObjServer` + `BufferPool` internally (inside the plugin);
    /// the gateway has no compile-time dependency on either.
    pub struct CuObjServerProvider {
        vtable:   *const RdmaServerVtable,
        ctx:      *mut c_void,
        buf_size: usize,
        _lib:     Library,
    }

    unsafe impl Send for CuObjServerProvider {}
    unsafe impl Sync for CuObjServerProvider {}

    impl CuObjServerProvider {
        /// Load the plugin and initialise it with the given config.
        pub fn new(cfg: &RdmaServerConfig) -> Result<Self, RdmaError> {
            let lib = find_library().ok_or_else(|| {
                RdmaError::CuObjConnectionFailed(
                    format!("{LIB_NAME_COMBINED} (or {LIB_NAME}) not found \
                             (searched LD_LIBRARY_PATH, exe dir, /lib64, /usr/lib64)")
                )
            })?;

            let vtable: *const RdmaServerVtable = unsafe {
                let get_vtable: Symbol<GetServerVtableFn> = lib
                    .get(b"s3_rdma_server_provider_get_vtable\0")
                    .map_err(|e| RdmaError::CuObjConnectionFailed(e.to_string()))?;
                let ptr = get_vtable();
                if ptr.is_null() {
                    return Err(RdmaError::CuObjConnectionFailed(
                        "s3_rdma_server_provider_get_vtable returned null".into()
                    ));
                }
                ptr
            };

            let addr_cstr = CString::new(cfg.listen_addr.as_str())
                .map_err(|e| RdmaError::CuObjConnectionFailed(e.to_string()))?;
            let params = RdmaServerInitParams {
                listen_addr: addr_cstr.as_ptr(),
                listen_port: cfg.listen_port,
                pool_count:  cfg.pool_count,
                buf_size:    cfg.buf_size,
            };

            let mut ctx = std::ptr::null_mut::<c_void>();
            if let Some(init_fn) = unsafe { (*vtable).init } {
                let rc = unsafe { init_fn(&params, &mut ctx) };
                if rc != 0 {
                    return Err(RdmaError::CuObjConnectionFailed(
                        format!("server vtable init() returned {rc}")
                    ));
                }
            }

            Ok(Self { vtable, ctx, buf_size: cfg.buf_size, _lib: lib })
        }

        fn vtable(&self) -> &RdmaServerVtable {
            unsafe { &*self.vtable }
        }
    }

    impl Drop for CuObjServerProvider {
        fn drop(&mut self) {
            if let Some(cleanup) = self.vtable().cleanup {
                unsafe { cleanup(self.ctx); }
            }
        }
    }

    impl RdmaServerProvider for CuObjServerProvider {
        fn name(&self) -> &str {
            // Read the provider name from the vtable's static C string.
            // Falls back to the compile-time constant if the pointer is null.
            let ptr = self.vtable().provider_name;
            if ptr.is_null() {
                return "CuObjServerProvider";
            }
            unsafe { std::ffi::CStr::from_ptr(ptr) }
                .to_str()
                .unwrap_or("CuObjServerProvider")
        }

        fn buf_size(&self) -> usize { self.buf_size }

        /// Delegates to the plugin's `handle_put` vtable entry.
        ///
        /// The plugin performs a cuObject `RDMA_READ` from the client's buffer,
        /// copies the transferred bytes into a pool slot, and returns them as a
        /// heap-allocated `ByteCursor`.  This wrapper reclaims that allocation.
        fn handle_put(
            &self,
            s3_key: &[u8],
            token: &[u8],
            offset: usize,
            size: usize,
        ) -> Result<Vec<u8>, RdmaError> {
            let f = self.vtable().handle_put.ok_or_else(|| {
                RdmaError::TransferFailed("handle_put not implemented in plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let token_cursor = ByteCursor::from_slice(token);
            let mut out      = ByteCursor::null();
            let rc = unsafe { f(self.ctx, &key_cursor, &token_cursor, offset, size, &mut out) };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("cuobj server handle_put returned {rc}")
                ));
            }
            // SAFETY: plugin is a cdylib and allocates via glibc malloc (not
            // mimalloc).  Using Box::from_raw here would cause an allocator
            // mismatch and SIGSEGV.  Use libc::free instead.
            let data = unsafe {
                let slice = std::slice::from_raw_parts(out.ptr, out.len);
                let v = slice.to_vec();
                libc::free(out.ptr as *mut libc::c_void);
                v
            };
            Ok(data)
        }

        /// Delegates to the plugin's `handle_get` vtable entry.
        ///
        /// Copies `data` into a pool slot and performs a cuObject `RDMA_WRITE`
        /// into the client's registered buffer via `token`.  Returns the number
        /// of bytes successfully written.
        fn handle_get(
            &self,
            s3_key: &[u8],
            data: &[u8],
            token: &[u8],
            offset: usize,
        ) -> Result<usize, RdmaError> {
            let f = self.vtable().handle_get.ok_or_else(|| {
                RdmaError::TransferFailed("handle_get not implemented in plugin".into())
            })?;
            let key_cursor   = ByteCursor::from_slice(s3_key);
            let data_cursor  = ByteCursor::from_slice(data);
            let token_cursor = ByteCursor::from_slice(token);
            let mut written: usize = 0;
            let rc = unsafe {
                f(self.ctx, &key_cursor, &data_cursor, &token_cursor, offset, &mut written)
            };
            if rc != 0 {
                return Err(RdmaError::TransferFailed(
                    format!("cuobj server handle_get returned {rc}")
                ));
            }
            Ok(written)
        }
    }
}

#[cfg(feature = "cuobj-server")]
pub use real::CuObjServerProvider;

