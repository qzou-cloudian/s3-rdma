//! Python bindings for `s3-rdma` via PyO3.
//!
//! Exposes [`RdmaProvider`] and [`MockRdmaProvider`] to Python.
//! Build with maturin from the `bindings/python/` directory:
//!
//! ```sh
//! cd bindings/python && python3 -m maturin develop
//! ```

use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::PyClassInitializer;
use pyo3::types::PyBytes;

use crate::endpoint::RdmaProvider;
use crate::endpoint::RdmaRole;
use crate::provider::{RdmaServerConfig, RdmaCompletionCallback};

fn parse_role(role: &str) -> PyResult<RdmaRole> {
    match role {
        "client" => Ok(RdmaRole::Client),
        "server" => Ok(RdmaRole::Server),
        "both"   => Ok(RdmaRole::Both),
        other => Err(PyRuntimeError::new_err(
            format!("unknown role '{other}' (valid: client, server, both)")
        )),
    }
}

// ── RdmaProvider Python class ─────────────────────────────────────────────────

/// Unified RDMA endpoint supporting client, server, or both roles.
///
/// Wraps the Rust ``RdmaProvider`` trait and supports all provider types
/// (``"mock"``, ``"cuobj"``, ``"auto"``) and roles (``"client"``,
/// ``"server"``, ``"both"``).
///
/// Example (client role)::
///
///     import s3_rdma
///     ep = s3_rdma.RdmaProvider(provider="mock", role="client")
///     ep.register_memory(data)
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     ep.process_rdma_reply(200, token, len(data), lambda code, reply: None)
///     ep.deregister_memory(data)
///
/// Example (server role)::
///
///     ep = s3_rdma.RdmaProvider(provider="mock", role="server")
///     received = ep.handle_put(b"bucket/key", token, size=len(data))
///     written  = ep.handle_get(b"bucket/key", received, token)
#[pyclass(name = "RdmaProvider", subclass)]
pub struct PyRdmaProvider {
    inner: Arc<dyn RdmaProvider>,
}

#[pymethods]
impl PyRdmaProvider {
    /// Create an RDMA endpoint.
    ///
    /// :param provider: ``"mock"`` (default), ``"cuobj"``, or ``"auto"``.
    /// :param role: ``"both"`` (default), ``"client"``, or ``"server"``.
    /// :param debug: emit trace lines to stderr (default ``False``).
    /// :param shm_path: SHM directory for the mock provider (default ``"/dev/shm"``).
    /// :param listen_addr: RDMA listen address for the cuObj server.
    /// :param listen_port: RDMA listen port for the cuObj server.
    /// :param pool_count: number of buffer pool slots for the cuObj server (default 4).
    /// :param buf_size: buffer slot size in bytes for the cuObj server (default 128 MiB).
    /// :raises RuntimeError: if the provider is unavailable or fails to initialise.
    #[new]
    #[pyo3(signature = (
        provider = "mock",
        role = "both",
        *,
        debug = false,
        shm_path = "/dev/shm",
        listen_addr = "",
        listen_port = 0u16,
        pool_count = 4usize,
        buf_size = 134217728usize,
    ))]
    pub fn new(
        provider:    &str,
        role:        &str,
        debug:       bool,
        shm_path:    &str,
        listen_addr: &str,
        listen_port: u16,
        pool_count:  usize,
        buf_size:    usize,
    ) -> PyResult<Self> {
        let endpoint_role = parse_role(role)?;
        let cfg = RdmaServerConfig {
            shm_path:    shm_path.to_owned(),
            listen_addr: listen_addr.to_owned(),
            listen_port,
            pool_count,
            buf_size,
        };
        crate::create_provider(provider, debug, endpoint_role, &cfg)
            .map(|inner| Self { inner })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ── Info ─────────────────────────────────────────────────────────────────

    /// Human-readable endpoint name (e.g. ``"MockRdmaProvider"``).
    fn name(&self) -> &str { self.inner.name() }

    /// Transport protocol: ``"mock"`` or ``"cuobj"``.
    fn protocol(&self) -> &str { self.inner.protocol() }

    /// ``True`` if this endpoint supports client-side operations.
    fn supports_client(&self) -> bool { self.inner.supports_client() }

    /// ``True`` if this endpoint supports server-side operations.
    fn supports_server(&self) -> bool { self.inner.supports_server() }

    /// Capacity in bytes of the server's per-slot transfer buffer.
    fn buf_size(&self) -> usize { self.inner.buf_size() }

    // ── Client methods ────────────────────────────────────────────────────────

    /// Returns ``True`` if the buffer at ``ptr`` of ``size`` bytes is RDMA-eligible.
    ///
    /// :param ptr: raw integer address of the buffer.
    /// :param size: buffer size in bytes.
    fn is_memory_suitable(&self, ptr: u64, size: usize) -> bool {
        self.inner.is_memory_suitable(ptr as *const u8, size)
    }

    /// Register a memory region for RDMA.
    ///
    /// :param data: the buffer to register (``bytes`` or ``bytearray``).
    fn register_memory<'py>(&self, data: &Bound<'py, PyBytes>) -> PyResult<()> {
        let b = data.as_bytes();
        self.inner
            .register_memory(b.as_ptr() as *mut u8, b.len())
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Deregister a previously registered memory region.
    ///
    /// :param data: the same buffer passed to :meth:`register_memory`.
    fn deregister_memory<'py>(&self, data: &Bound<'py, PyBytes>) -> PyResult<()> {
        let b = data.as_bytes();
        self.inner
            .deregister_memory(b.as_ptr() as *mut u8)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Generate an RDMA token for an S3 PUT (upload).
    ///
    /// :param data:   buffer containing the data to upload.
    /// :param offset: byte offset within the buffer for this sub-request.
    /// :param size:   number of bytes to transfer.
    /// :param s3_key: the S3 object key (as ``bytes``).
    /// :returns: opaque token to attach as ``x-amz-rdma-token``.
    fn prepare_put_token<'py>(
        &self,
        py:     Python<'py>,
        data:   &Bound<'py, PyBytes>,
        offset: usize,
        size:   usize,
        s3_key: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let b = data.as_bytes();
        let token = self.inner
            .prepare_put_token(s3_key, b.as_ptr(), size.min(b.len()), offset)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyBytes::new_bound(py, &token))
    }

    /// Generate an RDMA token for an S3 GET (download).
    ///
    /// :param data:   buffer that will receive the downloaded data.
    /// :param offset: byte offset within the buffer for this sub-request.
    /// :param size:   number of bytes to transfer.
    /// :param s3_key: the S3 object key (as ``bytes``).
    /// :returns: opaque token to attach as ``x-amz-rdma-token``.
    fn prepare_get_token<'py>(
        &self,
        py:     Python<'py>,
        data:   &Bound<'py, PyBytes>,
        offset: usize,
        size:   usize,
        s3_key: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let b = data.as_bytes();
        let token = self.inner
            .prepare_get_token(s3_key, b.as_ptr() as *mut u8, size.min(b.len()), offset)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        Ok(PyBytes::new_bound(py, &token))
    }

    /// Process the ``x-amz-rdma-reply`` status returned by the S3 server.
    ///
    /// Calls ``callback(error_code: int, reply_token: bytes)`` synchronously.
    ///
    /// :param rdma_status:    numeric status (200/204/206 = success, 501 = fallback, 0 = absent).
    /// :param request_token:  the token originally sent in the PUT/GET request.
    /// :param transferred:    bytes transferred (from ``x-amz-rdma-bytes``; use 0 if absent).
    /// :param callback:       callable invoked with ``(error_code, reply_token)``.
    fn process_rdma_reply(
        &self,
        rdma_status:   u16,
        request_token: &[u8],
        transferred:   usize,
        callback:      PyObject,
    ) -> PyResult<()> {
        let cb: RdmaCompletionCallback = Box::new(move |result| {
            Python::with_gil(|py| {
                let _ = callback.call1(py, (result.error_code, result.reply_token.as_slice()));
            });
        });
        self.inner
            .process_rdma_reply(rdma_status, request_token, transferred, cb)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ── Server methods ────────────────────────────────────────────────────────

    /// Handle an RDMA PUT from a client: reads ``size`` bytes from the
    /// client's registered buffer and returns them.
    ///
    /// :param s3_key: the S3 object key.
    /// :param token:  the RDMA descriptor token from ``x-amz-rdma-token``.
    /// :param size:   bytes to transfer (0 = use full token capacity, up to 128 MiB).
    /// :param offset: byte offset into the object (default 0).
    /// :returns: the bytes received from the client.
    #[pyo3(signature = (s3_key, token, size = 0, offset = 0))]
    fn handle_put<'py>(
        &self,
        py:     Python<'py>,
        s3_key: &[u8],
        token:  &[u8],
        size:   usize,
        offset: usize,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let raw_cap = self.inner.buf_size();
        let buf_cap = if size > 0 {
            size
        } else {
            // buf_size() returns usize::MAX for the mock when unconfigured;
            // cap at 128 MiB to avoid allocating a giant buffer by default.
            raw_cap.min(128 * 1024 * 1024).max(4096)
        };
        let mut buf = vec![0u8; buf_cap];
        let n = self.inner
            .handle_put(s3_key, token, &mut buf, offset, size)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        buf.truncate(n);
        Ok(PyBytes::new_bound(py, &buf))
    }

    /// Handle an RDMA GET from a client: writes ``data`` into the client's
    /// registered buffer and returns the number of bytes written.
    ///
    /// :param s3_key: the S3 object key.
    /// :param data:   the object payload to send to the client.
    /// :param token:  the RDMA descriptor token from ``x-amz-rdma-token``.
    /// :param offset: byte offset into the client buffer (default 0).
    /// :returns: number of bytes written to the client buffer.
    #[pyo3(signature = (s3_key, data, token, offset = 0))]
    fn handle_get(
        &self,
        s3_key: &[u8],
        data:   &[u8],
        token:  &[u8],
        offset: usize,
    ) -> PyResult<usize> {
        self.inner
            .handle_get(s3_key, data, token, offset)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!(
            "RdmaProvider(name={}, protocol={}, client={}, server={})",
            self.inner.name(),
            self.inner.protocol(),
            self.inner.supports_client(),
            self.inner.supports_server(),
        )
    }
}

// ── MockRdmaProvider — convenience subclass with a simpler constructor ─────────

/// Mock RDMA endpoint — no NVIDIA hardware required.
///
/// Equivalent to ``RdmaProvider(provider="mock", role="both")`` with a
/// simpler constructor.  Both client and server roles are active.
///
/// Example::
///
///     import s3_rdma
///     ep = s3_rdma.MockRdmaProvider(debug=True)
///     ep.register_memory(data)
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     received = ep.handle_put(b"bucket/key", token, size=len(data))
#[pyclass(name = "MockRdmaProvider", extends = PyRdmaProvider)]
pub struct PyMockRdmaProvider;

#[pymethods]
impl PyMockRdmaProvider {
    /// Create a mock endpoint.
    ///
    /// :param debug: if ``True``, print trace lines to stderr.
    #[new]
    #[pyo3(signature = (debug = false))]
    fn new(debug: bool) -> PyResult<PyClassInitializer<Self>> {
        let base = PyRdmaProvider::new("mock", "both", debug, "/dev/shm", "", 0, 4, 0)?;
        Ok(PyClassInitializer::from(base).add_subclass(PyMockRdmaProvider))
    }
}

// ── CuObjProvider — convenience subclass for the cuObject RDMA provider ───────

/// CuObj RDMA endpoint — requires NVIDIA hardware and the cuObject SDK plugin.
///
/// Equivalent to ``RdmaProvider(provider="cuobj", role=role)`` with a
/// simpler constructor.  Raises ``RuntimeError`` if the cuObj plugin
/// (``libs3rdmacuobjclient.so`` / ``libs3rdmacuobjserver.so``) cannot be
/// found at runtime.
///
/// Example (client role)::
///
///     import s3_rdma
///     ep = s3_rdma.CuObjProvider(role="client")
///     ep.register_memory(data)
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     ep.process_rdma_reply(200, token, len(data), lambda code, reply: None)
///     ep.deregister_memory(data)
///
/// Example (server role)::
///
///     ep = s3_rdma.CuObjProvider(role="server", listen_addr="192.168.1.1", listen_port=4791)
///     received = ep.handle_put(b"bucket/key", token, size=len(data))
///     written  = ep.handle_get(b"bucket/key", received, token)
#[pyclass(name = "CuObjProvider", extends = PyRdmaProvider)]
pub struct PyCuObjProvider;

#[pymethods]
impl PyCuObjProvider {
    /// Create a cuObj RDMA endpoint.
    ///
    /// :param role: ``"both"`` (default), ``"client"``, or ``"server"``.
    /// :param debug: emit trace lines to stderr (default ``False``).
    /// :param listen_addr: RDMA listen address for the server role (cuObj server).
    /// :param listen_port: RDMA listen port for the server role (cuObj server).
    /// :param pool_count: number of pre-allocated buffer pool slots (default 4).
    /// :param buf_size: size of each buffer pool slot in bytes (default 128 MiB).
    /// :raises RuntimeError: if the cuObj plugin is unavailable or fails to initialise.
    #[new]
    #[pyo3(signature = (
        role = "both",
        *,
        debug = false,
        listen_addr = "",
        listen_port = 0u16,
        pool_count = 4usize,
        buf_size = 134217728usize,
    ))]
    fn new(
        role:        &str,
        debug:       bool,
        listen_addr: &str,
        listen_port: u16,
        pool_count:  usize,
        buf_size:    usize,
    ) -> PyResult<PyClassInitializer<Self>> {
        let base = PyRdmaProvider::new(
            "cuobj", role, debug, "/dev/shm",
            listen_addr, listen_port, pool_count, buf_size,
        )?;
        Ok(PyClassInitializer::from(base).add_subclass(PyCuObjProvider))
    }
}

// ── Module ────────────────────────────────────────────────────────────────────

/// Returns a one-line summary of available RDMA providers.
#[pyfunction]
fn rdma_provider_info() -> String {
    crate::rdma_provider_info()
}

#[pymodule]
fn s3_rdma(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyRdmaProvider>()?;
    m.add_class::<PyMockRdmaProvider>()?;
    m.add_class::<PyCuObjProvider>()?;
    m.add_function(wrap_pyfunction!(rdma_provider_info, m)?)?;
    Ok(())
}
