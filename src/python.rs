//! Python bindings for `s3-rdma` via PyO3.
//!
//! Exposes [`RdmaProvider`] and [`MockRdmaProvider`] to Python.
//! Build with maturin from the `bindings/python/` directory:
//!
//! ```sh
//! cd bindings/python && python3 -m maturin develop
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::PyClassInitializer;
use pyo3::types::PyBytes;

use crate::provider::{
    InflightTransfers, PendingTransfer,
    RdmaRole, RdmaServerConfig, RdmaServerChannelExt, TelemetryConfig,
    DEFAULT_RDMA_BUF_SIZE,
};
use crate::TelemLevel;

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

/// Unified RDMA provider supporting client, server, or both roles.
///
/// Wraps the Rust ``RdmaProvider`` trait and supports all provider types
/// (``"mock"``, ``"cuobj"``, ``"auto"``) and roles (``"client"``,
/// ``"server"``, ``"both"``).
///
/// Example (client role)::
///
///     import s3_rdma
///     ep = s3_rdma.RdmaProvider(provider="mock", role="client")
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     ep.process_rdma_reply(200, token, len(data), lambda code, reply: None)
///
/// Example (server role)::
///
///     ep = s3_rdma.RdmaProvider(provider="mock", role="server")
///     received = ep.handle_put(b"bucket/key", token, size=len(data))
///     written  = ep.handle_get(b"bucket/key", received, token)
#[pyclass(name = "RdmaProvider", subclass)]
pub struct PyRdmaProvider {
    inner:   Arc<dyn crate::provider::RdmaProvider>,
    /// Per-provider registry of in-flight RDMA transfers, keyed by token bytes.
    inflight: InflightTransfers,
    /// Per-token destination buffer pointers for GET operations.
    get_buffers: Mutex<HashMap<Vec<u8>, (*mut u8, usize)>>,
}

#[pymethods]
impl PyRdmaProvider {
    /// Create an RDMA provider.
    ///
    /// :param provider: ``"mock"`` (default), ``"cuobj"``, or ``"auto"``.
    /// :param role: ``"both"`` (default), ``"client"``, or ``"server"``.
    /// :param debug: emit trace lines to stderr (default ``False``).
    /// :param shm_path: SHM directory for the mock provider (default ``"/dev/shm"``).
    /// :param listen_addr: RDMA listen address for the cuObj server.
    /// :param listen_port: RDMA listen port for the cuObj server.
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
        buf_size = DEFAULT_RDMA_BUF_SIZE,
    ))]
    pub fn new(
        provider:    &str,
        role:        &str,
        debug:       bool,
        shm_path:    &str,
        listen_addr: &str,
        listen_port: u16,
        buf_size:    usize,
    ) -> PyResult<Self> {
        let provider_role = parse_role(role)?;
        let cfg = RdmaServerConfig {
            shm_path:       shm_path.to_owned(),
            listen_addr:    listen_addr.to_owned(),
            listen_port,
            buf_size,
        };
        crate::create_provider(provider, debug, provider_role, Some(&cfg))
            .map(|inner| Self {
                inner,
                inflight: InflightTransfers::new(),
                get_buffers: Mutex::new(HashMap::new()),
            })
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    // ── Info ─────────────────────────────────────────────────────────────────

    /// Human-readable provider name (e.g. ``"MockRdmaProvider"``).
    fn name(&self) -> &str { self.inner.name() }

    /// Transport protocol: ``"mock"`` or ``"cuobj"``.
    fn protocol(&self) -> &str { self.inner.protocol() }

    /// ``True`` if this provider supports client-side operations.
    fn supports_client(&self) -> bool { self.inner.supports_client() }

    /// ``True`` if this provider supports server-side operations.
    fn supports_server(&self) -> bool { self.inner.supports_server() }

    /// Capacity in bytes of the server's per-slot transfer buffer.
    fn buf_size(&self) -> usize {
        self.inner.buf_size()
    }

    // ── Client methods ────────────────────────────────────────────────────────

    /// Returns ``True`` — the channel API validates suitability at bind time.
    fn is_memory_suitable(&self, _ptr: u64, _size: usize) -> bool { true }

    /// No-op — memory registration is handled internally by
    /// :meth:`prepare_put_token` and :meth:`prepare_get_token` when they
    /// allocate and bind a channel buffer.
    ///
    /// :param data: ignored (accepted for backward compatibility).
    fn register_memory(&self, _data: &Bound<'_, PyAny>) -> PyResult<()> { Ok(()) }

    /// No-op — channel resources are released automatically when
    /// :meth:`process_rdma_reply` consumes the pending transfer.
    ///
    /// :param data: ignored (accepted for backward compatibility).
    fn deregister_memory(&self, _data: &Bound<'_, PyAny>) -> PyResult<()> { Ok(()) }

    /// Generate an RDMA token for an S3 PUT (upload).
    ///
    /// Allocates an internal RDMA-registered buffer, copies ``data`` into it,
    /// and returns the opaque token.  The pending transfer is stored internally
    /// and consumed when :meth:`process_rdma_reply` is called.
    ///
    /// Accepts any buffer-protocol object for ``data``.
    ///
    /// :param data:   buffer containing the data to upload.
    /// :param offset: byte offset within the buffer for this sub-request.
    /// :param size:   number of bytes to transfer.
    /// :param s3_key: the S3 object key (as ``bytes``).
    /// :returns: opaque token to attach as ``x-amz-rdma-token``.
    fn prepare_put_token<'py>(
        &self,
        py:     Python<'py>,
        data:   &Bound<'_, PyAny>,
        offset: usize,
        size:   usize,
        s3_key: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let buf = PyBuffer::<u8>::get_bound(data)?;
        let data_ptr = buf.buf_ptr() as *const u8;
        let data_len = buf.len_bytes();

        let ch = self.inner.bind(data_len, s3_key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        // Copy the caller's data into the channel's registered buffer.
        let copy_len = data_len.min(ch.size());
        if copy_len > 0 {
            unsafe {
                std::ptr::copy_nonoverlapping(data_ptr, ch.ptr(), copy_len);
            }
        }

        let put_size = size.min(copy_len.saturating_sub(offset));
        let put_handle = ch.prepare_put(offset, put_size)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let token = put_handle.token().to_vec();
        self.inflight.insert(token.clone(), PendingTransfer::new(ch, put_handle));

        Ok(PyBytes::new_bound(py, &token))
    }

    /// Generate an RDMA token for an S3 GET (download).
    ///
    /// Allocates an internal RDMA-registered buffer and returns the opaque
    /// token.  After the server fills the buffer via ``handle_get``, call
    /// :meth:`process_rdma_reply` to copy the received data back into ``data``.
    ///
    /// Pass a writable buffer (``bytearray`` or similar) so that
    /// :meth:`process_rdma_reply` can write the received data back into it.
    ///
    /// :param data:   buffer that will receive the downloaded data.
    /// :param offset: byte offset within the buffer for this sub-request.
    /// :param size:   number of bytes to transfer.
    /// :param s3_key: the S3 object key (as ``bytes``).
    /// :returns: opaque token to attach as ``x-amz-rdma-token``.
    fn prepare_get_token<'py>(
        &self,
        py:     Python<'py>,
        data:   &Bound<'_, PyAny>,
        offset: usize,
        size:   usize,
        s3_key: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let buf = PyBuffer::<u8>::get_bound(data)?;
        let dst_ptr = buf.buf_ptr() as *mut u8;
        let dst_len = buf.len_bytes();

        let ch = self.inner.bind(dst_len, s3_key)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let get_size = size.min(dst_len.saturating_sub(offset));
        let get_handle = ch.prepare_get(offset, get_size)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        let token = get_handle.token().to_vec();
        self.inflight.insert(token.clone(), PendingTransfer::new(ch, get_handle));
        self.get_buffers.lock().unwrap().insert(token.clone(), (dst_ptr, dst_len));

        Ok(PyBytes::new_bound(py, &token))
    }

    /// Process the ``x-amz-rdma-reply`` status returned by the S3 server.
    ///
    /// Completes the pending RDMA transfer associated with ``request_token``.
    /// For GET operations, copies the received data back into the buffer
    /// originally passed to :meth:`prepare_get_token`.
    /// Calls ``callback(error_code, reply_token)`` synchronously.
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
        let transfer = self.inflight.remove(request_token)
            .ok_or_else(|| PyRuntimeError::new_err(
                "no pending transfer for the given token; \
                 call prepare_put_token or prepare_get_token first"
            ))?;

        let get_buf = self.get_buffers.lock().unwrap().remove(request_token);
        if let Some((dst_ptr, dst_len)) = get_buf {
            let chan_ptr  = transfer.channel().ptr();
            let chan_size = transfer.channel().size();
            transfer.complete_get(rdma_status, transferred)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
            let n = transferred.min(dst_len).min(chan_size);
            if n > 0 {
                unsafe {
                    std::ptr::copy_nonoverlapping(chan_ptr, dst_ptr, n);
                }
            }
        } else {
            transfer.complete_put(rdma_status, transferred)
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
        }

        Python::with_gil(|py| {
            let _ = callback.call1(py, (0i32, PyBytes::new_bound(py, b"").as_ref()));
        });
        Ok(())
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
        let buf_cap = if size > 0 { size } else {
            self.inner.buf_size().clamp(4096, DEFAULT_RDMA_BUF_SIZE)
        };
        let mut received: Vec<u8> = Vec::with_capacity(buf_cap);

        let ch = self.inner.open_channel(s3_key, token)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        ch.put(offset, buf_cap, |data: &[u8]| {
            received.extend_from_slice(data);
            Ok(data.len())
        }).map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        Ok(PyBytes::new_bound(py, &received))
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
        let data_owned = data.to_vec();
        let data_len = data_owned.len();

        let ch = self.inner.open_channel(s3_key, token)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;

        ch.get(offset, data_len, move |buf: &mut [u8]| {
            let n = data_len.min(buf.len());
            buf[..n].copy_from_slice(&data_owned[..n]);
            Ok(n)
        }).map_err(|e| PyRuntimeError::new_err(e.to_string()))
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

    // ── Telemetry ────────────────────────────────────────────────────────────

    /// ``True`` if this provider supports telemetry.
    fn supports_telemetry(&self) -> bool {
        self.inner.supports_telemetry()
    }

    /// (Re-)configure the telemetry output.
    ///
    /// :param use_otel: ``True`` for OpenTelemetry backend, ``False`` for plain stream.
    /// :param log_file: optional path to a log file (plain stream mode; ``None`` = stdout).
    /// :param level: verbosity level (0=Off, 1=Error, 2=Info, 3=Debug; default 1).
    #[pyo3(signature = (use_otel = false, log_file = None, level = 1u32))]
    fn setup_telemetry(
        &self,
        use_otel: bool,
        log_file: Option<&str>,
        level: u32,
    ) -> PyResult<()> {
        let lvl = match level {
            0 => TelemLevel::Off,
            1 => TelemLevel::Error,
            2 => TelemLevel::Info,
            _ => TelemLevel::Debug,
        };
        let cfg = TelemetryConfig {
            use_otel,
            log_file: log_file.map(|s| s.to_owned()),
            level: lvl,
        };
        self.inner.setup_telemetry(&cfg)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Shut down telemetry for this provider.
    fn shutdown_telemetry(&self) -> PyResult<()> {
        self.inner.shutdown_telemetry()
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Set telemetry verbosity level.
    ///
    /// :param level: verbosity level (0=Off, 1=Error, 2=Info, 3=Debug).
    fn set_telem_level(&self, level: u32) -> PyResult<()> {
        let lvl = match level {
            0 => TelemLevel::Off,
            1 => TelemLevel::Error,
            2 => TelemLevel::Info,
            _ => TelemLevel::Debug,
        };
        self.inner.set_telem_level(lvl)
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

// ── MockRdmaProvider — convenience subclass with a simpler constructor ─────────

/// Mock RDMA provider — no NVIDIA hardware required.
///
/// Equivalent to ``RdmaProvider(provider="mock", role="both")`` with a
/// simpler constructor.  Both client and server roles are active.
///
/// Example::
///
///     import s3_rdma
///     ep = s3_rdma.MockRdmaProvider(debug=True)
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     received = ep.handle_put(b"bucket/key", token, size=len(data))
#[pyclass(name = "MockRdmaProvider", extends = PyRdmaProvider)]
pub struct PyMockRdmaProvider;

#[pymethods]
impl PyMockRdmaProvider {
    /// Create a mock provider.
    ///
    /// :param debug: if ``True``, print trace lines to stderr.
    #[new]
    #[pyo3(signature = (debug = false))]
    fn new(debug: bool) -> PyResult<PyClassInitializer<Self>> {
        let base = PyRdmaProvider::new("mock", "both", debug, "/dev/shm", "", 0, 0)?;
        Ok(PyClassInitializer::from(base).add_subclass(PyMockRdmaProvider))
    }
}

// ── CuObjProvider — convenience subclass for the cuObject RDMA provider ───────

/// CuObj RDMA provider — requires NVIDIA hardware and the cuObject SDK plugin.
///
/// Equivalent to ``RdmaProvider(provider="cuobj", role=role)`` with a
/// simpler constructor.  Raises ``RuntimeError`` if the cuObj plugin
/// (``libs3rdmaclient.so`` / ``libs3rdmaserver.so``) cannot be
/// found at runtime.
///
/// Example (client role)::
///
///     import s3_rdma
///     ep = s3_rdma.CuObjProvider(role="client")
///     token = ep.prepare_put_token(data, 0, len(data), b"bucket/key")
///     ep.process_rdma_reply(200, token, len(data), lambda code, reply: None)
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
    /// Create a cuObj RDMA provider.
    ///
    /// :param role: ``"both"`` (default), ``"client"``, or ``"server"``.
    /// :param debug: emit trace lines to stderr (default ``False``).
    /// :param listen_addr: RDMA listen address for the server role (cuObj server).
    /// :param listen_port: RDMA listen port for the server role (cuObj server).
    /// :param buf_size: size of each buffer pool slot in bytes (default 128 MiB).
    /// :raises RuntimeError: if the cuObj plugin is unavailable or fails to initialise.
    #[new]
    #[pyo3(signature = (
        role = "both",
        *,
        debug = false,
        listen_addr = "",
        listen_port = 0u16,
        buf_size = DEFAULT_RDMA_BUF_SIZE,
    ))]
    fn new(
        role:        &str,
        debug:       bool,
        listen_addr: &str,
        listen_port: u16,
        buf_size:    usize,
    ) -> PyResult<PyClassInitializer<Self>> {
        let base = PyRdmaProvider::new(
            "cuobj", role, debug, "/dev/shm",
            listen_addr, listen_port, buf_size,
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
