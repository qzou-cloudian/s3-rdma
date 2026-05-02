//! Python bindings for `s3-rdma` via PyO3.
//!
//! Exposes [`MockRdmaEndpoint`] as `s3_rdma.MockRdmaEndpoint` to Python.
//! Build with maturin from the `python/` directory:
//!
//! ```sh
//! cd python && python3 -m maturin develop
//! ```

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::mock::MockRdmaEndpoint;
use crate::endpoint::RdmaEndpoint;

// ── MockRdmaEndpoint Python class ─────────────────────────────────────────────

/// Mock RDMA endpoint — no NVIDIA hardware required.
///
/// Generates deterministic token strings suitable for testing the S3 RDMA
/// upload/download path end-to-end without physical RDMA hardware.
///
/// Example::
///
///     import s3_rdma
///     p = s3_rdma.MockRdmaEndpoint(debug=True)
///     token = p.prepare_put_token(data, 0, len(data), b"bucket/key")
///     p.process_rdma_reply(200, token, len(data), lambda code, reply: print(code))
#[pyclass(name = "MockRdmaEndpoint")]
pub struct PyMockRdmaEndpoint {
    inner: MockRdmaEndpoint,
}

#[pymethods]
impl PyMockRdmaEndpoint {
    /// Create a new MockRdmaEndpoint.
    ///
    /// :param debug: if ``True``, print trace lines to stderr.
    #[new]
    #[pyo3(signature = (debug = false))]
    fn new(debug: bool) -> Self {
        Self { inner: MockRdmaEndpoint::new(debug, "/dev/shm", 0) }
    }

    /// Human-readable endpoint name.
    fn name(&self) -> &str {
        self.inner.name()
    }

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
        py: Python<'py>,
        data: &Bound<'py, PyBytes>,
        offset: usize,
        size: usize,
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
        py: Python<'py>,
        data: &Bound<'py, PyBytes>,
        offset: usize,
        size: usize,
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
    /// :param rdma_status:    numeric status from the response header (200/204/206 = success, 501 = fallback).
    /// :param request_token:  the original request token bytes.
    /// :param transferred:    number of bytes transferred (from ``x-amz-rdma-bytes``).
    /// :param callback:       callable invoked with ``(error_code, reply_token)``.
    fn process_rdma_reply(
        &self,
        rdma_status: u16,
        request_token: &[u8],
        transferred: usize,
        callback: PyObject,
    ) -> PyResult<()> {
        self.inner
            .process_rdma_reply(
                rdma_status,
                request_token,
                transferred,
                Box::new(move |result| {
                    Python::with_gil(|py| {
                        let _ = callback.call1(py, (result.error_code, result.reply_token.as_slice()));
                    });
                }),
            )
            .map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> String {
        format!("MockRdmaEndpoint(name={})", self.inner.name())
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
    m.add_class::<PyMockRdmaEndpoint>()?;
    m.add_function(wrap_pyfunction!(rdma_provider_info, m)?)?;
    Ok(())
}
