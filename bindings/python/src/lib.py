"""
PyO3 extension module — GPU-Direct RDMA provider for S3 (Rust → Python).

Build with:
    maturin develop --release     # editable install
    maturin build  --release      # produce .whl

This file is compiled by maturin/PyO3 into a native .so extension.
The s3-rdma Cargo.toml needs a `pyo3` feature that adds:

    [features]
    pyo3 = ["dep:pyo3"]

    [dependencies]
    pyo3 = { version = "0.21", features = ["extension-module"], optional = true }
"""

# The real implementation lives in ../s3-rdma/src/python.rs (to be created).
# Placeholder so maturin can locate the python-source directory.
