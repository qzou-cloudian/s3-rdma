//! Structured RDMA token abstraction.
//!
//! An [`RdmaToken`] wraps the raw bytes of an `x-amz-rdma-token` header value
//! and exposes the provider name, buffer offset, and buffer capacity extracted
//! from the token where the format is known.
//!
//! ## Token formats
//!
//! | Provider | Example | Parsed |
//! |----------|---------|--------|
//! | `"mock"` | `RDMA_PUT_rdma-12345-7fab_1073741824_0_key_b/k` | Full |
//! | `""` (opaque) | `<cuObj binary descriptor>` | size = 0, offset = 0 |
//!
//! ## Common workflows
//!
//! ### Server: detect whether to loop or single-shot
//!
//! ```rust
//! # use s3_rdma::RdmaToken;
//! # fn buf_size() -> usize { 128 * 1024 * 1024 }
//! let tok = RdmaToken::parse(b"RDMA_PUT_rdma-1-abc_536870912_0_key_b/k");
//! let server_slot = buf_size();   // e.g. 128 MiB
//! if tok.size() > server_slot {
//!     // multi-chunk: tok.split(server_slot) gives Vec<RdmaToken> covering [0..512 MiB)
//!     for slice in tok.split(server_slot) {
//!         // handle_put(key, slice.raw(), slice.offset(), slice.size())
//!     }
//! }
//! ```
//!
//! ### Client: split a large buffer into protocol chunks
//!
//! ```rust
//! # use s3_rdma::RdmaToken;
//! let tok = RdmaToken::parse(b"RDMA_PUT_rdma-1-abc_1073741824_0_key_b/k"); // 1 GiB
//! let max_transfer = 256 * 1024 * 1024; // 256 MiB
//! let chunks: Vec<RdmaToken> = tok.split(max_transfer);
//! assert_eq!(chunks.len(), 4);
//! assert_eq!(chunks[2].offset(), 512 * 1024 * 1024);
//! assert_eq!(chunks[2].size(),   256 * 1024 * 1024);
//! ```

use crate::provider::RDMA_TOKEN_SEPARATOR;

/// A structured RDMA transfer descriptor.
///
/// Wraps the raw `x-amz-rdma-token` bytes with parsed metadata:
///
/// | Field      | Meaning |
/// |------------|---------|
/// | `provider` | Which provider generated the token (`"mock"` or `""` for opaque). |
/// | `token`    | Raw bytes passed to `handle_put` / `handle_get`. |
/// | `offset`   | Byte offset within the client buffer this instance covers. |
/// | `size`     | Byte count this instance covers (`0` = unknown / single-shot). |
///
/// Use [`slice`](Self::slice) to derive a sub-range token for chunked transfers,
/// and [`split`](Self::split) to partition a large token into equal-sized slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdmaToken {
    /// Inferred provider (`"mock"` or `""` for opaque cuObj tokens).
    provider: String,
    /// Raw token bytes (the `x-amz-rdma-token` header value).
    token: Vec<u8>,
    /// Byte offset within the client buffer this instance covers.
    offset: usize,
    /// Byte count this instance covers (`0` = unknown / use single-shot).
    size: usize,
}

impl RdmaToken {
    // ── Constructors ─────────────────────────────────────────────────────────

    /// Parse raw token bytes into a structured descriptor.
    ///
    /// Mock tokens (`RDMA_{PUT|GET}_…`) are fully parsed; all other formats
    /// are treated as opaque with `size = 0` and `offset = 0`.
    pub fn parse(raw: &[u8]) -> Self {
        let (provider, offset, size) =
            std::str::from_utf8(raw).ok().and_then(Self::parse_mock_str)
                .unwrap_or_default();
        Self { provider, token: raw.to_vec(), offset, size }
    }

    /// Convenience constructor from a string slice.
    pub fn from_str(s: &str) -> Self {
        Self::parse(s.as_bytes())
    }

    /// Parse the `|`-separated multi-token header into individual tokens,
    /// optionally pairing each with a per-token size from `sizes_header`.
    ///
    /// Sizes from `sizes_header` override the token-embedded sizes where
    /// present.  Pass an empty string for `sizes_header` when the
    /// `x-amz-rdma-sizes` header is absent.
    ///
    /// # Example
    /// ```rust
    /// # use s3_rdma::RdmaToken;
    /// let toks = RdmaToken::parse_multi(
    ///     "RDMA_PUT_a_256_0_key_b/k|RDMA_PUT_b_128_0_key_b/k",
    ///     "256|128",
    /// );
    /// assert_eq!(toks.len(), 2);
    /// assert_eq!(toks[0].size(), 256);
    /// assert_eq!(toks[1].size(), 128);
    /// ```
    pub fn parse_multi(tokens_header: &str, sizes_header: &str) -> Vec<Self> {
        let raw_toks: Vec<&str> = tokens_header.split(RDMA_TOKEN_SEPARATOR).collect();
        let sizes: Vec<usize> = if sizes_header.is_empty() {
            vec![]
        } else {
            sizes_header
                .split(RDMA_TOKEN_SEPARATOR)
                .filter_map(|s| s.trim().parse().ok())
                .collect()
        };
        raw_toks
            .iter()
            .enumerate()
            .map(|(i, raw)| {
                let mut tok = Self::from_str(raw);
                // Explicit sizes header takes precedence over the token-embedded size.
                if let Some(&sz) = sizes.get(i) {
                    tok.size = sz;
                }
                tok
            })
            .collect()
    }

    // ── Field accessors ───────────────────────────────────────────────────────

    /// Raw token bytes (value of the `x-amz-rdma-token` header).
    pub fn raw(&self) -> &[u8] {
        &self.token
    }

    /// Token as a UTF-8 string, or `None` for binary (cuObj) tokens.
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.token).ok()
    }

    /// Inferred provider name (`"mock"`) or empty string for opaque tokens.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Byte offset within the client buffer this instance covers.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Byte count this instance covers.
    ///
    /// Returns `0` for opaque (cuObj) tokens where the size cannot be
    /// determined without a hardware call; callers should then use
    /// single-shot semantics (`handle_put(key, raw(), 0, 0)`).
    pub fn size(&self) -> usize {
        self.size
    }

    /// `true` when the token is opaque and neither `size` nor `offset` can be
    /// determined without calling into the RDMA hardware.
    pub fn is_opaque(&self) -> bool {
        self.provider.is_empty()
    }

    /// Full buffer capacity as encoded in the raw token bytes.
    ///
    /// For a sliced token (`slice(off, sz)`), this is the capacity of the
    /// *original* buffer, not the slice size.  Returns `0` for opaque tokens.
    ///
    /// For mock tokens this equals the `_{size}_` field regardless of any
    /// slicing that has been applied to this instance.
    pub fn total_buffer_size(&self) -> usize {
        std::str::from_utf8(&self.token).ok()
            .and_then(Self::parse_mock_str)
            .map(|(_, _, sz)| sz)
            .unwrap_or(0)
    }

    // ── Slicing / splitting ───────────────────────────────────────────────────

    /// Create a new `RdmaToken` covering the sub-range
    /// `[offset, offset + size)` of this instance's buffer.
    ///
    /// The raw token bytes are unchanged — `offset` and `size` become the
    /// arguments to `handle_put`/`handle_get`, directing the provider to
    /// read/write exactly that window of the client buffer.
    ///
    /// # Example
    /// ```rust
    /// # use s3_rdma::RdmaToken;
    /// let tok = RdmaToken::parse(b"RDMA_PUT_rdma-1-abc_512_0_key_b/k");
    /// let sl = tok.slice(256, 128);
    /// assert_eq!(sl.offset(), 256);
    /// assert_eq!(sl.size(), 128);
    /// // raw bytes unchanged — still points to the same buffer descriptor:
    /// assert_eq!(sl.raw(), tok.raw());
    /// ```
    pub fn slice(&self, offset: usize, size: usize) -> Self {
        Self {
            provider: self.provider.clone(),
            token: self.token.clone(),
            offset,
            size,
        }
    }

    /// Partition this token into slices of at most `chunk_size` bytes each,
    /// covering `[self.offset(), self.offset() + self.size())` in order.
    ///
    /// Returns `vec![self.clone()]` unchanged when:
    /// - `self.size() == 0` — unknown size (opaque token); caller must
    ///   use single-shot or loop-until-EOF separately.
    /// - `chunk_size == 0`.
    /// - `self.size() <= chunk_size` — already fits in one slot.
    ///
    /// # Example
    /// ```rust
    /// # use s3_rdma::RdmaToken;
    /// let tok = RdmaToken::parse(b"RDMA_PUT_rdma-1-abc_512_0_key_b/k");
    /// let slices = tok.split(128);
    /// assert_eq!(slices.len(), 4);
    /// assert_eq!(slices[0].offset(), 0);
    /// assert_eq!(slices[1].offset(), 128);
    /// assert_eq!(slices[3].offset(), 384);
    /// assert_eq!(slices[3].size(), 128);
    /// ```
    pub fn split(&self, chunk_size: usize) -> Vec<Self> {
        if self.size == 0 || chunk_size == 0 || self.size <= chunk_size {
            return vec![self.clone()];
        }
        let end = self.offset + self.size;
        let mut out = Vec::with_capacity((self.size + chunk_size - 1) / chunk_size);
        let mut off = self.offset;
        while off < end {
            let sz = chunk_size.min(end - off);
            out.push(self.slice(off, sz));
            off += sz;
        }
        out
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    /// Parse mock token string `RDMA_{PUT|GET}_{shm}_{size}_{offset}_key_{key}`.
    ///
    /// Returns `(provider, offset, size)` on success, `None` otherwise.
    fn parse_mock_str(s: &str) -> Option<(String, usize, usize)> {
        let mut parts = s.splitn(7, '_');
        if parts.next()? != "RDMA" { return None; }
        let op = parts.next()?;
        if op != "PUT" && op != "GET" { return None; }
        let _shm  = parts.next()?;                              // shm-name (may have hyphens)
        let size: usize  = parts.next()?.parse().ok()?;
        let offset: usize = parts.next()?.parse().ok()?;
        // The remaining "_key_{s3-key}" segment must be present.
        let kw = parts.next()?;
        if kw != "key" { return None; }
        Some(("mock".to_owned(), offset, size))
    }
}

// ── Standard trait impls ──────────────────────────────────────────────────────

impl From<&[u8]> for RdmaToken {
    fn from(raw: &[u8]) -> Self { Self::parse(raw) }
}

impl From<&str> for RdmaToken {
    fn from(s: &str) -> Self { Self::from_str(s) }
}

/// Parses without cloning: the raw bytes are moved into the token.
impl From<Vec<u8>> for RdmaToken {
    fn from(token: Vec<u8>) -> Self {
        let (provider, offset, size) =
            std::str::from_utf8(&token).ok().and_then(Self::parse_mock_str)
                .unwrap_or_default();
        Self { provider, token, offset, size }
    }
}

impl std::fmt::Display for RdmaToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.as_str() {
            Some(s) => write!(f, "{s}"),
            None    => write!(f, "<binary token {} bytes>", self.token.len()),
        }
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const MOCK_PUT: &str = "RDMA_PUT_rdma-12345-7fab12340000_1073741824_0_key_bucket/key";
    const MOCK_GET: &str = "RDMA_GET_rdma-67890-dead_268435456_4096_key_other/obj";

    #[test]
    fn parse_mock_put() {
        let tok = RdmaToken::from_str(MOCK_PUT);
        assert_eq!(tok.provider(), "mock");
        assert_eq!(tok.size(),   1073741824);
        assert_eq!(tok.offset(), 0);
        assert!(!tok.is_opaque());
        assert_eq!(tok.total_buffer_size(), 1073741824);
    }

    #[test]
    fn parse_mock_get() {
        let tok = RdmaToken::from_str(MOCK_GET);
        assert_eq!(tok.provider(), "mock");
        assert_eq!(tok.size(),   268435456);
        assert_eq!(tok.offset(), 4096);
    }

    #[test]
    fn parse_opaque() {
        let tok = RdmaToken::parse(b"\x00\x01\x02opaque_cuobj");
        assert_eq!(tok.provider(), "");
        assert_eq!(tok.size(), 0);
        assert_eq!(tok.offset(), 0);
        assert!(tok.is_opaque());
    }

    #[test]
    fn parse_opaque_ascii_not_rdma() {
        // A plain string that doesn't start with "RDMA_" is treated as opaque.
        let tok = RdmaToken::from_str("cuobj:abcdef1234");
        assert!(tok.is_opaque());
        assert_eq!(tok.size(), 0);
    }

    #[test]
    fn slice_preserves_raw() {
        let tok = RdmaToken::from_str(MOCK_PUT);
        let sl  = tok.slice(128, 64);
        assert_eq!(sl.raw(), tok.raw());   // same underlying descriptor
        assert_eq!(sl.offset(), 128);
        assert_eq!(sl.size(),   64);
        assert_eq!(sl.provider(), "mock");
        // total_buffer_size still reflects the full buffer, not the slice.
        assert_eq!(sl.total_buffer_size(), 1073741824);
    }

    #[test]
    fn split_equal_chunks() {
        let tok = RdmaToken::from_str("RDMA_PUT_rdma-1-abc_512_0_key_b/k");
        let slices = tok.split(128);
        assert_eq!(slices.len(), 4);
        for (i, sl) in slices.iter().enumerate() {
            assert_eq!(sl.offset(), i * 128);
            assert_eq!(sl.size(),   128);
            assert_eq!(sl.raw(),    tok.raw());
        }
    }

    #[test]
    fn split_uneven_last_chunk() {
        let tok = RdmaToken::from_str("RDMA_PUT_rdma-1-abc_300_0_key_b/k");
        let slices = tok.split(128);
        assert_eq!(slices.len(), 3);
        assert_eq!(slices[0].size(), 128);
        assert_eq!(slices[1].size(), 128);
        assert_eq!(slices[2].size(),  44); // 300 - 256
        assert_eq!(slices[2].offset(), 256);
    }

    #[test]
    fn split_fits_in_one() {
        let tok = RdmaToken::from_str("RDMA_PUT_rdma-1-abc_64_0_key_b/k");
        let slices = tok.split(128);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].offset(), 0);
        assert_eq!(slices[0].size(), 64);
    }

    #[test]
    fn split_opaque_returns_self() {
        let tok = RdmaToken::parse(b"\xde\xad\xbe\xef");
        let slices = tok.split(128);
        assert_eq!(slices.len(), 1);
        assert_eq!(slices[0].size(), 0); // unchanged
    }

    #[test]
    fn split_from_nonzero_offset() {
        // A sliced token covering bytes [256..512) of a 512-byte buffer.
        let tok = RdmaToken::from_str("RDMA_PUT_rdma-1-abc_512_0_key_b/k");
        let half = tok.slice(256, 256);
        let slices = half.split(128);
        assert_eq!(slices.len(), 2);
        assert_eq!(slices[0].offset(), 256);
        assert_eq!(slices[1].offset(), 384);
    }

    #[test]
    fn parse_multi_no_sizes() {
        let toks = RdmaToken::parse_multi(
            "RDMA_PUT_rdma-a_256_0_key_b/k|RDMA_PUT_rdma-b_128_0_key_b/k",
            "",
        );
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].size(), 256);
        assert_eq!(toks[1].size(), 128);
    }

    #[test]
    fn parse_multi_with_sizes_override() {
        // sizes header overrides token-embedded sizes.
        let toks = RdmaToken::parse_multi(
            "RDMA_PUT_rdma-a_1073741824_0_key_b/k|RDMA_PUT_rdma-b_1073741824_0_key_b/k",
            "268435456|134217728",
        );
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].size(), 268435456);
        assert_eq!(toks[1].size(), 134217728);
    }

    #[test]
    fn display_mock() {
        let tok = RdmaToken::from_str(MOCK_PUT);
        assert_eq!(tok.to_string(), MOCK_PUT);
    }

    #[test]
    fn display_binary() {
        let tok = RdmaToken::parse(b"\x80\x81\x82"); // invalid UTF-8
        assert!(tok.to_string().contains("binary token 3 bytes"));
    }

    #[test]
    fn from_vec_no_alloc() {
        let raw = b"RDMA_PUT_rdma-1-abc_512_0_key_b/k".to_vec();
        let tok = RdmaToken::from(raw.clone());
        assert_eq!(tok.raw(), raw.as_slice());
        assert_eq!(tok.size(), 512);
    }
}
