//! Structured RDMA token abstraction.
//!
//! An [`RdmaToken`] wraps the raw bytes of an `x-amz-rdma-token` header value
//! and exposes the provider name, buffer offset, and buffer capacity extracted
//! from the token where the format is known.
//!
//! ## Token formats
//!
//! | Provider  | Format / Example | Parsed |
//! |-----------|-----------------|--------|
//! | `"mock"`  | `RDMA_PUT_rdma-1-abc_536870912_0_key_b/k` | Full |
//! | `"cuobj"` | `7fab00000000:20000000:deadbeef:…`        | vaddr + size |
//! | `""`      | binary / unknown                           | size = 0, offset = 0 |
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
/// | `provider` | Which provider generated the token (`"mock"`, `"cuobj"`, or `""` for unknown). |
/// | `token`    | Raw bytes passed to `handle_put` / `handle_get`. |
/// | `offset`   | Byte offset within the client buffer this instance covers. |
/// | `size`     | Byte count this instance covers (`0` = unknown / single-shot). |
///
/// Use [`slice`](Self::slice) to derive a sub-range token for chunked transfers,
/// and [`split`](Self::split) to partition a large token into equal-sized slices.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdmaToken {
    /// Inferred provider: `"mock"`, `"cuobj"`, or `""` for unrecognised formats.
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
    /// Tries mock format first, then cuObj format.  Falls back to
    /// `provider = ""`, `size = 0`, `offset = 0` for unrecognised tokens.
    pub fn parse(raw: &[u8]) -> Self {
        let (provider, offset, size) =
            std::str::from_utf8(raw).ok()
                .and_then(|s| Self::parse_mock_str(s).or_else(|| Self::parse_cuobj_str(s)))
                .unwrap_or_default();
        Self { provider, token: raw.to_vec(), offset, size }
    }

    /// Convenience constructor from a string slice.
    pub fn from_str(s: &str) -> Self {
        Self::parse(s.as_bytes())
    }

    /// Parse the `|`-separated multi-token header into individual tokens.
    ///
    /// Each token's size is read from its embedded encoding.  Pass an empty
    /// string (or use [`from_str`](Self::from_str)) for single-token requests.
    ///
    /// # Example
    /// ```rust
    /// # use s3_rdma::RdmaToken;
    /// let toks = RdmaToken::parse_multi(
    ///     "RDMA_PUT_a_256_0_key_b/k|RDMA_PUT_b_128_0_key_b/k",
    /// );
    /// assert_eq!(toks.len(), 2);
    /// assert_eq!(toks[0].size(), 256);
    /// assert_eq!(toks[1].size(), 128);
    /// ```
    pub fn parse_multi(tokens_header: &str) -> Vec<Self> {
        tokens_header
            .split(RDMA_TOKEN_SEPARATOR)
            .map(Self::from_str)
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

    /// Inferred provider name: `"mock"`, `"cuobj"`, or `""` for unrecognised tokens.
    pub fn provider(&self) -> &str {
        &self.provider
    }

    /// Byte offset within the client buffer this instance covers.
    pub fn offset(&self) -> usize {
        self.offset
    }

    /// Byte count this instance covers.
    ///
    /// Returns `0` for unrecognised tokens where the size cannot be
    /// determined without a hardware call; callers should then use
    /// single-shot semantics (`handle_put(key, raw(), 0, 0)`).
    pub fn size(&self) -> usize {
        self.size
    }

    /// `true` when the token is unrecognised and neither `size` nor `offset`
    /// can be determined without calling into the RDMA hardware.
    pub fn is_opaque(&self) -> bool {
        self.provider.is_empty()
    }

    /// Base virtual address encoded in the raw token.
    ///
    /// For cuObj tokens (`provider == "cuobj"`) this is the client buffer's
    /// remote virtual address.  Adding [`offset`](Self::offset) gives the
    /// `remote_start` pointer required by `cuobj_srv_handle_put` /
    /// `cuobj_srv_handle_get`.
    ///
    /// Returns `0` for mock and unrecognised tokens.
    pub fn remote_addr(&self) -> usize {
        self.as_str()
            .and_then(|s| s.split(':').next())
            .and_then(|hex| usize::from_str_radix(hex, 16).ok())
            .unwrap_or(0)
    }

    /// Full buffer capacity as encoded in the raw token bytes.
    ///
    /// For a sliced token (`slice(off, sz)`), this is the capacity of the
    /// *original* buffer, not the slice size.
    ///
    /// - Mock tokens: the `_{size}_` field (decimal).
    /// - cuObj tokens: the second `:` field (hexadecimal).
    /// - Unknown tokens: `0`.
    pub fn total_buffer_size(&self) -> usize {
        std::str::from_utf8(&self.token).ok()
            .and_then(|s| {
                Self::parse_mock_str(s).map(|(_, _, sz)| sz)
                    .or_else(|| Self::parse_cuobj_str(s).map(|(_, _, sz)| sz))
            })
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

    /// Parse a cuObj RDMA descriptor string `<vaddr_hex>:<size_hex>:<rkey_hex>:…`.
    ///
    /// Returns `(provider, offset=0, size)` on success, `None` otherwise.
    /// The `offset` is always `0` on construction; use [`slice`](Self::slice)
    /// to adjust it for chunked transfers.
    fn parse_cuobj_str(s: &str) -> Option<(String, usize, usize)> {
        // Require at least three colon-separated fields.
        let mut fields = s.splitn(4, ':');
        let vaddr_hex = fields.next()?;
        let size_hex  = fields.next()?;
        let _rkey     = fields.next()?; // must be present
        // Both vaddr and size must parse as hexadecimal.
        usize::from_str_radix(vaddr_hex.trim(), 16).ok()?;
        let size = usize::from_str_radix(size_hex.trim(), 16).ok()?;
        Some(("cuobj".to_owned(), 0, size))
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
            std::str::from_utf8(&token).ok()
                .and_then(|s| Self::parse_mock_str(s).or_else(|| Self::parse_cuobj_str(s)))
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
    fn parse_multi_from_tokens() {
        let toks = RdmaToken::parse_multi(
            "RDMA_PUT_rdma-a_256_0_key_b/k|RDMA_PUT_rdma-b_128_0_key_b/k",
        );
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].size(), 256);
        assert_eq!(toks[1].size(), 128);
    }

    #[test]
    fn parse_multi_opaque_tokens() {
        // Tokens with fewer than 3 colon-separated fields → opaque (size = 0).
        let toks = RdmaToken::parse_multi("unknown-binary-blob|another-blob");
        assert_eq!(toks.len(), 2);
        assert_eq!(toks[0].size(), 0);
        assert_eq!(toks[1].size(), 0);
    }

    // ── cuObj token tests ─────────────────────────────────────────────────────

    const CUOBJ_TOK: &str = "7fab000000000000:20000000:deadbeef0000:extra";

    #[test]
    fn parse_cuobj_basic() {
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert_eq!(tok.provider(), "cuobj");
        assert_eq!(tok.size(), 0x20000000);  // 512 MiB
        assert_eq!(tok.offset(), 0);
        assert!(!tok.is_opaque());
    }

    #[test]
    fn parse_cuobj_remote_addr() {
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert_eq!(tok.remote_addr(), 0x7fab000000000000usize);
    }

    #[test]
    fn parse_cuobj_total_buffer_size() {
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert_eq!(tok.total_buffer_size(), 0x20000000);
        // After slicing, total_buffer_size still reflects the full token size.
        let sl = tok.slice(0x10000000, 0x10000000);
        assert_eq!(sl.total_buffer_size(), 0x20000000);
        assert_eq!(sl.size(), 0x10000000);
    }

    #[test]
    fn parse_cuobj_split() {
        let tok = RdmaToken::from_str(CUOBJ_TOK); // 512 MiB
        let chunk = 128 * 1024 * 1024; // 128 MiB
        let slices = tok.split(chunk);
        assert_eq!(slices.len(), 4);
        assert_eq!(slices[0].offset(), 0);
        assert_eq!(slices[1].offset(), chunk);
        assert_eq!(slices[3].offset(), 3 * chunk);
        assert_eq!(slices[3].size(), chunk);
        // raw bytes unchanged across all slices
        for sl in &slices {
            assert_eq!(sl.raw(), tok.raw());
            assert_eq!(sl.provider(), "cuobj");
        }
    }

    #[test]
    fn cuobj_not_confused_with_opaque() {
        // Must not accidentally be treated as opaque (provider == "").
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert!(!tok.is_opaque());
        assert_eq!(tok.provider(), "cuobj");
    }

    #[test]
    fn opaque_non_cuobj_still_empty_provider() {
        // A string with colons but non-hex first field → opaque.
        let tok = RdmaToken::from_str("not-hex:10000:rkey:extra");
        assert_eq!(tok.provider(), "");
        assert!(tok.is_opaque());
        assert_eq!(tok.size(), 0);
    }

    #[test]
    fn cuobj_remote_addr_with_offset() {
        // remote_addr() returns the base vaddr regardless of slice offset.
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        let sl = tok.slice(0x1000, 0x1000000);
        assert_eq!(sl.remote_addr(), 0x7fab000000000000usize);
        assert_eq!(sl.offset(), 0x1000);
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
