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
//! | `"mock"`  | `RDMA_PUT_rdma-1-abc_536870912_0_key_b/k`      | Full |
//! | `"cuobj"` | `7fab00000000:20000000:deadbeef:…`             | vaddr + size |
//! | `"cuobj"` | `7fab00000000:20000000:deadbeef:…:67108864:0`  | vaddr + ext size + ext offset |
//! | `""`      | binary / unknown                                | size = 0, offset = 0 |
//!
//! ### Extended cuObj format
//!
//! The cuObj SDK produces a fixed-size base descriptor string
//! (`<vaddr_hex>:<size_hex>:<rkey_hex>:…`).  To embed transfer-specific
//! `size` and `offset` without modifying the base format, two extra
//! colon-separated **decimal** fields are appended:
//!
//! ```text
//! <cuobj_base>:<decimal_size>:<decimal_offset>
//! ```
//!
//! Because the base descriptor is fixed-width, these trailing fields are
//! unambiguously detectable.  [`raw()`](RdmaToken::raw) always returns only
//! the base descriptor so the cuObj C SDK receives an unmodified token.
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
    /// Raw token bytes passed to the provider SDK (`handle_put` / `handle_get`).
    ///
    /// For cuObj tokens this is always the **base** descriptor (without any
    /// appended `:size:offset` extension), so the cuObj C SDK receives a clean
    /// token regardless of whether the extended format was used on the wire.
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
    /// Tries mock format first, then cuObj (with optional `:size:offset`
    /// extension). Falls back to `provider = ""`, `size = 0`, `offset = 0`
    /// for unrecognised tokens.
    ///
    /// For cuObj tokens with an extension, only the **base** descriptor is
    /// stored in the internal `token` field; `size` and `offset` come from
    /// the extension fields.
    pub fn parse(raw: &[u8]) -> Self {
        if let Ok(s) = std::str::from_utf8(raw) {
            if let Some((provider, offset, size)) = Self::parse_mock_str(s) {
                return Self { provider, token: raw.to_vec(), offset, size };
            }
            let (base, ext_size, ext_offset) = Self::split_cuobj_extension(s);
            if let Some(base_size) = Self::parse_cuobj_base(base) {
                return Self {
                    provider: "cuobj".to_owned(),
                    token:    base.as_bytes().to_vec(),
                    offset:   ext_offset.unwrap_or(0),
                    size:     ext_size.unwrap_or(base_size),
                };
            }
        }
        Self { provider: String::new(), token: raw.to_vec(), offset: 0, size: 0 }
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

    /// Raw token bytes to pass to the provider SDK (`handle_put` / `handle_get`).
    ///
    /// For cuObj tokens this is always the **base** descriptor without any
    /// `:size:offset` extension, even if the token was parsed from an extended
    /// wire format.
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
    /// - cuObj tokens: the `<size_hex>` field from the base descriptor.
    ///   For extended cuObj tokens (`base:ext_size:ext_offset`) this is
    ///   still the registered capacity from the base, **not** `ext_size`.
    /// - Unknown tokens: `0`.
    pub fn total_buffer_size(&self) -> usize {
        std::str::from_utf8(&self.token).ok()
            .and_then(|s| {
                Self::parse_mock_str(s).map(|(_, _, sz)| sz)
                    .or_else(|| Self::parse_cuobj_base(s))
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

    /// Validate a cuObj base descriptor and return its registered capacity.
    ///
    /// Accepts `<vaddr_hex>:<size_hex>:<rkey_hex>:…` with any number of
    /// trailing fields (the cuObj SDK may include additional data).
    /// Returns `Some(capacity_bytes)` on success, `None` otherwise.
    fn parse_cuobj_base(s: &str) -> Option<usize> {
        let mut fields = s.splitn(4, ':');
        let vaddr_hex = fields.next()?;
        let size_hex  = fields.next()?;
        let _rkey     = fields.next()?; // must be present
        // Both vaddr and size must parse as hexadecimal.
        usize::from_str_radix(vaddr_hex.trim(), 16).ok()?;
        let size = usize::from_str_radix(size_hex.trim(), 16).ok()?;
        Some(size)
    }

    /// Detect and split the extended cuObj format `<base>:<decimal_size>:<decimal_offset>`.
    ///
    /// Returns `(base_str, Some(size), Some(offset))` when the last two
    /// colon-separated fields are valid decimal integers **and** the prefix
    /// validates as a cuObj base descriptor.  Otherwise returns the input
    /// string unchanged with `None` for both optional fields.
    fn split_cuobj_extension(s: &str) -> (&str, Option<usize>, Option<usize>) {
        // Examine the last field.
        let (before_last, last) = match s.rfind(':') {
            Some(i) => (&s[..i], &s[i + 1..]),
            None    => return (s, None, None),
        };
        let ext_offset: usize = match last.parse() {
            Ok(n)  => n,
            Err(_) => return (s, None, None),
        };
        // Examine the second-to-last field.
        let (base, middle) = match before_last.rfind(':') {
            Some(i) => (&before_last[..i], &before_last[i + 1..]),
            None    => return (s, None, None),
        };
        let ext_size: usize = match middle.parse() {
            Ok(n)  => n,
            Err(_) => return (s, None, None),
        };
        // Confirm the prefix is a valid cuObj base descriptor.
        if Self::parse_cuobj_base(base).is_some() {
            (base, Some(ext_size), Some(ext_offset))
        } else {
            (s, None, None)
        }
    }
}

// ── Standard trait impls ──────────────────────────────────────────────────────

impl From<&[u8]> for RdmaToken {
    fn from(raw: &[u8]) -> Self { Self::parse(raw) }
}

impl From<&str> for RdmaToken {
    fn from(s: &str) -> Self { Self::from_str(s) }
}

/// Parses without cloning for non-cuObj tokens; for cuObj tokens the base
/// descriptor is extracted (allocating only the base slice when an extension
/// is present).
impl From<Vec<u8>> for RdmaToken {
    fn from(bytes: Vec<u8>) -> Self {
        if let Ok(s) = std::str::from_utf8(&bytes) {
            if let Some((provider, offset, size)) = Self::parse_mock_str(s) {
                return Self { provider, token: bytes, offset, size };
            }
            let (base, ext_size, ext_offset) = Self::split_cuobj_extension(s);
            if let Some(base_size) = Self::parse_cuobj_base(base) {
                // Store only the base descriptor; strip any extension.
                let token = if base.len() == s.len() { bytes } else { base.as_bytes().to_vec() };
                return Self {
                    provider: "cuobj".to_owned(),
                    token,
                    offset: ext_offset.unwrap_or(0),
                    size:   ext_size.unwrap_or(base_size),
                };
            }
        }
        Self { provider: String::new(), token: bytes, offset: 0, size: 0 }
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

    // ── Extended cuObj token tests ────────────────────────────────────────────

    /// Extended format: `<cuobj_base>:<decimal_size>:<decimal_offset>`.
    const CUOBJ_EXT: &str =
        "7fab000000000000:20000000:deadbeef0000:extra:67108864:4096";

    #[test]
    fn parse_cuobj_extended_basic() {
        let tok = RdmaToken::from_str(CUOBJ_EXT);
        assert_eq!(tok.provider(), "cuobj");
        // Extension fields override the base size_hex.
        assert_eq!(tok.size(),   67108864); // decimal extension, not 0x20000000
        assert_eq!(tok.offset(), 4096);
        assert!(!tok.is_opaque());
    }

    #[test]
    fn cuobj_extended_raw_strips_extension() {
        // raw() must return only the base descriptor; the cuObj C SDK must not
        // see the trailing `:size:offset` fields.
        let tok = RdmaToken::from_str(CUOBJ_EXT);
        assert_eq!(tok.raw(), CUOBJ_TOK.as_bytes());
    }

    #[test]
    fn cuobj_extended_total_buffer_size() {
        // total_buffer_size() always returns the registered capacity from the
        // base descriptor (size_hex), not the extension size.
        let tok = RdmaToken::from_str(CUOBJ_EXT);
        assert_eq!(tok.total_buffer_size(), 0x20000000);
    }

    #[test]
    fn cuobj_non_extended_raw_unchanged() {
        // A plain (non-extended) cuObj token: raw() returns the full string.
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert_eq!(tok.raw(), CUOBJ_TOK.as_bytes());
    }

    #[test]
    fn cuobj_extended_split() {
        // size=67108864 (64 MiB), offset=4096.
        let tok     = RdmaToken::from_str(CUOBJ_EXT);
        let chunk   = 16 * 1024 * 1024; // 16 MiB
        let slices  = tok.split(chunk);
        assert_eq!(slices.len(), 4);
        assert_eq!(slices[0].offset(), 4096);
        assert_eq!(slices[0].size(),   chunk);
        assert_eq!(slices[3].offset(), 4096 + 3 * chunk);
        assert_eq!(slices[3].size(),   chunk);
        // All slices carry the clean base descriptor.
        for sl in &slices {
            assert_eq!(sl.raw(), CUOBJ_TOK.as_bytes());
        }
    }

    #[test]
    fn from_vec_cuobj_extended_strips_extension() {
        let bytes = CUOBJ_EXT.as_bytes().to_vec();
        let tok   = RdmaToken::from(bytes);
        assert_eq!(tok.provider(), "cuobj");
        assert_eq!(tok.size(),   67108864);
        assert_eq!(tok.offset(), 4096);
        // Stored token is the base descriptor only.
        assert_eq!(tok.raw(), CUOBJ_TOK.as_bytes());
    }

    #[test]
    fn cuobj_extended_remote_addr() {
        // remote_addr() reads from the (always-base) stored token.
        let tok = RdmaToken::from_str(CUOBJ_EXT);
        assert_eq!(tok.remote_addr(), 0x7fab000000000000usize);
    }

    #[test]
    fn cuobj_base_not_falsely_extended() {
        // A base token whose last field is not decimal → no extension detected.
        let tok = RdmaToken::from_str(CUOBJ_TOK);
        assert_eq!(tok.size(), 0x20000000); // from size_hex, no override
        assert_eq!(tok.offset(), 0);
        assert_eq!(tok.raw(), CUOBJ_TOK.as_bytes());
    }
}
