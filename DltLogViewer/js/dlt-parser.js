/**
 * DLT Binary File Parser (JavaScript port of dlt_gpslog_parser4.py)
 * Reads DLT files in the browser using File API (ArrayBuffer, chunked)
 */

'use strict';

const DLT_MARKER = new Uint8Array([0x44, 0x4C, 0x54, 0x01]); // 'DLT\x01'
const CHUNK_SIZE = 16 * 1024 * 1024; // 16 MB — fewer async slice/arrayBuffer calls
const _utf8 = new TextDecoder('utf-8', { fatal: false });

const WALLCLOCK_RE = /(\d{4})[/\-](\d{2})[/\-](\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?/;
const EPOCH_TIME_RE = /\btime:\s*(\d{10,13})\b/;

// ---- Timestamp helpers --------------------------------------------------- //

/**
 * Parse a timestamp from decoded DLT record text.
 * Returns a Date object or null.
 */
export function parseDltTimestamp(text) {
  if (!text.includes('time:') && !text.includes('/') && !text.includes('-')) return null;

  const wm = WALLCLOCK_RE.exec(text);
  if (wm) {
    const [, year, month, day, hour, min, sec, frac] = wm;
    // milliseconds (3 digits)
    const ms = frac ? parseInt(frac.padEnd(3, '0').slice(0, 3), 10) : 0;
    return new Date(+year, +month - 1, +day, +hour, +min, +sec, ms);
  }

  const em = EPOCH_TIME_RE.exec(text);
  if (em) {
    let ts = parseFloat(em[1]);
    if (em[1].length === 13) ts /= 1000.0;
    return new Date(ts * 1000);
  }

  return null;
}

// ---- Low-level binary helpers -------------------------------------------- //

/**
 * Find the first occurrence of DLT\x01 in a Uint8Array starting at fromIndex.
 * Returns the index or -1.
 */
function findMarker(buf, fromIndex = 0) {
  const len = buf.length - 3;
  for (let i = fromIndex; i <= len; i++) {
    if (buf[i] === 0x44 && buf[i + 1] === 0x4C && buf[i + 2] === 0x54 && buf[i + 3] === 0x01) {
      return i;
    }
  }
  return -1;
}

/**
 * Find all marker positions in a Uint8Array.
 * Reuses a pre-allocated array to avoid GC pressure.
 */
const _markerPositions = { arr: new Int32Array(65536), len: 0 };

function findAllMarkers(buf) {
  let count = 0;
  let cap = _markerPositions.arr.length;
  let arr = _markerPositions.arr;
  const len = buf.length - 3;
  for (let i = 0; i <= len; i++) {
    if (buf[i] === 0x44 && buf[i + 1] === 0x4C && buf[i + 2] === 0x54 && buf[i + 3] === 0x01) {
      if (count >= cap) {
        cap *= 2;
        const next = new Int32Array(cap);
        next.set(arr);
        arr = next;
      }
      arr[count++] = i;
      i += 3; // skip past marker
    }
  }
  _markerPositions.arr = arr;
  _markerPositions.len = count;
  return _markerPositions;
}

/**
 * Extract the DLT relative timestamp from a record (bytes 4-11).
 * Returns seconds as a float, or null on error.
 */
function extractRelativeTime(recordBytes) {
  if (recordBytes.length < 12) return null;
  if (recordBytes[0] !== 0x44 || recordBytes[1] !== 0x4C ||
      recordBytes[2] !== 0x54 || recordBytes[3] !== 0x01) return null;

  const view = new DataView(
    recordBytes.buffer,
    recordBytes.byteOffset,
    recordBytes.byteLength
  );
  const seconds = view.getUint32(4, true);      // little-endian
  const microseconds = view.getUint32(8, true);  // little-endian
  if (microseconds >= 1_000_000) return null;
  return seconds + microseconds / 1_000_000;
}

/**
 * Decode a DLT record's bytes to a string, stripping null bytes.
 */
function decodeRecord(recordBytes) {
  return _utf8.decode(recordBytes).replace(/\0/g, '');
}

/**
 * Quick byte-level check: is this record likely interesting?
 * Avoids full decode for records we don't care about.
 * All required record types (#RpLog, #onLocationChanged, [MM_RESULT], requestTTS)
 * carry their marker prefix in every DLT record — no fallback scan needed.
 */
function isInterestingRecord(buf, start, end, interestingMarkers) {
  if (!interestingMarkers || interestingMarkers.length === 0) return true;
  for (const marker of interestingMarkers) {
    if (bytesContain(buf, start, end, marker)) return true;
  }
  return false;
}

function bytesContain(haystack, hStart, hEnd, needle) {
  if (needle.length === 0) return true;
  const first = needle[0];
  const nLen = needle.length;
  const limit = hEnd - nLen;
  for (let i = hStart; i <= limit; i++) {
    if (haystack[i] !== first) continue;
    let match = true;
    for (let j = 1; j < nLen; j++) {
      if (haystack[i + j] !== needle[j]) { match = false; break; }
    }
    if (match) return true;
  }
  return false;
}

// Pre-encode interesting marker strings to byte arrays
export function encodeMarkers(strings) {
  return strings.map(s => new TextEncoder().encode(s));
}

// ---- Concat Uint8Arrays -------------------------------------------------- //
function concatUint8(a, b) {
  const result = new Uint8Array(a.length + b.length);
  result.set(a, 0);
  result.set(b, a.length);
  return result;
}

// ---- Median helper (mirrors Python's statistics.median usage) ------------ //
export function medianOf(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

// ---- Main async iterator ------------------------------------------------- //

/**
 * Async generator that yields DLT records from a File object.
 *
 * Each yielded record is:
 *   { text: string, timestamp: Date|null }
 *
 * @param {File} file
 * @param {Function|null} progressCallback  (bytesRead, totalBytes)
 * @param {Uint8Array[]|null} interestingMarkers  pre-encoded byte arrays
 */
export async function* iterateDltRecords(file, progressCallback = null, interestingMarkers = null) {
  const totalSize = file.size;
  let bytesRead = 0;
  let carry = new Uint8Array(0);
  let firstMarkerFound = false;
  const offsetSamples = [];
  let relativeOffset = null; // wallclock - relativeTime

  // Build record from a region of buf[start..end) — avoids slice when filtering
  function buildRecord(buf, start, end) {
    if (!isInterestingRecord(buf, start, end, interestingMarkers)) return null;

    const recordBytes = buf.subarray(start, end);
    const text = decodeRecord(recordBytes);
    const wallclock = parseDltTimestamp(text);
    const relativeTime = extractRelativeTime(recordBytes);

    if (wallclock !== null && relativeTime !== null && offsetSamples.length < 512) {
      offsetSamples.push(wallclock.getTime() / 1000 - relativeTime);
      const n = offsetSamples.length;
      if (n === 1 || (n & (n - 1)) === 0) relativeOffset = medianOf(offsetSamples);
    }

    let timestamp = wallclock;
    if (timestamp === null && relativeOffset !== null && relativeTime !== null) {
      timestamp = new Date((relativeTime + relativeOffset) * 1000);
    }

    return { text, timestamp };
  }

  let offset = 0;
  while (offset < totalSize) {
    const chunkEnd = Math.min(offset + CHUNK_SIZE, totalSize);
    const slice = file.slice(offset, chunkEnd);
    const ab = await slice.arrayBuffer();
    const chunk = new Uint8Array(ab);
    bytesRead += chunk.length;
    offset += chunk.length;

    if (progressCallback) progressCallback(Math.min(bytesRead, totalSize), totalSize);

    // Avoid concat when carry is empty (common case for large chunks)
    let buffer;
    if (carry.length === 0) {
      buffer = chunk;
    } else {
      buffer = concatUint8(carry, chunk);
    }

    const markers = findAllMarkers(buffer);
    const posArr = markers.arr;
    const posLen = markers.len;

    if (posLen === 0) {
      carry = firstMarkerFound
        ? buffer.slice(Math.max(0, buffer.length - 3))
        : buffer;
      continue;
    }

    firstMarkerFound = true;

    // We can only emit records for which we know the end (= next marker start)
    if (posLen === 1) {
      carry = buffer.slice(posArr[0]);
      continue;
    }

    for (let i = 0; i < posLen - 1; i++) {
      const record = buildRecord(buffer, posArr[i], posArr[i + 1]);
      if (record !== null) yield record;
    }

    carry = buffer.slice(posArr[posLen - 1]);
  }

  // Flush carry
  if (carry.length > 0) {
    if (carry.length >= 4 &&
        carry[0] === 0x44 && carry[1] === 0x4C && carry[2] === 0x54 && carry[3] === 0x01) {
      const record = buildRecord(carry, 0, carry.length);
      if (record !== null) yield record;
    } else {
      const text = _utf8.decode(carry).replace(/\0/g, '').trim();
      if (text) {
        yield { text, timestamp: parseDltTimestamp(text) };
      }
    }
  }

  if (progressCallback) progressCallback(totalSize, totalSize);
}
