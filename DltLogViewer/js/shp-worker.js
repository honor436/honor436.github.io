/**
 * Persistent Web Worker — SHP spatial query server.
 *
 * Protocol (main → worker):
 *   { type: 'load', shpBuf, dbfBuf }
 *   { type: 'query', bounds: [w,s,e,n], queryId }
 *   { type: 'attrs', index }
 *
 * Protocol (worker → main):
 *   { type: 'progress', pct, msg }
 *   { type: 'ready', count, geomType }
 *   { type: 'features', data: [{index, wgs84Feature}], queryId }
 *   { type: 'attrs', index, props }
 *   { type: 'error', msg }
 *
 * DBF attributes are NOT parsed at load time.
 * Only the header (field defs + record size) is parsed once.
 * Individual records are read on-demand via 'attrs' messages.
 */
'use strict';

importScripts('https://unpkg.com/shpjs@4.0.4/dist/shp.js');

// ---- Worker-local state -------------------------------------------------- //
let _geometries = null;
let _bboxes     = null;   // Float64Array [w,s,e,n, ...] — compact spatial index

// DBF on-demand state
let _dbfRaw     = null;   // raw ArrayBuffer — never fully parsed
let _dbfFields  = null;   // [{name, type, length}]
let _dbfHdrSize = 0;
let _dbfRecSize = 0;

// ---- GCS_Tokyo (Bessel 1841) → WGS84 — EPSG:1838 Korea South ----------- //
const BESSEL_a  = 6377397.155;
const BESSEL_e2 = (() => { const f = 1/299.1528128;  return 2*f - f*f; })();
const WGS84_a   = 6378137.0;
const WGS84_e2  = (() => { const f = 1/298.257223563; return 2*f - f*f; })();
const DX = -147.0, DY = 506.0, DZ = 687.5;

function besselToWgs84(lon, lat) {
  const φ = lat * Math.PI / 180, λ = lon * Math.PI / 180;
  const sinφ = Math.sin(φ), cosφ = Math.cos(φ);
  const N = BESSEL_a / Math.sqrt(1 - BESSEL_e2 * sinφ * sinφ);
  const X2 = N * cosφ * Math.cos(λ) + DX;
  const Y2 = N * cosφ * Math.sin(λ) + DY;
  const Z2 = N * (1 - BESSEL_e2) * sinφ + DZ;
  const λ2 = Math.atan2(Y2, X2);
  const p  = Math.sqrt(X2*X2 + Y2*Y2);
  let φ2 = Math.atan2(Z2, p * (1 - WGS84_e2));
  for (let i = 0; i < 10; i++) {
    const Nw = WGS84_a / Math.sqrt(1 - WGS84_e2 * Math.sin(φ2) ** 2);
    φ2 = Math.atan2(Z2 + WGS84_e2 * Nw * Math.sin(φ2), p);
  }
  return [λ2 * 180 / Math.PI, φ2 * 180 / Math.PI];
}

function transformGeom(geom) {
  if (!geom) return geom;
  const tr = ([lng, lat]) => besselToWgs84(lng, lat);
  switch (geom.type) {
    case 'Point':           return { ...geom, coordinates: tr(geom.coordinates) };
    case 'LineString':      return { ...geom, coordinates: geom.coordinates.map(tr) };
    case 'MultiLineString': return { ...geom, coordinates: geom.coordinates.map(r => r.map(tr)) };
    case 'MultiPoint':      return { ...geom, coordinates: geom.coordinates.map(tr) };
    default:                return geom;
  }
}

// ---- Raw bbox in GCS_Tokyo coords (for index — no transform needed) ----- //
function rawBbox(geom) {
  let w = Infinity, s = Infinity, e = -Infinity, n = -Infinity;
  const v = ([x, y]) => {
    if (x < w) w = x; if (y < s) s = y;
    if (x > e) e = x; if (y > n) n = y;
  };
  if (!geom) return [0, 0, 0, 0];
  switch (geom.type) {
    case 'Point':           v(geom.coordinates); break;
    case 'LineString':      geom.coordinates.forEach(v); break;
    case 'MultiLineString': geom.coordinates.forEach(r => r.forEach(v)); break;
    case 'MultiPoint':      geom.coordinates.forEach(v); break;
  }
  return [w === Infinity ? 0 : w, s === Infinity ? 0 : s, e, n];
}

// ---- DBF on-demand reader ------------------------------------------------ //

function parseDbfHeader(buf) {
  const u8  = new Uint8Array(buf);
  const dv  = new DataView(buf);
  const hdrSize = dv.getUint16(8, true);
  const recSize = dv.getUint16(10, true);
  const fields  = [];
  for (let off = 32; off < hdrSize - 1; off += 32) {
    let name = '';
    for (let i = 0; i < 11 && u8[off + i]; i++) name += String.fromCharCode(u8[off + i]);
    if (!name) break;
    const type   = String.fromCharCode(u8[off + 11]);
    const length = u8[off + 16];
    fields.push({ name, type, length });
  }
  return { fields, hdrSize, recSize };
}

function readDbfRecord(index) {
  if (!_dbfRaw || !_dbfFields) return {};
  const decoder = new TextDecoder('euc-kr');
  let off = _dbfHdrSize + index * _dbfRecSize + 1;  // +1: skip deletion flag
  const props = {};
  for (const f of _dbfFields) {
    const slice = new Uint8Array(_dbfRaw, off, f.length);
    let val = decoder.decode(slice).trim();
    if (f.type === 'N' || f.type === 'F') {
      const n = parseFloat(val);
      if (!isNaN(n)) val = n;
    }
    if (val !== '' && val !== null && val !== undefined) props[f.name] = val;
    off += f.length;
  }
  return props;
}

// ---- Message handler ----------------------------------------------------- //

self.onmessage = async function ({ data }) {

  // ---- LOAD: parse SHP + build spatial index (DBF header only) ---------- //
  if (data.type === 'load') {
    try {
      postMessage({ type: 'progress', pct: 20, msg: '[1/2] 지오메트리 파싱 중...' });
      _geometries = shp.parseShp(data.shpBuf);
      const total = _geometries.length;

      // Parse DBF header only — store raw buffer for on-demand record reads
      if (data.dbfBuf) {
        const h = parseDbfHeader(data.dbfBuf);
        _dbfRaw     = data.dbfBuf;
        _dbfFields  = h.fields;
        _dbfHdrSize = h.hdrSize;
        _dbfRecSize = h.recSize;
      }

      postMessage({ type: 'progress', pct: 40,
        msg: `[2/2] 공간 인덱스 생성 중... (0 / ${total.toLocaleString()})` });

      // Build compact Float64Array bbox index — 100K chunks with yield
      _bboxes = new Float64Array(total * 4);
      const CHUNK = 100_000;
      for (let i = 0; i < total; i += CHUNK) {
        const end = Math.min(i + CHUNK, total);
        for (let j = i; j < end; j++) {
          const [w, s, e, n] = rawBbox(_geometries[j]);
          _bboxes[j*4] = w; _bboxes[j*4+1] = s; _bboxes[j*4+2] = e; _bboxes[j*4+3] = n;
        }
        const pct = 40 + Math.round((end / total) * 55);
        postMessage({ type: 'progress', pct,
          msg: `[2/2] 공간 인덱스 생성 중... (${end.toLocaleString()} / ${total.toLocaleString()})` });
        await new Promise(r => setTimeout(r, 0));  // yield — keep browser responsive
      }

      postMessage({ type: 'ready', count: total, geomType: _geometries[0]?.type || '' });

    } catch (err) {
      postMessage({ type: 'error', msg: err.message || String(err) });
    }

  // ---- QUERY: return only visible features (properties fetched on demand) //
  } else if (data.type === 'query') {
    if (!_bboxes) return;
    const [qw, qs, qe, qn] = data.bounds;
    const result = [];
    for (let i = 0, len = _bboxes.length; i < len; i += 4) {
      if (_bboxes[i+2] >= qw && _bboxes[i] <= qe &&
          _bboxes[i+3] >= qs && _bboxes[i+1] <= qn) {
        const idx = i >> 2;
        result.push({
          index: idx,
          wgs84Feature: {
            type: 'Feature',
            geometry: transformGeom(_geometries[idx]),
            properties: {},   // fetched on demand via 'attrs'
            _shpIndex: idx,
          },
        });
      }
    }
    postMessage({ type: 'features', data: result, queryId: data.queryId });

  // ---- ATTRS: read single DBF record by index ----------------------------- //
  } else if (data.type === 'attrs') {
    postMessage({ type: 'attrs', index: data.index, props: readDbfRecord(data.index) });
  }
};
