// POI 검색 — 순수 로직 (요청 빌드 / 응답 정규화 / 상세 포맷)
//
// 좌표 규약:
//   noorX = SK X좌표(경도 기반, 도 × 36000), noorY = SK Y좌표(위도 기반)
//   skCoordToWgs84(noorX, noorY) → [lat, lon]
//   wgs84ToSkCoord(lat, lon)     → [skX, skY]
//
// 서버 URL·헤더·요청/응답 포맷은 환경에 따라 다를 수 있어 방어적으로 파싱한다.
// 응답 컨테이너 경로를 알 수 없을 때는 객체 트리에서 가장 큰 객체 배열을 POI 목록으로 추정한다.

import { skCoordToWgs84, wgs84ToSkCoord } from './coordinate.js';

export const DEFAULT_POI_SEARCH_URL = 'https://ntmapstg.tmap.co.kr:9443/tmap-channel/poi/search';
export const DEFAULT_POI_DETAIL_URL = 'https://ntmapstg.tmap.co.kr:9443/tmap-channel/poi/detail';

// ---- 헤더 ------------------------------------------------------------------

export function poiSearchHeaders() {
  // Connection / Content-Length 는 브라우저가 강제로 설정하므로 제외(설정해도 무시됨).
  return {
    'requestHashToken': '1988490197',
    'Accept': 'application/json',
    'CIH': '582079726',
    'Network-Type': 'WIFI',
    'Nonce': '1988490197',
    'Requester': 'CLIENT_SSL',
    'Content-Type': 'application/json',
  };
}

function poiAppHeader() {
  return {
    appLaunchCount: 0,
    pushDeviceKey: '',
    screenHeight: 976,
    screenWidth: 878,
    svcType: 116,
    using: 'MAIN',
    appVersion: '2.0.17',
    buildNo: '510000',
    carrier: '',
    deviceId: '2-1-93e68f13-53e6-3d68-9390-2c391dafdaf1',
    modelNo: 'sdk_gphone_arm64',
    osType: 'AND',
    osVersion: '11',
    reqTime: nowReqTime(),
    resolution: 'HD',
  };
}

function nowReqTime() {
  const d = new Date();
  return d.getFullYear().toString() +
    String(d.getMonth() + 1).padStart(2, '0') +
    String(d.getDate()).padStart(2, '0') +
    String(d.getHours()).padStart(2, '0') +
    String(d.getMinutes()).padStart(2, '0') +
    String(d.getSeconds()).padStart(2, '0');
}

// ---- 요청 빌드 -------------------------------------------------------------

export function buildPoiSearchBody(name, opts = {}) {
  const { noorX = 0, noorY = 0, reqCnt = 70, reqSeq = 1 } = opts;
  return {
    name,
    noorX: String(noorX),
    noorY: String(noorY),
    poiGroupYn: 'Y',
    radius: '0',
    reqCnt,
    reqSearchEngineInfo: {
      searchFilterType: '2',
      searchFrom: 'pth',
      searchMethod: 'aut.kwd',
    },
    areaName: '',
    reqSeq,
    searchTypCd: 'A',
    tnowDisplayYn: 'Y',
    header: poiAppHeader(),
  };
}

export function buildPoiDetailBody(poi, opts = {}) {
  const { noorX = 0, noorY = 0, reqSeq = 1 } = opts;
  const raw = (poi && poi.raw) || {};
  return {
    poiId: (poi && poi.id) || '',
    pkey: raw.pkey || (poi && poi.id) || '',
    navSeq: raw.navSeq || '',
    noorX: String(noorX),
    noorY: String(noorY),
    reqSeq,
    header: poiAppHeader(),
  };
}

// ---- 지도 중심 → SK 좌표 ---------------------------------------------------

export function centerToNoor(lat, lon) {
  const [skX, skY] = wgs84ToSkCoord(lat, lon);
  return { noorX: skX, noorY: skY };
}

// ---- 응답 정규화 -----------------------------------------------------------

export function parsePoiResults(json) {
  const arr = findPoiArray(json);
  return arr.map(normalizePoi).filter(p => p.name);
}

export function normalizePoi(item) {
  if (!item || typeof item !== 'object') return { name: '', id: '', tel: '', address: '', lat: null, lon: null, raw: item };
  const name = firstStr(item, ['name', 'poiName', 'title', 'fullName', 'bldName', 'navSeq']);
  const id = firstVal(item, ['pkey', 'poiId', 'id', 'navSeq', 'poiKey']);
  const tel = firstStr(item, ['tel', 'telNo', 'telNumber', 'telDisp', 'phone']);
  const address = firstStr(item, [
    'fullAddrRoad', 'newAddr', 'roadAddr', 'fullAddr', 'addr',
    'lnoAdres', 'rnoAdres', 'address', 'roadName',
  ]);
  const { lat, lon } = extractLatLon(item);
  return {
    name: name || '',
    id: id != null ? String(id) : '',
    tel: tel || '',
    address: address || '',
    lat, lon,
    raw: item,
  };
}

function extractLatLon(item) {
  // SK 좌표(정수, 도 × 36000) 우선 — 요청과 동일 규약.
  const skPairs = [['noorX', 'noorY'], ['centerX', 'centerY'], ['frontX', 'frontY'], ['navX', 'navY'], ['x', 'y']];
  for (const [xk, yk] of skPairs) {
    const x = num(item[xk]);
    const y = num(item[yk]);
    if (x != null && y != null && Math.abs(x) > 1000 && Math.abs(y) > 1000) {
      const r = skCoordToWgs84(x, y);
      if (r) return { lat: r[0], lon: r[1] };
    }
  }
  // WGS84 도 단위.
  const wgsPairs = [['centerLat', 'centerLon'], ['frontLat', 'frontLon'], ['lat', 'lon'], ['latitude', 'longitude'], ['wgsLat', 'wgsLon'], ['noorLat', 'noorLon']];
  for (const [latk, lonk] of wgsPairs) {
    const lat = num(item[latk]);
    const lon = num(item[lonk]);
    if (lat != null && lon != null && Math.abs(lat) <= 90 && Math.abs(lon) <= 180 && (lat !== 0 || lon !== 0)) {
      return { lat, lon };
    }
  }
  return { lat: null, lon: null };
}

function findPoiArray(json) {
  if (Array.isArray(json)) return json;
  if (!json || typeof json !== 'object') return [];
  const known = [
    json.poiInfo?.poi,
    json.searchPoiInfo?.pois?.poi,
    json.pois?.poi,
    json.poiGroup,
    json.pois,
    json.poi,
    json.searchResult?.poi,
    json.body?.poiInfo?.poi,
    json.result?.poi,
    json.results,
  ];
  for (const c of known) if (Array.isArray(c) && c.length) return c;

  // 알려진 경로가 없으면 트리에서 객체 배열 중 가장 큰 것을 추정.
  let best = [];
  const seen = new Set();
  const stack = [json];
  while (stack.length) {
    const cur = stack.pop();
    if (!cur || typeof cur !== 'object' || seen.has(cur)) continue;
    seen.add(cur);
    for (const v of Object.values(cur)) {
      if (Array.isArray(v)) {
        if (v.length && v.every(e => e && typeof e === 'object' && !Array.isArray(e)) && v.length > best.length) best = v;
        v.forEach(e => { if (e && typeof e === 'object') stack.push(e); });
      } else if (v && typeof v === 'object') {
        stack.push(v);
      }
    }
  }
  return best;
}

// ---- 상세 포맷 (데이터 → HTML 문자열) --------------------------------------

export function formatPoiDetailHtml(poi, detailJson) {
  const rows = [];
  rows.push(`<div style="font-size:14px;font-weight:700;margin-bottom:6px">${esc(poi.name)}</div>`);
  if (poi.address) rows.push(detailRow('주소', poi.address));
  if (poi.tel) rows.push(detailRow('전화', poi.tel));
  if (poi.lat != null && poi.lon != null) {
    rows.push(detailRow('좌표', `${poi.lat.toFixed(6)}, ${poi.lon.toFixed(6)} (WGS84)`));
  }
  if (poi.id) rows.push(detailRow('ID', poi.id));
  let detailBlock = '';
  if (detailJson != null) {
    const pretty = typeof detailJson === 'string' ? detailJson : JSON.stringify(detailJson, null, 2);
    detailBlock =
      '<details open style="margin-top:8px"><summary style="cursor:pointer;font-size:11px;color:#64748b">상세 응답</summary>' +
      `<pre style="max-height:240px;overflow:auto;font-size:11px;background:#0f172a;color:#e2e8f0;padding:8px;border-radius:6px;white-space:pre-wrap">${esc(pretty)}</pre></details>`;
  }
  return rows.join('') + detailBlock;
}

function detailRow(label, value) {
  return `<div style="font-size:12px;line-height:1.7"><span style="color:#64748b">${esc(label)}:</span> ${esc(String(value))}</div>`;
}

// ---- helpers --------------------------------------------------------------

function firstStr(obj, keys) {
  for (const k of keys) {
    const v = obj[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
    if (typeof v === 'number') return String(v);
  }
  return '';
}

function firstVal(obj, keys) {
  for (const k of keys) {
    const v = obj[k];
    if (v != null && v !== '') return v;
  }
  return null;
}

function num(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function esc(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
