/**
 * TMAP POI keyword search (tmap-channel findpois).
 *
 * 상용/스테이징/개발 서버를 구분해 검색 URL을 만들고, 키워드 + 현재 좌표로
 * 요청 바디를 구성한 뒤, 응답에서 POI 목록을 정규화한다.
 *
 * 좌표 규약: noorX/noorY 는 SK 정수 좌표(scale 36000). 지도 표시 시에는
 * coordinate.js 의 skCoordToWgs84 로 변환한다.
 */

'use strict';

// 서버 환경 → 호스트. 라벨은 UI 드롭다운에 그대로 노출된다.
export const POI_SEARCH_ENVS = {
  dev:  { label: '개발',   host: 'ntmapdev.tmap.co.kr' },
  stg:  { label: '스테이징', host: 'ntmapstg.tmap.co.kr' },
  prod: { label: '상용',   host: 'ntmap.tmap.co.kr' },
};

const FINDPOIS_PATH = '/tmap-channel/poi/search/findpois';
const PORT = 9443;

export function buildPoiSearchUrl(env) {
  const e = POI_SEARCH_ENVS[env];
  if (!e) throw new Error('unknown POI search env: ' + env);
  return `https://${e.host}:${PORT}${FINDPOIS_PATH}`;
}

// 사용자가 제공한 요청 샘플의 고정 헤더. reqTime 은 전송 시점에 채운다.
function defaultHeader() {
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
    reqTime: '',
    resolution: 'HD',
  };
}

/**
 * 검색 키워드 + 현재 좌표(SK)로 findpois 요청 바디 생성.
 * @param {{ keyword:string, noorX:number|string, noorY:number|string, reqSeq?:number }} opts
 */
export function buildPoiSearchBody({ keyword, noorX, noorY, reqSeq = 1 }) {
  return {
    name: keyword,
    noorX: String(noorX),
    noorY: String(noorY),
    poiGroupYn: 'Y',
    radius: '0',
    reqCnt: 70,
    reqSearchEngineInfo: {
      searchFilterType: '2',
      searchFrom: 'pth',
      searchMethod: 'aut.kwd',
    },
    areaName: '',
    reqSeq,
    searchTypCd: 'A',
    tnowDisplayYn: 'Y',
    header: defaultHeader(),
  };
}

// ---- POI detail ----------------------------------------------------------- //

// POI 상세 요청 헤더(제공된 상세 샘플 기준 — 검색과 svcType/appVersion 등이 다름).
function detailHeader() {
  return {
    appLaunchCount: 0,
    pushDeviceKey: '',
    screenHeight: 854,
    screenWidth: 768,
    svcType: 114,
    using: 'MAIN',
    appVersion: '1.0.13',
    buildNo: '100000',
    carrier: '',
    deviceId: '2-1-8b8e9a44-cba3-3beb-9a5d-5c6eec7d2d79',
    modelNo: 'SM-T510',
    osType: 'AND',
    osVersion: '10',
    reqTime: '',
    resolution: 'WSVGA',
  };
}

/**
 * 검색 결과 선택 → POI 상세 조회 요청 바디(findOption=PKEY).
 * @param {{ name:string, poiId:string|number, pkey:string|number, findOption?:string }} opts
 */
export function buildPoiDetailBody({ name, poiId, pkey, findOption = 'PKEY' }) {
  return {
    findOption,
    name,
    poiId: poiId != null ? String(poiId) : '',
    pkey: pkey != null ? String(pkey) : '',
    header: detailHeader(),
  };
}

/**
 * 상세 URL 도출: 검색 URL 의 'findpois' 경로만 'findpoidetails' 로 치환.
 * 호스트/포트/스킴은 검색 URL 을 그대로 따른다(편집 가능).
 */
export function derivePoiDetailUrl(searchUrl) {
  const u = String(searchUrl || '');
  if (/findpois\b/.test(u)) return u.replace(/findpois\b/, 'findpoidetails');
  return u.replace(/\/?$/, '/').replace(/\/$/, '/findpoidetails');
}

// ---- request headers ------------------------------------------------------ //

function nowReqTime14() {
  const n = new Date();
  return n.getFullYear().toString() +
    String(n.getMonth() + 1).padStart(2, '0') + String(n.getDate()).padStart(2, '0') +
    String(n.getHours()).padStart(2, '0') + String(n.getMinutes()).padStart(2, '0') +
    String(n.getSeconds()).padStart(2, '0');
}

/**
 * findpois 서버 요구 헤더.
 * Connection / Content-Length 는 브라우저가 자동 설정하는 금지 헤더(fetch 가
 * 무시)이므로 포함하지 않는다. Client_ReqTime 은 기본적으로 현재시간.
 * @param {{ reqTime?:string, requestHashToken?:string, cih?:string }} [opts]
 */
export function buildPoiSearchHeaders(opts = {}) {
  return {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'requestHashToken': opts.requestHashToken || '1988490197',
    'CIH': opts.cih || '582079726',
    'Client_ReqTime': opts.reqTime || nowReqTime14(),
  };
}

// ---- header text (Key: Value) --------------------------------------------- //
//
// Tmap 요청 테스터 규칙: "Key: Value" 한 줄에 하나, '#' 주석/빈 줄 무시,
// 값이 비었거나 <...> 플레이스홀더면 전송에서 제외하고 skipped 로 보고.

export function parseHeaderText(text) {
  const headers = {};
  const skipped = [];
  for (const rawLine of String(text || '').split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const idx = line.indexOf(':');
    if (idx < 0) continue;
    const key = line.slice(0, idx).trim();
    const value = line.slice(idx + 1).trim();
    if (!key) continue;
    if (value === '' || /^<.*>$/.test(value)) { skipped.push(key); continue; }
    headers[key] = value;
  }
  return { headers, skipped };
}

/**
 * findpois 요청용 기본 헤더 템플릿(Key: Value 텍스트).
 * AccessKey/AccessToken/Nonce 는 플레이스홀더 — 사용자가 최신값으로 채워야
 * 검색이 동작한다(미입력 시 전송에서 제외됨).
 */
export function buildDefaultHeaderText(opts = {}) {
  const reqTime = opts.reqTime || nowReqTime14();
  return [
    '# 값이 비었거나 <...> 인 헤더는 전송에서 제외됩니다.',
    '# AccessKey / AccessToken 은 최신값으로 채워야 검색이 동작합니다(만료 주의).',
    'Accept: application/json',
    'Content-Type: application/json',
    'AccessKey: <AccessKey 입력>',
    'AccessToken: <AccessToken 입력>',
    'CIH: 582079726',
    'Nonce: <Nonce>',
    'requestHashToken: 1988490197',
    'Client_ReqTime: ' + reqTime,
    'Requester: CLIENT_SSL',
  ].join('\n');
}

// ---- response decoding ---------------------------------------------------- //
//
// Tmap 채널 응답은 JSON(utf-8) 이거나 CP949 바이너리(에러 메시지)일 수 있다.
// charset → utf-8 → euc-kr 순으로 디코딩한다.

export function decodeTmapBody(bytes, contentType = '') {
  const u8 = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
  const order = [];
  const m = /charset=([\w-]+)/i.exec(contentType || '');
  if (m) order.push(m[1].toLowerCase());
  order.push('utf-8', 'euc-kr');
  for (const enc of order) {
    try { return new TextDecoder(enc, { fatal: true }).decode(u8); } catch {}
  }
  try { return new TextDecoder('euc-kr').decode(u8); } catch { return ''; }
}

/**
 * 디코딩된 본문에서 Tmap 채널 오류 힌트를 추출. 정상 응답이면 null.
 * 예: "NCH01000 서비스가 지연되고 있습니다" → 인증 헤더 누락/만료 안내.
 */
export function extractTmapErrorHint(text) {
  const s = String(text || '');
  const code = /\bNCH\d{5}\b/.exec(s);
  if (code) {
    return `채널 오류 ${code[0]} — AccessKey/AccessToken 누락·만료 가능성(헤더 확인 필요)`;
  }
  if (/서비스가\s*지연/.test(s)) {
    return '채널이 요청을 거부했습니다(서비스 지연/인증) — AccessKey·AccessToken 확인';
  }
  return null;
}

// ---- lenient JSON ---------------------------------------------------------- //

/**
 * 응답 텍스트를 최대한 JSON 객체로 파싱한다. 순수 JSON 이 아니면
 * BOM/공백 제거 후, 첫 '{' 또는 '[' 부터 끝까지를 점진적으로 줄여가며 시도.
 * 끝내 실패하면 null.
 */
export function coerceJson(text) {
  if (typeof text !== 'string' || text.length === 0) return null;
  try { return JSON.parse(text); } catch {}
  const t = text.replace(/^﻿/, '').trim();
  if (t !== text) { try { return JSON.parse(t); } catch {} }
  const start = t.search(/[\{\[]/);
  if (start < 0) return null;
  for (let end = t.length; end > start; end--) {
    const ch = t[end - 1];
    if (ch !== '}' && ch !== ']') continue;
    try { return JSON.parse(t.slice(start, end)); } catch {}
  }
  return null;
}

// ---- response parsing ----------------------------------------------------- //

const NAME_KEYS = ['name', 'poiName', 'bldName', 'fullName'];
const ADDR_KEYS = ['fullAddressRoad', 'roadName', 'fullAddress', 'newAddress', 'address', 'addr'];
const TEL_KEYS  = ['tel', 'telNo', 'telNumber'];
// 좌표 후보 (우선순위 순). 각 쌍 [xKey, yKey].
const COORD_PAIRS = [
  ['noorX', 'noorY'], ['frontX', 'frontY'], ['centerX', 'centerY'],
  ['navX', 'navY'], ['posX', 'posY'], ['x', 'y'],
];

function pick(obj, keys) {
  for (const k of keys) {
    if (obj[k] != null && obj[k] !== '') return obj[k];
  }
  return undefined;
}

function pickCoords(obj) {
  for (const [xk, yk] of COORD_PAIRS) {
    const x = Number(obj[xk]), y = Number(obj[yk]);
    if (Number.isFinite(x) && Number.isFinite(y) && x !== 0 && y !== 0) {
      return { x, y };
    }
  }
  return null;
}

function looksLikePoi(obj) {
  return obj && typeof obj === 'object' && !Array.isArray(obj) &&
    pick(obj, NAME_KEYS) != null && pickCoords(obj) != null;
}

// JSON 트리를 폭넓게 훑어 POI 객체들로 이뤄진 첫 배열을 찾는다.
function deepFindPoiArray(json) {
  const queue = [json];
  while (queue.length) {
    const cur = queue.shift();
    if (Array.isArray(cur)) {
      if (cur.length && cur.some(looksLikePoi)) return cur.filter(looksLikePoi);
      for (const v of cur) if (v && typeof v === 'object') queue.push(v);
    } else if (cur && typeof cur === 'object') {
      for (const v of Object.values(cur)) if (v && typeof v === 'object') queue.push(v);
    }
  }
  return null;
}

function normalizePoi(raw) {
  const coords = pickCoords(raw) || { x: null, y: null };
  return {
    name: pick(raw, NAME_KEYS) || '',
    x: coords.x,
    y: coords.y,
    address: pick(raw, ADDR_KEYS) || '',
    tel: pick(raw, TEL_KEYS) || '',
    poiId: pick(raw, ['poiId', 'id', 'poiID']),
    pkey: pick(raw, ['pkey', 'pKey', 'navSeqPkey']),
    raw,
  };
}

/**
 * findpois 응답 → [{ name, x, y, address, tel, raw }]. (x/y 는 SK 좌표)
 */
export function parsePoiSearchResponse(json) {
  if (!json || typeof json !== 'object') return [];
  const direct = [
    json?.poiInfo?.pois?.poi,
    json?.searchPoiInfo?.pois?.poi,
    json?.pois?.poi,
    json?.poiInfo?.poi,
    json?.poiList,
    json?.polist,
  ];
  let list = direct.find(Array.isArray) || deepFindPoiArray(json);
  if (!Array.isArray(list)) return [];
  return list.map(normalizePoi).filter(p => p.name && p.x != null && p.y != null);
}
