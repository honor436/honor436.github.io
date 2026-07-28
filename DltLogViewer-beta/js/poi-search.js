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

/**
 * 경로요청 rpFlag 결정: 경유지(via) 기본 18, 목적지(dest) 기본 16.
 * POI 검색/상세에서 받은 rpFlag(숫자)가 있으면 그것을 우선 사용한다.
 */
export function resolveRpFlag(kind, poiRpFlag) {
  if (poiRpFlag != null && poiRpFlag !== '' && !Number.isNaN(Number(poiRpFlag))) {
    return Number(poiRpFlag);
  }
  return kind === 'dest' ? 16 : 18;
}

/**
 * 경로요청 POI ID 결정: 검색/상세에서 받은 poiId 를 문자열로 정규화한다.
 * POI 가 아닌 지점(지도 우클릭 등)은 값이 없으므로 null 을 돌려주고,
 * 호출측이 destPoiId 를 비우거나 경유지 poiID 를 생략하게 한다.
 */
export function resolvePoiId(poiId) {
  if (poiId == null) return null;
  const s = String(poiId).trim();
  return s === '' ? null : s;
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

// 실제 POI 상세 엔드포인트 경로 (검색의 /poi/search/findpois 와 다름).
export const POI_DETAIL_PATH = '/tmap-channel/poi/detailinfo/findpoidetailinfoforauto';

// pkey 없이 poiId 만으로 상세를 조회할 때 쓰는 findOption.
// 충전소(ES3)·주유소(GAS3)는 경로 응답에 poiId 만 있어 이 옵션을 쓴다.
export const POI_DETAIL_FIND_BY_POI_ID = 'POI_ID';

/**
 * 상세 URL 도출: 검색 URL 의 호스트(스킴+호스트+포트)를 그대로 쓰고
 * 실제 상세 경로(POI_DETAIL_PATH)를 붙인다.
 */
export function derivePoiDetailUrl(searchUrl) {
  const u = String(searchUrl || '');
  const origin = /^(https?:\/\/[^/]+)/i.exec(u);
  return origin ? origin[1] + POI_DETAIL_PATH : POI_DETAIL_PATH;
}

// ---- 경로상 POI 검색 (findpoisbyroute/v2) --------------------------------- //
//
// 현재 경로가 있으면 vertex 좌표열을 line_string(단일 세그먼트) 으로, RPLINK 를
// link_id("mesh_link_dir") 로 넣고 start/end_point 를 채운다. 경로가 없으면
// user_point(지도 중심)만 보낸다. 좌표는 WGS84 {lon, lat}.
// 엔드포인트는 findpois 의 /poi/search 와 다른 /poi/search/findpoisbyroute/v2.

export const FINDPOISBYROUTE_V2_PATH = '/tmap-channel/poi/search/findpoisbyroute/v2';

// 경로상 POI 요청 헤더(채널 app 컨텍스트, svcType 113). reqTime 은 전송 시점에
// 채운다(빈 문자열이면 UI 가 현재시간을 넣는다).
function routePoiHeader() {
  return {
    appLaunchCount: 1,
    appVersion: '3.20.600',
    buildNo: '320600',
    carrier: 'T-Mobile',
    deviceId: '0-1-c42f91826d949fef2f2b5006e2f8404127e13189656dcec41311b1c4dce78430',
    modelNo: 'BMW Flavoured AOSP on arm64 Emulator',
    osType: 'AND',
    osVersion: '14',
    pushDeviceKey: '',
    reqTime: '',
    resolution: 'QUAD_HD',
    screenHeight: 1218,
    screenWidth: 2560,
    svcType: 113,
    using: 'MAIN',
  };
}

function toLonLat(p) {
  return { lon: Number(p.lon), lat: Number(p.lat) };
}

// referrer_code 모드 — FindEvPoisByRouteRequest 주석 기준.
export const ROUTE_POI_REFERRER_CODES = [
  { code: 'routeSearchPoiev',        label: 'EV 충전소(경로)' },
  { code: 'radiusSearchPoiev',       label: 'EV 충전소(반경)' },
  { code: 'routeSearchMoment',       label: '모멘티' },
  { code: 'routeSearchPickup',       label: '픽업' },
  { code: 'routeSearchPickupDisplay', label: '픽업 노출' },
];
export const ROUTE_POI_SORTS = ['distance', 'score', 'evcharger'];

/**
 * 경로상 POI 검색(findpoisbyroute/v2) 요청 바디 생성.
 * FindEvPoisByRouteRequest 포맷. 경로 지오메트리(userPoint/lineStringCoords/…)와
 * 검색 옵션(options)을 받아, 값이 있는 필드만 바디에 담는다.
 *
 * @param {Object} arg
 * @param {{lat:number,lon:number}} arg.userPoint          사용자 현재 좌표(항상 포함)
 * @param {Array<{lat:number,lon:number}>} [arg.lineStringCoords] 경로 궤적 좌표열
 * @param {string[]} [arg.linkIds]                          경로 링크("mesh_link_dir")
 * @param {{lat:number,lon:number}} [arg.startPoint]        기본: 좌표열 첫 점
 * @param {{lat:number,lon:number}} [arg.endPoint]          기본: 좌표열 끝 점
 * @param {Object} [arg.options]  referrerCode, sort, pageNo, pageSize, radius, direction,
 *   groupKeyword, totalDistance, operatorId, evChargeType, powerType, evChargeStatus,
 *   tmapPrivateEvYn, tnowDisplayYn, openNowYn, evPublicType, evPncYn, evPncOem,
 *   evKwMinvalue, pickupYn, geoPolygon
 */
export function buildRoutePoiSearchBody({
  userPoint,
  lineStringCoords = null,
  linkIds = null,
  startPoint = null,
  endPoint = null,
  options = {},
} = {}) {
  const o = options || {};
  const {
    referrerCode = 'routeSearchPoiev',
    sort = 'distance',
    pageNo = 1,
    pageSize = 1,
    radius = '0',                 // 데이터 클래스 기본값
    evPncYn = 'N',                // 비-null 기본값 → 항상 포함
    evPncOem = '',
  } = o;

  const hasRoute = Array.isArray(lineStringCoords) && lineStringCoords.length > 0;
  const body = {
    page_no: pageNo,
    page_size: pageSize,
    referrer_code: referrerCode,
    sort,
    radius: String(radius),
  };
  if (hasRoute) {
    body.line_string = [{
      road_type: 0,
      distance: 0,
      coordinates: lineStringCoords.map(toLonLat),
      time: 0,
    }];
    body.start_point = toLonLat(startPoint || lineStringCoords[0]);
    body.end_point = toLonLat(endPoint || lineStringCoords[lineStringCoords.length - 1]);
  }
  body.user_point = userPoint ? toLonLat(userPoint) : { lon: 0, lat: 0 };
  if (hasRoute && Array.isArray(linkIds) && linkIds.length) {
    body.link_id = linkIds.slice();
  }

  // 값이 있을 때만 담는 선택 필드 (빈 문자열/undefined/null 은 제외).
  const put = (key, val) => {
    if (val !== undefined && val !== null && val !== '') body[key] = val;
  };
  put('direction', o.direction);
  put('group_keyword', o.groupKeyword);
  put('operator_id', o.operatorId);
  put('ev_charge_type', o.evChargeType);
  put('power_type', o.powerType);
  put('ev_charge_status', o.evChargeStatus);
  put('tmap_private_ev_yn', o.tmapPrivateEvYn);
  put('tnow_display_yn', o.tnowDisplayYn);
  put('open_now_yn', o.openNowYn);
  put('ev_public_type', o.evPublicType);
  put('pickup_yn', o.pickupYn);
  put('geo_polygon', o.geoPolygon);
  if (o.totalDistance !== undefined && o.totalDistance !== null && o.totalDistance !== '') {
    body.total_distance = Number(o.totalDistance);
  }
  if (o.evKwMinvalue !== undefined && o.evKwMinvalue !== null && o.evKwMinvalue !== '') {
    body.ev_kw_minvalue = Number(o.evKwMinvalue);
  }

  body.ev_pnc_yn = evPncYn;
  body.ev_pnc_oem = evPncOem;
  body.header = routePoiHeader();
  return body;
}

/**
 * findpoisbyroute 응답(poiSearches, snake_case)을 findpois 와 같은
 * { name, x, y, address, poiId, pkey, rpFlag } 형태로 정규화한다.
 * 좌표는 center_x/center_y(SK 정수)를 사용한다.
 */
export function parseRoutePoiResponse(json) {
  if (!json || typeof json !== 'object') return [];
  const list = Array.isArray(json.poiSearches) ? json.poiSearches
    : (Array.isArray(json.poiSearchs) ? json.poiSearchs : deepFindPoiArray(json));
  if (!Array.isArray(list)) return [];
  return list.map(raw => {
    const coords = pickCoords(raw) || { x: null, y: null };
    return {
      name: raw.name || raw.org_name || '',
      x: coords.x,
      y: coords.y,
      address: raw.full_address_road || raw.full_address_jibun || '',
      tel: '',
      poiId: raw.poi_id,
      pkey: raw.pkey,
      rpFlag: raw.rp_flag,
      distance: raw.distance,
      raw,
    };
  }).filter(p => p.name && p.x != null && p.y != null);
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
  if (/<html[\s>]/i.test(s) || /서비스\s*이용에\s*불편/.test(s)) {
    return 'HTML 오류 페이지 응답 — 엔드포인트 URL/경로가 틀렸거나 게이트웨이가 차단했습니다(상세 URL 확인 필요)';
  }
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

// findpois / findpoisbyroute 는 같은 POI 채널이라 camelCase·snake_case 가 섞여
// 내려올 수 있다. 두 표기를 모두 후보로 둔다.
const NAME_KEYS = ['name', 'poiName', 'bldName', 'fullName', 'org_name', 'orgName'];
const ADDR_KEYS = ['fullAddressRoad', 'roadName', 'fullAddress', 'newAddress', 'address', 'addr',
  'full_address_road', 'full_address_jibun', 'fullAddressJibun'];
const TEL_KEYS  = ['tel', 'telNo', 'telNumber'];
// 좌표 후보 (우선순위 순). 각 쌍 [xKey, yKey]. SK 정수 좌표 기준.
// 경유지·목적지로 쓰는 좌표는 안내 좌표(navX1/navY1)가 우선이고, 없으면
// 중심 좌표(centerX/centerY). 그 뒤는 다른 응답 형태를 위한 예비 후보.
const COORD_PAIRS = [
  ['navX1', 'navY1'], ['nav_x1', 'nav_y1'],
  ['centerX', 'centerY'], ['center_x', 'center_y'],
  ['noorX', 'noorY'], ['frontX', 'frontY'],
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
    poiId: pick(raw, ['poiId', 'id', 'poiID', 'poi_id']),
    pkey: pick(raw, ['pkey', 'pKey', 'navSeqPkey']),
    rpFlag: pick(raw, ['rpFlag', 'rpflag', 'rp_flag']),
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
    json?.poiSearches,
    json?.poiSearchs,
  ];
  let list = direct.find(Array.isArray) || deepFindPoiArray(json);
  if (!Array.isArray(list)) return [];
  return list.map(normalizePoi).filter(p => p.name && p.x != null && p.y != null);
}

/**
 * POI 상세 응답 → { name, x, y, address, tel, poiId, pkey, rpFlag, raw } 또는 null.
 * 경유지·목적지 설정에 필요한 값만 뽑는다. 좌표 규칙은 검색과 동일
 * (navX1/navY1 우선, 없으면 centerX/centerY). 상세 응답 형태가 채널/버전마다
 * 달라 대표 컨테이너를 먼저 보고, 없으면 트리를 훑어 좌표가 있는 객체를 찾는다.
 */
export function parsePoiDetailResponse(json) {
  if (!json || typeof json !== 'object') return null;
  const direct = [
    json?.poiDetailInfo,
    json?.poiDetail,
    json?.poiInfo,
    json?.result,
    json,
  ];
  for (const cand of direct) {
    if (cand && typeof cand === 'object' && !Array.isArray(cand) && pickCoords(cand)) {
      return normalizePoi(cand);
    }
  }
  const arr = deepFindPoiArray(json);
  if (arr && arr.length) return normalizePoi(arr[0]);
  const obj = deepFindCoordObject(json);
  return obj ? normalizePoi(obj) : null;
}

// 트리에서 유효 좌표를 가진 첫 객체를 찾는다 (명칭은 없어도 됨).
function deepFindCoordObject(json) {
  const queue = [json];
  while (queue.length) {
    const cur = queue.shift();
    if (Array.isArray(cur)) {
      for (const v of cur) if (v && typeof v === 'object') queue.push(v);
    } else if (cur && typeof cur === 'object') {
      if (pickCoords(cur)) return cur;
      for (const v of Object.values(cur)) if (v && typeof v === 'object') queue.push(v);
    }
  }
  return null;
}
