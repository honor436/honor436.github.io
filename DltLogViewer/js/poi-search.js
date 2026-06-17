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
