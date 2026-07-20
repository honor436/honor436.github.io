/**
 * 서버 테스트 페이지 설정/로직.
 *
 * - 환경(상용/준상용/개발)별 호스트
 * - 요청 타입 레지스트리(REQUEST_TYPES): 메뉴는 이 배열로 렌더되므로
 *   새 타입은 항목만 추가하면 메뉴에 자동 노출된다.
 * - 요청/응답 처리 헬퍼는 DltLogViewer 의 poi-search.js 를 재사용.
 */

'use strict';

import { buildPoiSearchBody, buildPoiDetailBody, buildCategorySearchBody, FINDPOISBYROUTE_V2_PATH } from '../DltLogViewer/js/poi-search.js';
import { buildIsochroneBody } from '../DltLogViewer/js/route-request.js';

// 환경 → 호스트. 라벨은 UI 드롭다운에 그대로 노출.
export const TEST_ENVS = {
  prod: { label: '상용',   host: 'ntmap.tmap.co.kr',    port: 9443 },
  stg:  { label: '준상용', host: 'ntmapstg.tmap.co.kr', port: 9443 },
  dev:  { label: '개발',   host: 'ntmapdev.tmap.co.kr', port: 9443 },
};

export function buildTestUrl(env, path) {
  const e = TEST_ENVS[env];
  if (!e) throw new Error('unknown env: ' + env);
  if (!path) return `https://${e.host}:${e.port}`;
  return `https://${e.host}:${e.port}${path.startsWith('/') ? path : '/' + path}`;
}

// poisByRoute 는 정식 포맷 확정 전 — 편집 템플릿(사용자가 채워 전송).
function poisByRouteSample() {
  return {
    name: '',
    reqCnt: 50,
    radius: '0',
    routeVertex: '',         // 경로 vertex (좌표열) — 채워서 사용
    searchTypCd: 'A',
    reqSearchEngineInfo: { searchFilterType: '2', searchFrom: 'pth', searchMethod: 'aut.kwd' },
    header: {},
  };
}

// 요청 타입 레지스트리 — 새 타입은 여기에 항목만 추가하면 메뉴에 노출됨.
export const REQUEST_TYPES = [
  {
    id: 'findpois',
    label: 'POI 검색',
    method: 'POST',
    path: '/tmap-channel/poi/search/findpois',
    sampleBody: () => buildPoiSearchBody({ keyword: '스타벅스', noorX: '4575887', noorY: '1351438' }),
  },
  {
    id: 'findpoidetails',
    label: 'POI 상세',
    method: 'POST',
    path: '/tmap-channel/poi/detailinfo/findpoidetailinfoforauto',
    sampleBody: () => buildPoiDetailBody({ name: 'EV충전소 행당대림아파트', poiId: '10185460', pkey: '1018546001' }),
  },
  {
    id: 'isochrone',
    label: '도달 가능 거리 조회',
    method: 'POST',
    path: '/tmap-channel/rsd/route/isochrone',
    sampleBody: buildIsochroneBody,
  },
  {
    id: 'categorysearch',
    label: '카테고리 검색',
    method: 'POST',
    path: FINDPOISBYROUTE_V2_PATH,
    sampleBody: () => buildCategorySearchBody(),
  },
  {
    id: 'poisbyroute',
    label: '경로상 POI (poisByRoute)',
    method: 'POST',
    path: '/tmap-channel/poi/search/findpoisbyroute',
    implemented: false,
    sampleBody: poisByRouteSample,
  },
];

export function getRequestType(id) {
  return REQUEST_TYPES.find(r => r.id === id) || null;
}
