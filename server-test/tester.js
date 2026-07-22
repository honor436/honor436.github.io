/**
 * 서버 테스트 페이지 설정/로직.
 *
 * - 환경(상용/준상용/개발)별 호스트
 * - 요청 타입 레지스트리(REQUEST_TYPES): 메뉴는 이 배열로 렌더되므로
 *   새 타입은 항목만 추가하면 메뉴에 자동 노출된다.
 * - 요청/응답 처리 헬퍼는 DltLogViewer 의 poi-search.js 를 재사용.
 */

'use strict';

import { buildPoiSearchBody, buildPoiDetailBody, buildRoutePoiSearchBody, FINDPOISBYROUTE_V2_PATH } from '../DltLogViewer/js/poi-search.js';
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

// 경로상 POI 검색 샘플 — 실 경로 대신 편집용 예시 경로(coords + link_id)를 채워
// findpoisbyroute/v2 형식을 그대로 보여준다. 사용자는 좌표/링크를 갈아끼워 전송.
function routePoiSample() {
  const coords = [
    { lon: 126.98503051471543, lat: 37.56608474300974 },
    { lon: 126.98445834865241, lat: 37.56606806812341 },
    { lon: 126.98410005027014, lat: 37.5660652842617 },
    { lon: 126.98366120401481, lat: 37.566068053832105 },
    { lon: 126.98341400577341, lat: 37.56607082683843 },
    { lon: 126.98275296041555, lat: 37.56602082110331 },
  ];
  return buildRoutePoiSearchBody({
    userPoint: { lon: 126.98502349853516, lat: 37.56645202636719 },
    startPoint: { lon: 126.98502203206233, lat: 37.56641783368988 },
    endPoint: { lon: 126.9799393522724, lat: 37.56961216075511 },
    lineStringCoords: coords,
    linkIds: ['4787_763_0', '4787_765_1', '4787_764_0', '4787_814_1', '4787_813_0'],
    options: { referrerCode: 'routeSearchPoiev', sort: 'distance', radius: '100' },
  });
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
    id: 'routepoisearch',
    label: '경로상 POI 검색',
    method: 'POST',
    path: FINDPOISBYROUTE_V2_PATH,
    sampleBody: routePoiSample,
  },
];

export function getRequestType(id) {
  return REQUEST_TYPES.find(r => r.id === id) || null;
}
