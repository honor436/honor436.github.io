/**
 * 경로 탐색(RSD) 요청 URL 빌더.
 * 서버(상용/준상용/개발) × 경로 타입(전기차/일반) 조합으로 엔드포인트 URL 생성.
 */

'use strict';

export const ROUTE_ENVS = {
  prod: { label: '상용',   host: 'ntmap.tmap.co.kr' },
  stg:  { label: '준상용', host: 'ntmapstg.tmap.co.kr' },
  dev:  { label: '개발',   host: 'ntmapdev.tmap.co.kr' },
};

const PORT = 9443;

/**
 * 경로 타입별 엔드포인트 URL.
 * @param {string} env  'prod' | 'stg' | 'dev'
 * @param {string|boolean} routeType  'ev' | 'normal' | 'isochrone'
 *   (구버전 호환: true=ev, false=normal)
 */
export function buildRouteUrl(env, routeType) {
  const e = ROUTE_ENVS[env];
  if (!e) throw new Error('unknown route env: ' + env);
  if (routeType === true) routeType = 'ev';
  else if (routeType === false) routeType = 'normal';
  const path = routeType === 'isochrone' ? '/tmap-channel/rsd/route/isochrone'
             : routeType === 'ev'        ? '/tmap-channel/rsd/ev/route'
             :                             '/tmap-channel/rsd/route';
  return `https://${e.host}:${PORT}${path}`;
}

/**
 * 현재 도달거리(currentRange) = 80% 도달거리 × (현재 잔량 / 80% 전력).
 * chargedEnergy 가 0 이하이면 0.
 */
export function computeCurrentRange(chargedRange, chargedEnergy, currentEnergy) {
  const cr = Number(chargedRange), ce = Number(chargedEnergy), cur = Number(currentEnergy);
  if (!(ce > 0) || !Number.isFinite(cr) || !Number.isFinite(cur)) return 0;
  return Math.round(cr * cur / ce);
}

/**
 * 도달 가능 범위(isochrone) 응답에서 GeoJSON Polygon 링 배열 추출.
 * isochrone.geometry.coordinates = int[][][] (Polygon: 링들의 배열, 각 링 = [x,y] 좌표쌍 배열).
 * 좌표는 SK 정규화(8자리). 반환: [[ [x,y], ... ], ...]
 */
export function extractIsochroneRings(json) {
  const coords = json && json.isochrone && json.isochrone.geometry && json.isochrone.geometry.coordinates;
  if (!Array.isArray(coords)) return [];
  return coords.filter(ring => Array.isArray(ring) && ring.length > 0);
}
