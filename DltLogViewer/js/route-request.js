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
 * @param {string} env  'prod' | 'stg' | 'dev'
 * @param {boolean} evMode  true=전기차(ev/route), false=일반(route)
 */
export function buildRouteUrl(env, evMode) {
  const e = ROUTE_ENVS[env];
  if (!e) throw new Error('unknown route env: ' + env);
  const path = evMode ? '/tmap-channel/rsd/ev/route' : '/tmap-channel/rsd/route';
  return `https://${e.host}:${PORT}${path}`;
}
