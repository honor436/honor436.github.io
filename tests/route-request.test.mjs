import test from 'node:test';
import assert from 'node:assert/strict';
import { ROUTE_ENVS, buildRouteUrl } from '../DltLogViewer/js/route-request.js';

// ---- ROUTE_ENVS ----------------------------------------------------------- //
//
// 경로 탐색 서버: 상용 / 준상용 / 개발 → ntmap / ntmapstg / ntmapdev

test('ROUTE_ENVS_has_prod_stg_dev_korean_labels', () => {
  assert.equal(ROUTE_ENVS.prod.label, '상용');
  assert.equal(ROUTE_ENVS.stg.label, '준상용');
  assert.equal(ROUTE_ENVS.dev.label, '개발');
});

// ---- buildRouteUrl -------------------------------------------------------- //
//
// 전기차(ev): /tmap-channel/rsd/ev/route, 일반: /tmap-channel/rsd/route

test('buildRouteUrl_ev_stg', () => {
  assert.equal(
    buildRouteUrl('stg', true),
    'https://ntmapstg.tmap.co.kr:9443/tmap-channel/rsd/ev/route'
  );
});

test('buildRouteUrl_normal_prod', () => {
  assert.equal(
    buildRouteUrl('prod', false),
    'https://ntmap.tmap.co.kr:9443/tmap-channel/rsd/route'
  );
});

test('buildRouteUrl_ev_dev', () => {
  assert.equal(
    buildRouteUrl('dev', true),
    'https://ntmapdev.tmap.co.kr:9443/tmap-channel/rsd/ev/route'
  );
});

test('buildRouteUrl_unknown_env_throws', () => {
  assert.throws(() => buildRouteUrl('qa', true));
});

// ---- computeCurrentRange (비율 기반) -------------------------------------- //
//
// 현재 도달거리 = 80% 도달거리 × (현재 잔량 / 80% 전력). chargedEnergy 0 이면 0.
import { computeCurrentRange } from '../DltLogViewer/js/route-request.js';

test('computeCurrentRange_scales_by_energy_ratio', () => {
  assert.equal(computeCurrentRange(37600, 55816, 6970), 4695);
});

test('computeCurrentRange_full_ratio', () => {
  assert.equal(computeCurrentRange(40000, 50000, 50000), 40000);
  assert.equal(computeCurrentRange(40000, 50000, 25000), 20000);
});

test('computeCurrentRange_zero_charged_energy_is_0', () => {
  assert.equal(computeCurrentRange(40000, 0, 1000), 0);
});

// ---- Range Map (isochrone) 경로 타입 ------------------------------------- //

test('buildRouteUrl_isochrone_stg', () => {
  assert.equal(
    buildRouteUrl('stg', 'isochrone'),
    'https://ntmapstg.tmap.co.kr:9443/tmap-channel/rsd/route/isochrone'
  );
});

test('buildRouteUrl_string_routeType_ev_and_normal', () => {
  assert.equal(buildRouteUrl('dev', 'ev'), 'https://ntmapdev.tmap.co.kr:9443/tmap-channel/rsd/ev/route');
  assert.equal(buildRouteUrl('prod', 'normal'), 'https://ntmap.tmap.co.kr:9443/tmap-channel/rsd/route');
});

// ---- extractIsochroneRings (도달 가능 범위 GeoJSON Polygon) --------------- //
//
// 응답 isochrone.geometry(Polygon).coordinates(int[][][]) → 링 배열.
import { extractIsochroneRings } from '../DltLogViewer/js/route-request.js';

test('extractIsochroneRings_returns_polygon_rings', () => {
  const json = { isochrone: { type: 'Feature', geometry: { type: 'Polygon',
    coordinates: [ [ [45720000,15600000],[45730000,15600000],[45730000,15610000],[45720000,15600000] ] ] } } };
  const rings = extractIsochroneRings(json);
  assert.equal(rings.length, 1);
  assert.equal(rings[0].length, 4);
  assert.deepEqual(rings[0][0], [45720000, 15600000]);
});

test('extractIsochroneRings_empty_for_missing', () => {
  assert.deepEqual(extractIsochroneRings(null), []);
  assert.deepEqual(extractIsochroneRings({}), []);
  assert.deepEqual(extractIsochroneRings({ isochrone: { geometry: {} } }), []);
});
