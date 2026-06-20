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

// ---- applyEvBatteryToIsochrone (EV 배터리 → isochrone 반영) --------------- //
//
// EV 현재 배터리(currentEnergy) → contoursEnergy, 현재 도달거리(currentRange) → contoursMeters.
import { applyEvBatteryToIsochrone } from '../DltLogViewer/js/route-request.js';

test('applyEvBatteryToIsochrone_maps_current_energy_and_range', () => {
  const iso = { contoursEnergy: 1, contoursMeters: 2, vehicleId: 'volvo_XC40' };
  const out = applyEvBatteryToIsochrone(iso, { currentEnergy: 51200, currentRange: 300000 });
  assert.equal(out.contoursEnergy, 51200);
  assert.equal(out.contoursMeters, 300000);
  assert.equal(out.vehicleId, 'volvo_XC40');
});

test('applyEvBatteryToIsochrone_keeps_iso_when_ev_missing', () => {
  const iso = { contoursEnergy: 51200, contoursMeters: 300000 };
  const out = applyEvBatteryToIsochrone(iso, {});
  assert.equal(out.contoursEnergy, 51200);
  assert.equal(out.contoursMeters, 300000);
});

// ---- buildIsochroneBody (도달 가능 거리 조회 요청 바디) -------------------- //
//
// 제공된 샘플(BMW RV11) 기준 isochrone 요청 바디. consumptionParam 은
// stringified JSON, header.reqTime 은 전송 시 채우도록 비워둔다.
import { buildIsochroneBody } from '../DltLogViewer/js/route-request.js';

test('buildIsochroneBody_has_contours_and_depart_fields', () => {
  const body = buildIsochroneBody();
  assert.equal(body.contoursEnergy, 56000);
  assert.equal(body.contoursMeters, 300000);
  assert.equal(body.departXPos, 4575789);
  assert.equal(body.departYPos, 1345346);
  assert.equal(body.auxiliaryPower, 1200);
  assert.equal(body.slopeFlag, 1);
  assert.equal(body.vehicleId, 'RV11');
  assert.equal(body.vehicleMass, 2000);
  assert.equal(body.vendor, 'BMW');
  assert.equal(body.version, '1.1');
});

test('buildIsochroneBody_consumptionParam_is_stringified_json', () => {
  const body = buildIsochroneBody();
  assert.equal(typeof body.consumptionParam, 'string');
  const cp = JSON.parse(body.consumptionParam);
  assert.equal(cp.batteryCapacity, 80000);
  assert.equal(cp.mass, 2000);
  assert.equal(cp.vehicleId, 'RV11');
  assert.equal(cp.vendor, 'BMW');
  assert.equal(cp.chargingModeGen6, 'PERFORMANCE');
  assert.ok(Array.isArray(cp.csc) && cp.csc.length === 11);
});

test('buildIsochroneBody_header_reqTime_empty_svcType_113', () => {
  const body = buildIsochroneBody();
  assert.equal(body.header.reqTime, '');
  assert.equal(body.header.svcType, 113);
  assert.equal(body.header.using, 'MAIN');
});
