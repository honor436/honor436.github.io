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

test('applyEvBatteryToIsochrone_copies_consumptionParam_from_ev', () => {
  const iso = { contoursEnergy: 1, contoursMeters: 2, consumptionParam: 'ISO_OLD' };
  const out = applyEvBatteryToIsochrone(iso, { currentEnergy: 100, currentRange: 200, consumptionParam: 'EV_CP' });
  assert.equal(out.consumptionParam, 'EV_CP'); // EV 경로의 consumptionParam 동일 적용
});

test('applyEvBatteryToIsochrone_keeps_iso_consumptionParam_when_ev_missing', () => {
  const iso = { consumptionParam: 'ISO_CP' };
  const out = applyEvBatteryToIsochrone(iso, { currentEnergy: 100 });
  assert.equal(out.consumptionParam, 'ISO_CP');
});

test('applyEvBatteryToIsochrone_copies_vehicle_fields_from_ev', () => {
  const iso = { slopeFlag: 1, vehicleId: 'RV11', vehicleMass: 2000, vendor: 'X' };
  const out = applyEvBatteryToIsochrone(iso, {
    currentEnergy: 100, currentRange: 200,
    slopeFlag: 0, vehicleId: '11CF', vehicleMass: 2580, vendor: 'BMW',
  });
  assert.equal(out.slopeFlag, 0);
  assert.equal(out.vehicleId, '11CF');
  assert.equal(out.vehicleMass, 2580);
  assert.equal(out.vendor, 'BMW');
});

test('applyEvBatteryToIsochrone_copies_aux_efficientSpeed_and_battery_fields', () => {
  // EV·도달가능범위 공통 데이터: 보조전력/효율속도 + 배터리 4필드도 그대로 가져온다.
  const iso = { contoursEnergy: 1, contoursMeters: 2, auxiliaryPower: 99, efficientSpeed: 5 };
  const out = applyEvBatteryToIsochrone(iso, {
    chargedEnergy: 70000, chargedRange: 500000, currentEnergy: 100, currentRange: 200,
    auxiliaryPower: 1200, efficientSpeed: 0,
  });
  assert.equal(out.auxiliaryPower, 1200);
  assert.equal(out.efficientSpeed, 0);
  assert.equal(out.chargedEnergy, 70000);
  assert.equal(out.chargedRange, 500000);
  assert.equal(out.currentEnergy, 100);
  assert.equal(out.currentRange, 200);
  assert.equal(out.contoursEnergy, 100);   // = currentEnergy
  assert.equal(out.contoursMeters, 200);    // = currentRange
});

test('applyEvBatteryToIsochrone_keeps_iso_vehicle_fields_when_ev_missing', () => {
  const iso = { slopeFlag: 1, vehicleId: 'RV11', vehicleMass: 2000, vendor: 'BMW' };
  const out = applyEvBatteryToIsochrone(iso, { currentEnergy: 100 });
  assert.equal(out.slopeFlag, 1);
  assert.equal(out.vehicleId, 'RV11');
  assert.equal(out.vehicleMass, 2000);
  assert.equal(out.vendor, 'BMW');
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

// ---- resolveIsochroneHeaders (지도화면 전송 헤더 보정) --------------------- //
//
// 과거 버그: 지도화면 isochrone 전송 시 인증 헤더(AccessKey/AccessToken/CIH/...)
// 를 전부 버리고 Accept/Content-Type 만 보내 서버가 거부했다.
// 인증 헤더는 유지하고 응답 포맷(JSON)에 맞춰 Accept 만 보정해야 한다.
import { resolveIsochroneHeaders } from '../DltLogViewer/js/route-request.js';

test('resolveIsochroneHeaders_keeps_auth_headers_and_forces_json_accept', () => {
  const input = {
    Accept: 'application/octet-stream',
    'Content-Type': 'application/json; charset=UTF-8',
    AccessKey: 'AK', AccessToken: 'AT', CIH: '582079726',
    Nonce: '858416984', requestHashToken: '1988490197',
    Client_ReqTime: '20260620141840', Requester: 'CLIENT_SSL',
  };
  const out = resolveIsochroneHeaders(input);
  assert.equal(out.AccessKey, 'AK');
  assert.equal(out.AccessToken, 'AT');
  assert.equal(out.CIH, '582079726');
  assert.equal(out.Nonce, '858416984');
  assert.equal(out.requestHashToken, '1988490197');
  assert.equal(out.Requester, 'CLIENT_SSL');
  assert.equal(out.Client_ReqTime, '20260620141840');
  // 응답이 JSON 이므로 Accept 는 application/json 으로 보정
  assert.equal(out.Accept, 'application/json');
  assert.equal(out['Content-Type'], 'application/json; charset=UTF-8');
});

test('resolveIsochroneHeaders_does_not_mutate_input', () => {
  const input = { Accept: 'application/octet-stream', AccessKey: 'AK' };
  const out = resolveIsochroneHeaders(input);
  assert.equal(input.Accept, 'application/octet-stream');
  assert.notEqual(out, input);
});

// ---- resolveRouteTypeSwitch (경로 타입 전환 시 설정 값 유지) --------------- //
//
// 경로 타입(ev/normal/isochrone)을 바꿔도 사용자가 설정한 EV 배터리/도달범위
// 값이 유지되어야 한다. 같은 스키마(ev↔normal)면 현재 바디 그대로, 스키마가
// 바뀌면(iso↔route) 직전에 저장한 해당 종류의 바디를 복원한다.
import { resolveRouteTypeSwitch } from '../DltLogViewer/js/route-request.js';

const DEF = { chargedEnergy: 70000, chargedRange: 500000, currentEnergy: 6970, currentRange: 47000 };
const ISO = { contoursEnergy: 56000, contoursMeters: 300000, auxiliaryPower: 1200 };

test('resolveRouteTypeSwitch_same_kind_keeps_current_body', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'normal', currentBody: { chargedEnergy: 1 },
    saved: { route: null, isochrone: null }, evEdited: false,
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body, null); // 변경 없음(현재 유지)
});

test('resolveRouteTypeSwitch_route_to_iso_always_derives_from_ev', () => {
  // 도달가능범위는 항상 EV 경로 파라미터를 그대로 사용(단일 데이터). 편집 여부 무관.
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'isochrone', currentBody: { ...DEF },
    saved: { route: null, isochrone: null }, evEdited: false,
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.contoursEnergy, DEF.currentEnergy); // 6970 (EV 잔량)
  assert.equal(r.body.contoursMeters, DEF.currentRange);  // 47000 (EV 도달거리)
  assert.deepEqual(r.saved.route, DEF); // 떠난 EV 바디 저장
});

test('resolveRouteTypeSwitch_route_to_iso_uses_edited_ev_battery', () => {
  const editedEv = { ...DEF, currentEnergy: 51200, currentRange: 280000 };
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'isochrone', currentBody: editedEv,
    saved: { route: null, isochrone: null }, evEdited: false,
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.contoursEnergy, 51200);
  assert.equal(r.body.contoursMeters, 280000);
});

test('resolveRouteTypeSwitch_iso_to_route_restores_saved_ev_edits', () => {
  // 사용자가 EV 배터리를 바꿔둔 바디가 저장돼 있으면, 기본값이 아니라 그걸 복원
  const editedEv = { ...DEF, chargedEnergy: 12345 };
  const r = resolveRouteTypeSwitch({
    prevType: 'isochrone', nextType: 'ev', currentBody: { ...ISO },
    saved: { route: editedEv, isochrone: null }, evEdited: true,
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.chargedEnergy, 12345); // 사용자 설정 유지
  assert.deepEqual(r.saved.isochrone, ISO);  // 떠난 iso 바디 저장
});

test('resolveRouteTypeSwitch_iso_to_route_no_saved_uses_default', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'isochrone', nextType: 'ev', currentBody: { ...ISO },
    saved: { route: null, isochrone: null }, evEdited: false,
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.deepEqual(r.body, DEF);
});

test('resolveRouteTypeSwitch_does_not_mutate_saved_input', () => {
  const saved = { route: null, isochrone: null };
  resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'isochrone', currentBody: { ...DEF },
    saved, evEdited: false, defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(saved.route, null); // 원본 불변
});

// ---- buildIsoBodyFromEvBattery ("EV 경로 데이터 가져오기" 버튼) ------------ //
//
// EV 경로 바디의 현재 배터리(currentEnergy/currentRange)를 isochrone 바디의
// contoursEnergy/contoursMeters 로 가져와 적용. 저장된 EV 바디가 없으면 fallback.
import { buildIsoBodyFromEvBattery } from '../DltLogViewer/js/route-request.js';

test('buildIsoBodyFromEvBattery_uses_saved_ev_current_battery', () => {
  const ev = { ...DEF, currentEnergy: 41000, currentRange: 230000 };
  const out = buildIsoBodyFromEvBattery(ISO, ev, DEF);
  assert.equal(out.contoursEnergy, 41000);
  assert.equal(out.contoursMeters, 230000);
  assert.equal(out.auxiliaryPower, 1200); // 기존 iso 값 유지
});

test('buildIsoBodyFromEvBattery_falls_back_when_no_saved_ev', () => {
  const out = buildIsoBodyFromEvBattery(ISO, null, DEF);
  assert.equal(out.contoursEnergy, DEF.currentEnergy); // 6970
  assert.equal(out.contoursMeters, DEF.currentRange);  // 47000
});
