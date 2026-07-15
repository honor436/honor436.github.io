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
// 전기차(ev): /tmap-channel/rsd/ev/route
// 일반(TVAS 4.4+): /tmap-channel/rsd/route/planningroutemultiformat

test('buildRouteUrl_ev_stg', () => {
  assert.equal(
    buildRouteUrl('stg', true),
    'https://ntmapstg.tmap.co.kr:9443/tmap-channel/rsd/ev/route'
  );
});

test('buildRouteUrl_normal_prod', () => {
  assert.equal(
    buildRouteUrl('prod', false),
    'https://ntmap.tmap.co.kr:9443/tmap-channel/rsd/route/planningroutemultiformat'
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
  assert.equal(buildRouteUrl('prod', 'normal'), 'https://ntmap.tmap.co.kr:9443/tmap-channel/rsd/route/planningroutemultiformat');
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
// 경로 타입(ev/normal/isochrone)은 서로 다른 요청 스키마다. 타입을 바꿀 때 떠나는
// 타입의 바디를 저장하고, 들어가는 타입의 저장본(없으면 기본 템플릿)을 복원한다.
// - 일반은 EV 와 포맷이 다르므로 최초 전환 시 EV 바디에서 공통 필드만 가져와 만든다.
// - 도달범위(isochrone)는 항상 EV 경로 파라미터를 그대로 사용한다(단일 데이터).
import { resolveRouteTypeSwitch } from '../DltLogViewer/js/route-request.js';

const DEF = { chargedEnergy: 70000, chargedRange: 500000, currentEnergy: 6970, currentRange: 47000 };
const ISO = { contoursEnergy: 56000, contoursMeters: 300000, auxiliaryPower: 1200 };
// 공통 필드 + EV 전용 필드가 섞인 EV 경로 바디.
const EV_FULL = {
  ...DEF, tvas: '5.9', departXPos: 100, departYPos: 200, destXPos: 300, destYPos: 400,
  destSearchFlag: 'LeaveReSearch', departRoadType: 'None', routePlanTypes: ['Traffic_Recommend'],
  consumptionParam: '{"mass":2580}', vehicleId: '11CF', vendor: 'BMW', header: { svcType: 113 },
};
const SAVED0 = () => ({ ev: null, normal: null, isochrone: null });

test('resolveRouteTypeSwitch_same_type_keeps_current_body', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'ev', currentBody: { chargedEnergy: 1 },
    saved: SAVED0(), defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body, null); // 같은 타입 → 변경 없음
});

test('resolveRouteTypeSwitch_ev_to_normal_builds_normal_body', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'normal', currentBody: { ...EV_FULL },
    saved: SAVED0(), defaultBody: EV_FULL, isochroneBody: ISO,
  });
  // 일반 바디: 공통 필드 유지 + 필수 보정, EV 전용 필드 제거
  assert.equal(r.body.detailLocFlag, 'NotApplied');
  assert.equal(r.body.resFlag, 1);
  assert.equal(r.body.tvas, '5.9');
  assert.equal(r.body.departXPos, 100);
  assert.equal(r.body.destXPos, 300);
  assert.equal('chargedEnergy' in r.body, false);
  assert.equal('consumptionParam' in r.body, false);
  assert.equal('vehicleId' in r.body, false);
  assert.deepEqual(r.saved.ev, EV_FULL); // 떠난 EV 바디 저장
});

test('resolveRouteTypeSwitch_normal_to_ev_restores_saved_ev_edits', () => {
  const editedEv = { ...EV_FULL, chargedEnergy: 12345 };
  const r = resolveRouteTypeSwitch({
    prevType: 'normal', nextType: 'ev', currentBody: { detailLocFlag: 'NotApplied' },
    saved: { ev: editedEv, normal: null, isochrone: null },
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.chargedEnergy, 12345);       // 저장된 EV 설정 복원
});

test('resolveRouteTypeSwitch_normal_to_ev_no_saved_uses_default', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'normal', nextType: 'ev', currentBody: { detailLocFlag: 'NotApplied' },
    saved: SAVED0(), defaultBody: DEF, isochroneBody: ISO,
  });
  assert.deepEqual(r.body, DEF);
});

test('resolveRouteTypeSwitch_ev_to_normal_restores_saved_normal', () => {
  const savedNormal = { detailLocFlag: 'NotApplied', tvas: '5.9', destXPos: 999 };
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'normal', currentBody: { ...EV_FULL },
    saved: { ev: null, normal: savedNormal, isochrone: null },
    defaultBody: EV_FULL, isochroneBody: ISO,
  });
  assert.deepEqual(r.body, savedNormal); // 저장된 일반 바디 복원
});

test('resolveRouteTypeSwitch_ev_to_iso_applies_ev_battery', () => {
  // 도달범위는 항상 EV 경로 파라미터 사용(단일 데이터).
  const r = resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'isochrone', currentBody: { ...DEF },
    saved: SAVED0(), defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.contoursEnergy, DEF.currentEnergy); // 6970
  assert.equal(r.body.contoursMeters, DEF.currentRange);  // 47000
  assert.deepEqual(r.saved.ev, DEF);                      // 떠난 EV 바디 저장
});

test('resolveRouteTypeSwitch_iso_to_ev_restores_saved_ev_edits', () => {
  const editedEv = { ...DEF, chargedEnergy: 12345 };
  const r = resolveRouteTypeSwitch({
    prevType: 'isochrone', nextType: 'ev', currentBody: { ...ISO },
    saved: { ev: editedEv, normal: null, isochrone: null },
    defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(r.body.chargedEnergy, 12345); // 사용자 설정 유지
  assert.deepEqual(r.saved.isochrone, ISO);  // 떠난 iso 바디 저장
});

test('resolveRouteTypeSwitch_iso_to_ev_no_saved_uses_default', () => {
  const r = resolveRouteTypeSwitch({
    prevType: 'isochrone', nextType: 'ev', currentBody: { ...ISO },
    saved: SAVED0(), defaultBody: DEF, isochroneBody: ISO,
  });
  assert.deepEqual(r.body, DEF);
});

test('resolveRouteTypeSwitch_does_not_mutate_saved_input', () => {
  const saved = SAVED0();
  resolveRouteTypeSwitch({
    prevType: 'ev', nextType: 'isochrone', currentBody: { ...DEF },
    saved, defaultBody: DEF, isochroneBody: ISO,
  });
  assert.equal(saved.ev, null); // 원본 불변
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

// ---- buildNormalBodyFromEv (EV 바디 → 일반 길안내 바디) -------------------- //
//
// 일반 길안내(/rsd/route/planningroutemultiformat, TVAS 4.4+)는 EV 와 요청 포맷이
// 다르다. EV 전용 필드(배터리/충전/소비/차량)를 제거하고, EV 바디에서 일반 스펙에
// 존재하는 공통 필드만 그대로 가져온 뒤 일반 필수 필드를 보정한다.
import { buildNormalBodyFromEv } from '../DltLogViewer/js/route-request.js';

// EV defaultBody 축약본(공통 필드 + EV 전용 필드 혼재).
const EV_SAMPLE = {
  // 공통 필드
  tvas: '5.9', routePlanTypes: ['Traffic_Recommend'], serviceFlag: 'Realtime',
  departXPos: 4581514, departYPos: 1322213, departRoadType: 'None',
  destXPos: 4630714, destYPos: 1291586, destSearchFlag: 'LeaveReSearch',
  destRpFlag: 16, angle: 130, speed: 0, hipassFlag: 0, tollCarType: 'Car',
  wayPoints: [], header: { svcType: 113, using: 'MAIN' }, version: '1.1',
  // EV 전용 필드(일반 스펙에 없음 → 제거되어야 함)
  chargedEnergy: 70000, chargedRange: 500000, currentEnergy: 6970, currentRange: 47000,
  consumptionParam: '{"mass":2580}', socketType: ['DcCombo'], evMobilityProviders: [],
  vehicleId: '11CF', vehicleMass: 2580, vendor: 'BMW', maxCharge: 69770,
  minEnergy: 6977, slopeFlag: 0, efficientSpeed: 0, auxiliaryPower: 1200,
  destEVChargerFlag: false, ecoModeFlag: 0, applyEvChargingTimeOnETA: true,
};

test('buildNormalBodyFromEv_removes_ev_only_fields', () => {
  const out = buildNormalBodyFromEv(EV_SAMPLE);
  for (const k of ['chargedEnergy', 'chargedRange', 'currentEnergy', 'currentRange',
    'consumptionParam', 'socketType', 'evMobilityProviders', 'vehicleId', 'vehicleMass',
    'vendor', 'maxCharge', 'minEnergy', 'slopeFlag', 'efficientSpeed', 'auxiliaryPower',
    'destEVChargerFlag', 'ecoModeFlag', 'applyEvChargingTimeOnETA']) {
    assert.equal(k in out, false, `EV 전용 필드 ${k} 는 제거되어야 함`);
  }
});

test('buildNormalBodyFromEv_keeps_common_fields_from_ev', () => {
  const out = buildNormalBodyFromEv(EV_SAMPLE);
  assert.equal(out.tvas, '5.9');
  assert.equal(out.departXPos, 4581514);
  assert.equal(out.departYPos, 1322213);
  assert.equal(out.destXPos, 4630714);
  assert.equal(out.destYPos, 1291586);
  assert.equal(out.destSearchFlag, 'LeaveReSearch');
  assert.equal(out.departRoadType, 'None');
  assert.deepEqual(out.routePlanTypes, ['Traffic_Recommend']);
  assert.deepEqual(out.wayPoints, []);
  assert.equal(out.header.svcType, 113);
  assert.equal(out.version, '1.1');
});

test('buildNormalBodyFromEv_adds_required_normal_fields', () => {
  const out = buildNormalBodyFromEv(EV_SAMPLE);
  // Tvas4.5+ 요청 시 detailLocFlag 는 반드시 "NotApplied"
  assert.equal(out.detailLocFlag, 'NotApplied');
  // resFlag 1 = binary 응답(default)
  assert.equal(out.resFlag, 1);
});

test('buildNormalBodyFromEv_does_not_mutate_input', () => {
  const ev = { ...EV_SAMPLE };
  buildNormalBodyFromEv(ev);
  assert.equal(ev.chargedEnergy, 70000); // 원본 불변
  assert.equal('detailLocFlag' in ev, false);
});
