import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { parsePoiResults, parsePoiSearchResponse, parseRoutePoiResponse, isEvChargerPoi } from '../DltLogViewer/js/poi-search.js';
import { applyWaypointsToRouteBody, waypointFromPoi } from '../DltLogViewer/js/route-waypoints.js';

// 실제 POI 검색 응답 (EV 충전소 10건). poiSearches[] + camelCase(poiId/pkey/navX1/rpFlag).
// 모든 항목에 poiId·pkey 가 있으므로 파싱 결과에서도 항상 빠짐없이 나와야 한다.
const json = JSON.parse(readFileSync(new URL('./fixtures/poi-search-ev-charger-10.json', import.meta.url), 'utf8'));
const raws = json.poiSearches;

// ---- parsePoiResults: 화면에서 쓰는 검색 응답 파싱 진입점 ------------------ //

test('parsePoiResults_ev_charger_fixture_returns_all_10_pois', () => {
  assert.equal(parsePoiResults(json).length, 10);
});

test('parsePoiResults_ev_charger_fixture_keeps_every_poiId_and_pkey', () => {
  const pois = parsePoiResults(json);
  assert.deepEqual(pois.map(p => String(p.poiId)), raws.map(r => r.poiId));
  assert.deepEqual(pois.map(p => String(p.pkey)), raws.map(r => r.pkey));
});

test('parsePoiResults_ev_charger_fixture_uses_nav_coords_name_rpFlag_and_road_address', () => {
  const p = parsePoiResults(json)[0];
  assert.deepEqual(
    [p.name, Number(p.x), Number(p.y), Number(p.rpFlag), p.address],
    ['광장극동2차아파트 전기차충전소', 4575799, 1351473, 16, '서울 광진구 아차산로 552']
  );
});

// 두 파서 어느 쪽으로 들어가도 poiId 가 빠지지 않아야 한다.
test('both_poi_parsers_keep_every_poiId_for_ev_charger_fixture', () => {
  const expected = raws.map(r => r.poiId);
  assert.deepEqual(parsePoiSearchResponse(json).map(p => String(p.poiId)), expected);
  assert.deepEqual(parseRoutePoiResponse(json).map(p => String(p.poiId)), expected);
});

// ---- 검색 결과 → 경유지/목적지 요청 데이터 ---------------------------------- //

const fakeSk = () => [0, 0];

test('ev_charger_fixture_result_selected_as_dest_sets_destPoiId', () => {
  const p = parsePoiResults(json)[2];
  const dest = { lat: 37.5, lon: 127.1, name: p.name, rpFlag: p.rpFlag, poiId: p.poiId };
  const body = applyWaypointsToRouteBody({}, { depart: null, dest, vias: [] }, fakeSk);
  assert.equal(body.destPoiId, '11239559');
});

test('ev_charger_fixture_result_selected_as_via_sets_wayPoint_poiID', () => {
  const p = parsePoiResults(json)[9];
  const via = { lat: 37.5, lon: 127.1, name: p.name, rpFlag: p.rpFlag, poiId: p.poiId };
  const body = applyWaypointsToRouteBody({}, { depart: null, dest: null, vias: [via] }, fakeSk);
  assert.equal(body.wayPoints[0].poiID, '10943397');
});

// ---- 전기차 충전소 판단 --------------------------------------------------- //
//
// fastEvChargerYn / normalEvChargerYn / superFastChargerYn 중 하나라도 'Y' 면 전기차 충전소.

test('isEvChargerPoi_true_when_normalEvChargerYn_is_Y', () => {
  assert.equal(isEvChargerPoi({ fastEvChargerYn: 'N', normalEvChargerYn: 'Y', superFastChargerYn: 'N' }), true);
});

test('isEvChargerPoi_false_when_all_charger_flags_are_N_or_missing', () => {
  assert.equal(isEvChargerPoi({ fastEvChargerYn: 'N', normalEvChargerYn: 'N', superFastChargerYn: 'N' }), false);
  assert.equal(isEvChargerPoi({ name: '서울역' }), false);
});

// fixture 9번째(광장신동아파밀리에아파트 1)는 세 값 모두 'N' → 충전소 아님, 나머지 9건은 충전소.
test('parsePoiResults_ev_charger_fixture_marks_isEvCharger_per_charger_flags', () => {
  const flags = parsePoiResults(json).map(p => p.isEvCharger);
  assert.deepEqual(flags, [true, true, true, true, true, true, true, true, false, true]);
});

// ---- 충전소 검색 결과 → evChargerFlag / destEVChargerFlag ------------------ //

const emptyWps = () => ({ depart: null, dest: null, vias: [] });

test('ev_charger_result_selected_as_dest_sets_destEVChargerFlag_true', () => {
  const wps = emptyWps();
  wps.dest = waypointFromPoi(parsePoiResults(json)[0], 37.5, 127.1);
  assert.equal(applyWaypointsToRouteBody({ destEVChargerFlag: false }, wps, fakeSk).destEVChargerFlag, true);
});

test('ev_charger_result_selected_as_via_sets_evChargerFlag_true', () => {
  const wps = emptyWps();
  wps.vias.push(waypointFromPoi(parsePoiResults(json)[4], 37.5, 127.1));
  assert.equal(applyWaypointsToRouteBody({}, wps, fakeSk).wayPoints[0].evChargerFlag, true);
});

test('non_charger_result_keeps_ev_flags_false', () => {
  const p = parsePoiResults(json)[8];   // 세 충전기 플래그 모두 'N'
  const wps = { depart: null, dest: waypointFromPoi(p, 37.5, 127.1), vias: [waypointFromPoi(p, 37.5, 127.1)] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.deepEqual([body.destEVChargerFlag, body.wayPoints[0].evChargerFlag], [false, false]);
});

test('waypointFromPoi_keeps_name_rpFlag_and_poiId', () => {
  const wp = waypointFromPoi(parsePoiResults(json)[2], 37.5, 127.1);
  assert.deepEqual([wp.name, Number(wp.rpFlag), wp.poiId], ['대한제지 전기차충전소', 16, '11239559']);
});
