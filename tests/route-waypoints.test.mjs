import test from 'node:test';
import assert from 'node:assert/strict';
import { applyWaypointsToRouteBody, updateWaypointField, buildWaypointEditorHtml } from '../DltLogViewer/js/route-waypoints.js';

// 좌표 변환은 테스트에서 고정값으로 대체한다 (변환 자체는 coordinate.test.mjs 가 검증).
const fakeSk = (lat, lon) => [Math.round(lat * 1000), Math.round(lon * 1000)];

// ---- applyWaypointsToRouteBody ------------------------------------------- //
//
// 경로 요청 패널의 출발/경유/목적지 상태를 요청 바디(JSON)에 반영한다.

test('applyWaypointsToRouteBody_via_evChargerFlag_true_is_added_to_wayPoint', () => {
  const wps = { depart: null, dest: null, vias: [{ lat: 37, lon: 127, evChargerFlag: true }] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.equal(body.wayPoints[0].evChargerFlag, true);
});

test('applyWaypointsToRouteBody_via_without_evChargerFlag_sends_false', () => {
  const wps = { depart: null, dest: null, vias: [{ lat: 37, lon: 127 }] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.equal(body.wayPoints[0].evChargerFlag, false);
});

test('applyWaypointsToRouteBody_dest_evChargerFlag_sets_destEVChargerFlag', () => {
  const wps = { depart: null, dest: { lat: 35, lon: 128, evChargerFlag: true }, vias: [] };
  const body = applyWaypointsToRouteBody({ destEVChargerFlag: false }, wps, fakeSk);
  assert.equal(body.destEVChargerFlag, true);
});

test('applyWaypointsToRouteBody_dest_without_evChargerFlag_sends_false', () => {
  const wps = { depart: null, dest: { lat: 35, lon: 128 }, vias: [] };
  const body = applyWaypointsToRouteBody({ destEVChargerFlag: true }, wps, fakeSk);
  assert.equal(body.destEVChargerFlag, false);
});

test('applyWaypointsToRouteBody_no_vias_removes_wayPoints_after_delete', () => {
  const wps = { depart: null, dest: null, vias: [] };
  const body = applyWaypointsToRouteBody({ wayPoints: [{ x: 1, y: 2 }] }, wps, fakeSk);
  assert.equal('wayPoints' in body, false);
});

test('applyWaypointsToRouteBody_depart_sets_sk_pos_name_and_angle', () => {
  const wps = { depart: { lat: 37.5, lon: 127.1, name: '서울역', angle: 45 }, dest: null, vias: [] };
  const body = applyWaypointsToRouteBody({ angle: 130 }, wps, fakeSk);
  assert.deepEqual(
    [body.departXPos, body.departYPos, body.departName, body.angle],
    [37500, 127100, '서울역', 45]
  );
});

test('applyWaypointsToRouteBody_dest_non_poi_clears_sample_poiId_and_uses_default_rpFlag', () => {
  const wps = { depart: null, dest: { lat: 35, lon: 128 }, vias: [] };
  const body = applyWaypointsToRouteBody({ destPoiId: '501087', destRpFlag: 1 }, wps, fakeSk);
  assert.deepEqual([body.destPoiId, body.destRpFlag], ['', 16]);
});

test('applyWaypointsToRouteBody_via_keeps_order_name_rpFlag_and_poiID', () => {
  const wps = { depart: null, dest: null, vias: [
    { lat: 36, lon: 127, name: '휴게소', rpFlag: 7, poiId: 123 },
    { lat: 36.5, lon: 127.5 },
  ] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.deepEqual(body.wayPoints.map(w => [w.wayPointName, w.rpFlag, w.poiID]), [
    ['휴게소', 7, '123'],
    ['경유지2', 18, ''],
  ]);
});

test('applyWaypointsToRouteBody_does_not_mutate_input_body', () => {
  const input = { wayPoints: [] };
  applyWaypointsToRouteBody(input, { depart: null, dest: null, vias: [{ lat: 1, lon: 2 }] }, fakeSk);
  assert.deepEqual(input, { wayPoints: [] });
});

// ---- updateWaypointField -------------------------------------------------- //
//
// 패널 카드의 입력값(문자열/체크박스)을 지점 상태에 반영한다. 잘못된 값이면 false.

test('updateWaypointField_lat_string_is_parsed_as_number', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'lat', '37.123456'), true);
  assert.equal(wp.lat, 37.123456);
});

test('updateWaypointField_invalid_lat_is_rejected_and_unchanged', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'lat', 'abc'), false);
  assert.equal(updateWaypointField(wp, 'lat', '95'), false);
  assert.equal(updateWaypointField(wp, 'lon', ''), false);
  assert.deepEqual(wp, { lat: 37, lon: 127 });
});

test('updateWaypointField_evChargerFlag_takes_checkbox_boolean', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'evChargerFlag', true), true);
  assert.equal(wp.evChargerFlag, true);
  updateWaypointField(wp, 'evChargerFlag', false);
  assert.equal(wp.evChargerFlag, false);
});

test('updateWaypointField_name_is_trimmed_and_blank_clears_it', () => {
  const wp = { lat: 37, lon: 127, name: '이전' };
  updateWaypointField(wp, 'name', '  서울역 ');
  assert.equal(wp.name, '서울역');
  updateWaypointField(wp, 'name', '   ');
  assert.equal(wp.name, undefined);
});

test('updateWaypointField_rpFlag_integer_or_blank_for_default', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'rpFlag', '7'), true);
  assert.equal(wp.rpFlag, 7);
  assert.equal(updateWaypointField(wp, 'rpFlag', '1.5'), false);
  assert.equal(wp.rpFlag, 7);
  updateWaypointField(wp, 'rpFlag', '');
  assert.equal(wp.rpFlag, undefined);
});

test('updateWaypointField_angle_accepts_0_to_359_and_blank_clears', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'angle', '270'), true);
  assert.equal(wp.angle, 270);
  assert.equal(updateWaypointField(wp, 'angle', '360'), false);
  assert.equal(updateWaypointField(wp, 'angle', '-1'), false);
  assert.equal(wp.angle, 270);
  updateWaypointField(wp, 'angle', '');
  assert.equal(wp.angle, undefined);
});

test('updateWaypointField_poiId_is_trimmed_and_blank_clears', () => {
  const wp = { lat: 37, lon: 127, poiId: '1' };
  updateWaypointField(wp, 'poiId', ' 501087 ');
  assert.equal(wp.poiId, '501087');
  updateWaypointField(wp, 'poiId', '');
  assert.equal(wp.poiId, undefined);
});

test('updateWaypointField_unknown_field_is_rejected', () => {
  const wp = { lat: 37, lon: 127 };
  assert.equal(updateWaypointField(wp, 'x', '1'), false);
  assert.deepEqual(wp, { lat: 37, lon: 127 });
});

// ---- buildWaypointEditorHtml ---------------------------------------------- //
//
// 출발 → 경유(순서대로) → 목적지를 세로 카드로 렌더링. 펼친 카드만 상세 입력을 포함한다.

const keysInOrder = (html) => [...html.matchAll(/data-wp-key="([^"]+)"/g)].map(m => m[1]);

test('buildWaypointEditorHtml_orders_depart_vias_dest_vertically', () => {
  const wps = { depart: { lat: 1, lon: 2 }, dest: { lat: 5, lon: 6 }, vias: [{ lat: 3, lon: 4 }, { lat: 3.5, lon: 4.5 }] };
  const html = buildWaypointEditorHtml(wps, new Set(), fakeSk);
  assert.deepEqual(keysInOrder(html), ['depart', 'via:0', 'via:1', 'dest']);
});

test('buildWaypointEditorHtml_empty_shows_right_click_hint', () => {
  const html = buildWaypointEditorHtml({ depart: null, dest: null, vias: [] }, new Set(), fakeSk);
  assert.match(html, /지도 우클릭/);
  assert.equal(keysInOrder(html).length, 0);
});

test('buildWaypointEditorHtml_collapsed_card_has_no_detail_inputs', () => {
  const wps = { depart: { lat: 1, lon: 2 }, dest: null, vias: [] };
  const html = buildWaypointEditorHtml(wps, new Set(), fakeSk);
  assert.doesNotMatch(html, /data-wp-field=/);
});

test('buildWaypointEditorHtml_expanded_card_shows_editable_name_lat_lon_and_sk', () => {
  const wps = { depart: { lat: 37.5, lon: 127.1, name: '서울역' }, dest: null, vias: [] };
  const html = buildWaypointEditorHtml(wps, new Set(['depart']), fakeSk);
  assert.match(html, /data-wp-field="name"[^>]*value="서울역"/);
  assert.match(html, /data-wp-field="lat"[^>]*value="37.500000"/);
  assert.match(html, /data-wp-field="lon"[^>]*value="127.100000"/);
  assert.match(html, /37500, 127100/);
});

test('buildWaypointEditorHtml_via_card_has_evChargerFlag_checkbox_and_delete_button', () => {
  const wps = { depart: null, dest: null, vias: [{ lat: 1, lon: 2, evChargerFlag: true }] };
  const html = buildWaypointEditorHtml(wps, new Set(['via:0']), fakeSk);
  assert.match(html, /type="checkbox" data-wp-field="evChargerFlag" checked/);
  assert.match(html, /data-wp-delete/);
});

test('buildWaypointEditorHtml_dest_card_has_destEVChargerFlag_checkbox_unchecked_by_default', () => {
  const wps = { depart: null, dest: { lat: 5, lon: 6 }, vias: [] };
  const html = buildWaypointEditorHtml(wps, new Set(['dest']), fakeSk);
  assert.match(html, /destEVChargerFlag/);
  assert.match(html, /type="checkbox" data-wp-field="evChargerFlag">/);
});

test('buildWaypointEditorHtml_via_and_dest_cards_edit_rpFlag_and_poiId_with_default_placeholder', () => {
  const wps = { depart: null, dest: { lat: 5, lon: 6, poiId: '501087' }, vias: [{ lat: 1, lon: 2, rpFlag: 7 }] };
  const html = buildWaypointEditorHtml(wps, new Set(['via:0', 'dest']), fakeSk);
  assert.match(html, /data-wp-field="rpFlag" value="7" placeholder="18"/);
  assert.match(html, /data-wp-field="rpFlag" value="" placeholder="16"/);
  assert.match(html, /data-wp-field="poiId" value="501087"/);
});

test('buildWaypointEditorHtml_depart_card_edits_angle_without_evChargerFlag', () => {
  const wps = { depart: { lat: 1, lon: 2, angle: 45 }, dest: null, vias: [] };
  const html = buildWaypointEditorHtml(wps, new Set(['depart']), fakeSk);
  assert.match(html, /data-wp-field="angle" value="45"/);
  assert.doesNotMatch(html, /evChargerFlag/);
});

test('buildWaypointEditorHtml_escapes_waypoint_name', () => {
  const wps = { depart: { lat: 1, lon: 2, name: '<b>"A"</b>' }, dest: null, vias: [] };
  const html = buildWaypointEditorHtml(wps, new Set(['depart']), fakeSk);
  assert.doesNotMatch(html, /<b>"A"/);
  assert.match(html, /&lt;b&gt;&quot;A&quot;/);
});

test('applyWaypointsToRouteBody_via_non_poi_sends_empty_string_poiID', () => {
  const wps = { depart: null, dest: null, vias: [{ lat: 36, lon: 127 }] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.equal(body.wayPoints[0].poiID, '');
});

test('applyWaypointsToRouteBody_numeric_poi_ids_are_sent_as_strings', () => {
  const wps = { depart: null, dest: { lat: 5, lon: 6, poiId: 501087 }, vias: [{ lat: 1, lon: 2, poiId: 42 }] };
  const body = applyWaypointsToRouteBody({}, wps, fakeSk);
  assert.equal(body.destPoiId, '501087');
  assert.equal(body.wayPoints[0].poiID, '42');
});

test('buildWaypointEditorHtml_labels_poi_fields_with_request_field_names', () => {
  const wps = { depart: null, dest: { lat: 5, lon: 6 }, vias: [{ lat: 1, lon: 2 }] };
  const html = buildWaypointEditorHtml(wps, new Set(['via:0', 'dest']), fakeSk);
  assert.match(html, /<span>poiID<\/span><input type="text" data-wp-field="poiId"/);
  assert.match(html, /<span>destPoiId<\/span><input type="text" data-wp-field="poiId"/);
});
