import test from 'node:test';
import assert from 'node:assert/strict';
import {
  TEST_ENVS,
  buildTestUrl,
  REQUEST_TYPES,
  getRequestType,
} from '../server-test/tester.js';

// ---- TEST_ENVS ------------------------------------------------------------ //
//
// 서버 테스트 페이지 환경: 상용 / 준상용 / 개발.

test('TEST_ENVS_has_prod_stg_dev_korean_labels', () => {
  assert.equal(TEST_ENVS.prod.label, '상용');
  assert.equal(TEST_ENVS.stg.label, '준상용');
  assert.equal(TEST_ENVS.dev.label, '개발');
});

// ---- buildTestUrl --------------------------------------------------------- //

test('buildTestUrl_joins_env_host_and_path', () => {
  assert.equal(
    buildTestUrl('prod', '/tmap-channel/poi/search/findpois'),
    'https://ntmap.tmap.co.kr:9443/tmap-channel/poi/search/findpois'
  );
  assert.equal(
    buildTestUrl('dev', '/tmap-channel/poi/search/findpois'),
    'https://ntmapdev.tmap.co.kr:9443/tmap-channel/poi/search/findpois'
  );
});

test('buildTestUrl_normalizes_missing_leading_slash', () => {
  assert.equal(
    buildTestUrl('stg', 'tmap-channel/x'),
    'https://ntmapstg.tmap.co.kr:9443/tmap-channel/x'
  );
});

test('buildTestUrl_unknown_env_throws', () => {
  assert.throws(() => buildTestUrl('qa', '/x'));
});

// ---- REQUEST_TYPES registry (확장 가능) ----------------------------------- //
//
// 메뉴는 이 배열로 렌더된다. 새 요청 타입은 항목만 추가하면 메뉴에 노출됨.

test('REQUEST_TYPES_includes_poi_search_detail_and_poisbyroute', () => {
  const ids = REQUEST_TYPES.map(r => r.id);
  assert.ok(ids.includes('findpois'), 'POI 검색');
  assert.ok(ids.includes('findpoidetails'), 'POI 상세');
  assert.ok(ids.includes('poisbyroute'), 'poisByRoute');
});

test('REQUEST_TYPES_each_has_label_and_path', () => {
  for (const r of REQUEST_TYPES) {
    assert.ok(typeof r.id === 'string' && r.id.length > 0);
    assert.ok(typeof r.label === 'string' && r.label.length > 0);
    assert.ok(typeof r.path === 'string' && r.path.startsWith('/'));
    assert.equal(typeof r.sampleBody, 'function');
  }
});

test('getRequestType_returns_entry_with_sample_body', () => {
  const t = getRequestType('findpois');
  assert.ok(t);
  const body = t.sampleBody();
  assert.equal(body.name, '스타벅스');
});

test('getRequestType_unknown_returns_null', () => {
  assert.equal(getRequestType('nope'), null);
});

// ---- 구현 상태 (미구현 표시) ---------------------------------------------- //

test('REQUEST_TYPES_poisbyroute_is_marked_unimplemented', () => {
  assert.equal(getRequestType('poisbyroute').implemented, false);
});

test('REQUEST_TYPES_poi_search_and_detail_are_implemented', () => {
  assert.notEqual(getRequestType('findpois').implemented, false);
  assert.notEqual(getRequestType('findpoidetails').implemented, false);
});

// ---- 도달 가능 거리 조회 (isochrone) -------------------------------------- //
//
// 새 메뉴: 도달 가능 거리 조회 → /tmap-channel/rsd/route/isochrone

test('REQUEST_TYPES_includes_isochrone_reachable_distance', () => {
  const t = getRequestType('isochrone');
  assert.ok(t, '도달 가능 거리 조회 메뉴가 등록되어 있어야 한다');
  assert.equal(t.label, '도달 가능 거리 조회');
  assert.equal(t.path, '/tmap-channel/rsd/route/isochrone');
  assert.equal(t.method, 'POST');
  assert.notEqual(t.implemented, false);
});

test('REQUEST_TYPES_isochrone_sample_body_matches_contours_request', () => {
  const body = getRequestType('isochrone').sampleBody();
  assert.equal(body.contoursEnergy, 56000);
  assert.equal(body.contoursMeters, 300000);
  assert.equal(body.departXPos, 4575789);
  assert.equal(body.departYPos, 1345346);
  assert.equal(typeof body.consumptionParam, 'string');
  assert.equal(JSON.parse(body.consumptionParam).batteryCapacity, 80000);
  assert.equal(body.header.reqTime, '');
});
