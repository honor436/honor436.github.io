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

test('REQUEST_TYPES_includes_poi_search_detail_and_route_poi', () => {
  const ids = REQUEST_TYPES.map(r => r.id);
  assert.ok(ids.includes('findpois'), 'POI 검색');
  assert.ok(ids.includes('findpoidetails'), 'POI 상세');
  assert.ok(ids.includes('routepoisearch'), '경로상 POI 검색');
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

// ---- 구현 상태 ------------------------------------------------------------ //

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

// ---- 경로상 POI 검색 (findpoisbyroute/v2) --------------------------------- //
//
// 메뉴: 경로상 POI 검색 → /tmap-channel/poi/search/findpoisbyroute/v2
// 기존 '카테고리 검색'/'경로상 POI(poisByRoute, 미구현)' 은 이 메뉴로 대체·제거.

test('REQUEST_TYPES_includes_route_poi_search', () => {
  const t = getRequestType('routepoisearch');
  assert.ok(t, '경로상 POI 검색 메뉴가 등록되어 있어야 한다');
  assert.equal(t.label, '경로상 POI 검색');
  assert.equal(t.path, '/tmap-channel/poi/search/findpoisbyroute/v2');
  assert.equal(t.method, 'POST');
  assert.notEqual(t.implemented, false);
});

test('REQUEST_TYPES_route_poi_sample_body_matches_route_search_request', () => {
  const body = getRequestType('routepoisearch').sampleBody();
  assert.equal(body.referrer_code, 'routeSearchPoiev');
  assert.equal(body.sort, 'distance');
  assert.equal(body.radius, '100');
  assert.equal(body.page_size, 1);
  assert.ok(Array.isArray(body.line_string) && body.line_string.length > 0, '샘플은 line_string 을 포함해 형식을 보여준다');
  assert.ok(Array.isArray(body.link_id) && body.link_id.length > 0, '샘플은 link_id 를 포함한다');
  assert.equal(body.header.svcType, 113);
});

test('REQUEST_TYPES_removes_old_category_and_poisbyroute', () => {
  assert.equal(getRequestType('categorysearch'), null, '카테고리 검색은 제거됨');
  assert.equal(getRequestType('poisbyroute'), null, '기존 경로상 POI(poisByRoute) 미구현 메뉴는 제거됨');
});
