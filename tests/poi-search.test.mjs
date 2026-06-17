import test from 'node:test';
import assert from 'node:assert/strict';
import {
  POI_SEARCH_ENVS,
  buildPoiSearchUrl,
  buildPoiSearchBody,
  parsePoiSearchResponse,
} from '../DltLogViewer/js/poi-search.js';

// ---- buildPoiSearchUrl ---------------------------------------------------- //
//
// 상용(prod)/스테이징(stg)/개발(dev) 서버별로 findpois 엔드포인트 URL을 만든다.
//   dev  → ntmapdev.tmap.co.kr
//   stg  → ntmapstg.tmap.co.kr
//   prod → ntmap.tmap.co.kr
// 포트 9443, 경로 /tmap-channel/poi/search/findpois 는 공통.

test('buildPoiSearchUrl_dev_uses_ntmapdev_host', () => {
  assert.equal(
    buildPoiSearchUrl('dev'),
    'https://ntmapdev.tmap.co.kr:9443/tmap-channel/poi/search/findpois'
  );
});

test('buildPoiSearchUrl_stg_uses_ntmapstg_host', () => {
  assert.equal(
    buildPoiSearchUrl('stg'),
    'https://ntmapstg.tmap.co.kr:9443/tmap-channel/poi/search/findpois'
  );
});

test('buildPoiSearchUrl_prod_uses_ntmap_host', () => {
  assert.equal(
    buildPoiSearchUrl('prod'),
    'https://ntmap.tmap.co.kr:9443/tmap-channel/poi/search/findpois'
  );
});

test('buildPoiSearchUrl_unknown_env_throws', () => {
  assert.throws(() => buildPoiSearchUrl('qa'));
});

test('POI_SEARCH_ENVS_has_three_korean_labels', () => {
  assert.equal(POI_SEARCH_ENVS.dev.label, '개발');
  assert.equal(POI_SEARCH_ENVS.stg.label, '스테이징');
  assert.equal(POI_SEARCH_ENVS.prod.label, '상용');
});

// ---- buildPoiSearchBody --------------------------------------------------- //
//
// 검색 키워드 + 현재 좌표(SK noorX/noorY)로 findpois 요청 바디를 만든다.

test('buildPoiSearchBody_sets_keyword_as_name', () => {
  const body = buildPoiSearchBody({ keyword: 'BMW 드라이빙센터', noorX: 4575887, noorY: 1351438 });
  assert.equal(body.name, 'BMW 드라이빙센터');
});

test('buildPoiSearchBody_sets_current_coords_as_string', () => {
  const body = buildPoiSearchBody({ keyword: '스타벅스', noorX: 4575887, noorY: 1351438 });
  assert.equal(body.noorX, '4575887');
  assert.equal(body.noorY, '1351438');
});

test('buildPoiSearchBody_has_fixed_search_engine_fields', () => {
  const body = buildPoiSearchBody({ keyword: 'x', noorX: 1, noorY: 2 });
  assert.equal(body.reqCnt, 70);
  assert.equal(body.searchTypCd, 'A');
  assert.equal(body.reqSearchEngineInfo.searchMethod, 'aut.kwd');
  assert.ok(body.header, 'body must carry a header');
});

// ---- parsePoiSearchResponse ----------------------------------------------- //
//
// findpois 응답에서 POI 목록을 추출해 { name, x, y, address } 로 정규화한다.
// 좌표는 SK(noorX/noorY)로 내려온다.

test('parsePoiSearchResponse_extracts_pois_from_poiInfo', () => {
  const json = {
    poiInfo: {
      totalCnt: '1',
      pois: {
        poi: [
          { name: 'BMW 드라이빙센터', noorX: '4575887', noorY: '1351438', fullAddressRoad: '인천 영종', tel: '02-123' },
        ],
      },
    },
  };
  const pois = parsePoiSearchResponse(json);
  assert.equal(pois.length, 1);
  assert.equal(pois[0].name, 'BMW 드라이빙센터');
  assert.equal(pois[0].x, 4575887);
  assert.equal(pois[0].y, 1351438);
  assert.equal(pois[0].address, '인천 영종');
});

test('parsePoiSearchResponse_returns_empty_for_missing_data', () => {
  assert.deepEqual(parsePoiSearchResponse(null), []);
  assert.deepEqual(parsePoiSearchResponse({}), []);
});

test('parsePoiSearchResponse_deep_scans_when_shape_differs', () => {
  const json = { result: { searchPoiInfo: { pois: { poi: [
    { poiName: '스타벅스 강남', frontX: '4575000', frontY: '1351000' },
  ] } } } };
  const pois = parsePoiSearchResponse(json);
  assert.equal(pois.length, 1);
  assert.equal(pois[0].name, '스타벅스 강남');
  assert.equal(pois[0].x, 4575000);
  assert.equal(pois[0].y, 1351000);
});
