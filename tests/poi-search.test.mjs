import test from 'node:test';
import assert from 'node:assert/strict';
import {
  POI_SEARCH_ENVS,
  buildPoiSearchUrl,
  buildPoiSearchBody,
  parsePoiSearchResponse,
  coerceJson,
  buildPoiSearchHeaders,
  parseHeaderText,
  buildDefaultHeaderText,
  decodeTmapBody,
  extractTmapErrorHint,
  buildPoiDetailBody,
  derivePoiDetailUrl,
  resolveRpFlag,
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

// ---- coerceJson ----------------------------------------------------------- //
//
// findpois 응답이 순수 JSON 이 아닐 때(선행 BOM/공백/접두 바이트)도 최대한
// 살려서 객체로 파싱한다. 도저히 안 되면 null.

test('coerceJson_parses_plain_json', () => {
  assert.deepEqual(coerceJson('{"a":1}'), { a: 1 });
});

test('coerceJson_strips_bom_and_whitespace', () => {
  assert.deepEqual(coerceJson('﻿  {"a":1}  '), { a: 1 });
});

test('coerceJson_recovers_json_after_prefix_garbage', () => {
  assert.deepEqual(coerceJson('something{"a":1}'), { a: 1 });
});

test('coerceJson_returns_null_for_non_json', () => {
  assert.equal(coerceJson('totally not json'), null);
  assert.equal(coerceJson(''), null);
  assert.equal(coerceJson(null), null);
});

// ---- buildPoiSearchHeaders ------------------------------------------------ //
//
// findpois 서버가 요구하는 HTTP 헤더. Client_ReqTime 은 현재시간(yyyyMMddHHmmss),
// requestHashToken/CIH 는 기본값 제공(편집 가능). Connection/Content-Length 는
// 브라우저가 자동 설정하는 금지 헤더라 포함하지 않는다.

test('buildPoiSearchHeaders_has_required_static_headers', () => {
  const h = buildPoiSearchHeaders();
  assert.equal(h['Accept'], 'application/json');
  assert.equal(h['requestHashToken'], '1988490197');
  assert.equal(h['CIH'], '582079726');
});

test('buildPoiSearchHeaders_client_reqtime_is_14_digit_timestamp', () => {
  const h = buildPoiSearchHeaders();
  assert.match(h['Client_ReqTime'], /^\d{14}$/);
});

test('buildPoiSearchHeaders_does_not_set_forbidden_headers', () => {
  const h = buildPoiSearchHeaders();
  assert.equal(h['Connection'], undefined);
  assert.equal(h['Content-Length'], undefined);
});

test('buildPoiSearchHeaders_respects_reqTime_override', () => {
  const h = buildPoiSearchHeaders({ reqTime: '20210511180658' });
  assert.equal(h['Client_ReqTime'], '20210511180658');
});

// ---- parseHeaderText ------------------------------------------------------ //
//
// "Key: Value" 한 줄에 하나. '#' 주석/빈 줄 무시. 값이 비었거나 <...>
// 플레이스홀더면 전송 제외하고 skipped 로 보고 (Tmap 요청 테스터 규칙).

test('parseHeaderText_parses_key_value_lines', () => {
  const { headers } = parseHeaderText('Accept: application/json\nCIH: 123');
  assert.equal(headers['Accept'], 'application/json');
  assert.equal(headers['CIH'], '123');
});

test('parseHeaderText_ignores_comments_and_blanks', () => {
  const { headers } = parseHeaderText('# 주석\n\nAccept: application/json\n');
  assert.deepEqual(Object.keys(headers), ['Accept']);
});

test('parseHeaderText_skips_empty_and_placeholder_values', () => {
  const { headers, skipped } = parseHeaderText('AccessKey: <AccessKey>\nAccessToken:   \nCIH: 99');
  assert.equal(headers['CIH'], '99');
  assert.equal(headers['AccessKey'], undefined);
  assert.equal(headers['AccessToken'], undefined);
  assert.deepEqual(skipped.sort(), ['AccessKey', 'AccessToken']);
});

test('parseHeaderText_keeps_colons_in_value', () => {
  const { headers } = parseHeaderText('URL: https://a.b/c:9443/x');
  assert.equal(headers['URL'], 'https://a.b/c:9443/x');
});

// ---- buildDefaultHeaderText ----------------------------------------------- //

test('buildDefaultHeaderText_includes_required_auth_headers', () => {
  const t = buildDefaultHeaderText();
  assert.match(t, /AccessKey:/);
  assert.match(t, /AccessToken:/);
  assert.match(t, /Requester:\s*CLIENT_SSL/);
  assert.match(t, /Content-Type:\s*application\/json/);
});

test('buildDefaultHeaderText_client_reqtime_is_14_digits_and_overridable', () => {
  assert.match(buildDefaultHeaderText(), /Client_ReqTime:\s*\d{14}/);
  assert.match(buildDefaultHeaderText({ reqTime: '20210511180658' }), /Client_ReqTime:\s*20210511180658/);
});

// ---- decodeTmapBody ------------------------------------------------------- //
//
// Tmap 채널 응답은 JSON(utf-8) 이거나 CP949 바이너리. arrayBuffer 를 받아
// charset → utf-8 → euc-kr 순으로 디코딩한다.

test('decodeTmapBody_decodes_utf8_json', () => {
  const bytes = new TextEncoder().encode('{"a":1}');
  assert.equal(decodeTmapBody(bytes, 'application/json'), '{"a":1}');
});

test('decodeTmapBody_decodes_cp949_binary_as_korean', () => {
  // EUC-KR bytes for "서비스" — invalid as UTF-8, so euc-kr fallback kicks in
  const bytes = new Uint8Array([0xBC, 0xAD, 0xBA, 0xF1, 0xBD, 0xBA]);
  const out = decodeTmapBody(bytes, 'application/binary');
  assert.match(out, /서비스/);
});

// ---- extractTmapErrorHint ------------------------------------------------- //

test('extractTmapErrorHint_detects_channel_error_code', () => {
  const hint = extractTmapErrorHint('010100 NCH01000 서비스가 지연되고 있습니다.');
  assert.ok(hint && /NCH01000/.test(hint));
});

test('extractTmapErrorHint_returns_null_for_normal_json', () => {
  assert.equal(extractTmapErrorHint('{"poiInfo":{"pois":{"poi":[]}}}'), null);
});

// ---- pkey/poiId extraction ------------------------------------------------ //

test('parsePoiSearchResponse_extracts_pkey_and_poiId', () => {
  const json = { poiInfo: { pois: { poi: [
    { name: 'EV충전소 행당대림아파트', noorX: '4575887', noorY: '1351438', poiId: '10185460', pkey: '1018546001' },
  ] } } };
  const pois = parsePoiSearchResponse(json);
  assert.equal(pois[0].poiId, '10185460');
  assert.equal(pois[0].pkey, '1018546001');
});

// ---- buildPoiDetailBody --------------------------------------------------- //
//
// 검색 결과 선택 → POI 상세 조회 요청 바디(findOption=PKEY).

test('buildPoiDetailBody_sets_pkey_lookup_fields', () => {
  const body = buildPoiDetailBody({ name: 'EV충전소 행당대림아파트', poiId: '10185460', pkey: '1018546001' });
  assert.equal(body.findOption, 'PKEY');
  assert.equal(body.name, 'EV충전소 행당대림아파트');
  assert.equal(body.poiId, '10185460');
  assert.equal(body.pkey, '1018546001');
  assert.ok(body.header, 'detail body must carry a header');
});

test('buildPoiDetailBody_coerces_ids_to_string', () => {
  const body = buildPoiDetailBody({ name: 'x', poiId: 10185460, pkey: 1018546001 });
  assert.equal(body.poiId, '10185460');
  assert.equal(body.pkey, '1018546001');
});

// ---- derivePoiDetailUrl --------------------------------------------------- //
//
// 상세 URL 은 검색 URL 의 호스트를 그대로 쓰고 실제 상세 경로
// (/tmap-channel/poi/detailinfo/findpoidetailinfoforauto) 로 도출한다.

test('derivePoiDetailUrl_uses_detailinfo_path_on_same_host', () => {
  assert.equal(
    derivePoiDetailUrl('https://ntmapdev.tmap.co.kr:9443/tmap-channel/poi/search/findpois'),
    'https://ntmapdev.tmap.co.kr:9443/tmap-channel/poi/detailinfo/findpoidetailinfoforauto'
  );
});

test('derivePoiDetailUrl_keeps_custom_host', () => {
  assert.equal(
    derivePoiDetailUrl('https://tmap-channel-aws.tmobiapi.com/tmap-channel/poi/search/findpois'),
    'https://tmap-channel-aws.tmobiapi.com/tmap-channel/poi/detailinfo/findpoidetailinfoforauto'
  );
});

// ---- POI detail header (제공된 상세 샘플 기준: svcType 114) ---------------- //

test('buildPoiDetailBody_uses_detail_header_svctype_114', () => {
  const body = buildPoiDetailBody({ name: 'x', poiId: '1', pkey: '11' });
  assert.equal(body.header.svcType, 114);
  assert.equal(body.header.appVersion, '1.0.13');
});

// ---- HTML 게이트웨이 오류 페이지 감지 ------------------------------------- //

test('extractTmapErrorHint_detects_html_error_page', () => {
  const html = '<html><head><title></title></head><body>서비스 이용에 불편을 드려 죄송합니다.</body></html>';
  const hint = extractTmapErrorHint(html);
  assert.ok(hint && /(HTML|엔드포인트|경로)/.test(hint));
});

// ---- rpFlag (경유지 18 / 목적지 16, POI rpFlag 우선) ---------------------- //

test('parsePoiSearchResponse_extracts_rpFlag', () => {
  const json = { poiInfo: { pois: { poi: [
    { name: '행당대림', noorX: '4575887', noorY: '1351438', rpFlag: 7 },
  ] } } };
  assert.equal(parsePoiSearchResponse(json)[0].rpFlag, 7);
});

test('resolveRpFlag_defaults_via_18_dest_16', () => {
  assert.equal(resolveRpFlag('via'), 18);
  assert.equal(resolveRpFlag('dest'), 16);
});

test('resolveRpFlag_prefers_poi_rpFlag', () => {
  assert.equal(resolveRpFlag('via', 7), 7);
  assert.equal(resolveRpFlag('dest', '5'), 5);
});

test('resolveRpFlag_ignores_empty_or_nan_poi_rpFlag', () => {
  assert.equal(resolveRpFlag('via', null), 18);
  assert.equal(resolveRpFlag('via', ''), 18);
  assert.equal(resolveRpFlag('dest', 'abc'), 16);
});
