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
  buildRoutePoiSearchBody,
  parseRoutePoiResponse,
  FINDPOISBYROUTE_V2_PATH,
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

// 일반 POI 검색(findpois) 응답도 POI 채널이라 snake_case(poiSearches/center_x)로
// 내려올 수 있다. 이 형태에서 결과가 0건이면 검색이 "동작하지 않는" 것처럼 보인다.
test('parsePoiSearchResponse_handles_snake_case_poiSearches', () => {
  const json = { poiSearches: [
    { poi_id: '1', pkey: '11', name: '스타벅스 강남', org_name: '스타벅스 강남점',
      center_x: 4573200, center_y: 1350142, full_address_road: '서울 강남구 봉은사로 151', rp_flag: 16 },
  ] };
  const pois = parsePoiSearchResponse(json);
  assert.equal(pois.length, 1);
  assert.equal(pois[0].name, '스타벅스 강남');
  assert.equal(pois[0].x, 4573200);
  assert.equal(pois[0].y, 1350142);
  assert.equal(pois[0].address, '서울 강남구 봉은사로 151');
  assert.equal(pois[0].poiId, '1');
  assert.equal(pois[0].pkey, '11');
  assert.equal(pois[0].rpFlag, 16);
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

// ---- 경로상 POI 검색 (findpoisbyroute/v2) --------------------------------- //
//
// 현재 경로가 있으면 vertex 좌표열을 line_string 으로, RPLINK 를 link_id 로 넣고
// start/end_point 를 채운다. 경로가 없으면 user_point(지도 중심)만 보낸다.
// 좌표는 WGS84 {lon, lat}, 엔드포인트는 /poi/search/findpoisbyroute/v2.

test('FINDPOISBYROUTE_V2_PATH_is_v2_endpoint', () => {
  assert.equal(FINDPOISBYROUTE_V2_PATH, '/tmap-channel/poi/search/findpoisbyroute/v2');
});

test('buildRoutePoiSearchBody_has_fixed_route_search_fields', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.5, lon: 127.0 } });
  assert.equal(body.referrer_code, 'routeSearchPoiev');
  assert.equal(body.sort, 'distance');
  assert.equal(body.radius, '0');       // 데이터 클래스 기본값
  assert.equal(body.page_no, 1);
  assert.equal(body.page_size, 1);
});

// referrer_code / sort / radius / page 는 옵션으로 바꿀 수 있어야 한다.
test('buildRoutePoiSearchBody_options_override_mode_sort_radius_page', () => {
  const body = buildRoutePoiSearchBody({
    userPoint: { lat: 37.5, lon: 127.0 },
    options: { referrerCode: 'radiusSearchPoiev', sort: 'evcharger', radius: '3', pageNo: 2, pageSize: 30 },
  });
  assert.equal(body.referrer_code, 'radiusSearchPoiev');
  assert.equal(body.sort, 'evcharger');
  assert.equal(body.radius, '3');
  assert.equal(body.page_no, 2);
  assert.equal(body.page_size, 30);
});

// 데이터 클래스의 선택 필드는 값이 있을 때만 바디에 포함된다.
test('buildRoutePoiSearchBody_includes_optional_filters_when_set', () => {
  const body = buildRoutePoiSearchBody({
    userPoint: { lat: 37.5, lon: 127.0 },
    options: {
      direction: 'all', groupKeyword: '주유소', operatorId: 'GS', powerType: '급속',
      evChargeType: 'DC_COMBO', evChargeStatus: 'CHARGING_STANDBY', openNowYn: 'Y',
      evPublicType: 'public', evKwMinvalue: 100, pickupYn: 'Y', totalDistance: 12000,
    },
  });
  assert.equal(body.direction, 'all');
  assert.equal(body.group_keyword, '주유소');
  assert.equal(body.operator_id, 'GS');
  assert.equal(body.power_type, '급속');
  assert.equal(body.ev_charge_type, 'DC_COMBO');
  assert.equal(body.ev_charge_status, 'CHARGING_STANDBY');
  assert.equal(body.open_now_yn, 'Y');
  assert.equal(body.ev_public_type, 'public');
  assert.equal(body.ev_kw_minvalue, 100);
  assert.equal(body.pickup_yn, 'Y');
  assert.equal(body.total_distance, 12000);
});

test('buildRoutePoiSearchBody_omits_optional_filters_when_unset', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.5, lon: 127.0 } });
  for (const k of ['direction', 'group_keyword', 'operator_id', 'power_type', 'ev_charge_type',
    'ev_charge_status', 'open_now_yn', 'ev_public_type', 'ev_kw_minvalue', 'pickup_yn',
    'total_distance', 'tmap_private_ev_yn', 'tnow_display_yn', 'geo_polygon']) {
    assert.equal(k in body, false, `${k} 는 값이 없으면 바디에서 제외돼야 한다`);
  }
});

// 한전 PnC 필터는 데이터 클래스 기본값(N/"")으로 항상 포함된다.
test('buildRoutePoiSearchBody_pnc_defaults_present', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.5, lon: 127.0 } });
  assert.equal(body.ev_pnc_yn, 'N');
  assert.equal(body.ev_pnc_oem, '');
});

test('buildRoutePoiSearchBody_always_sets_user_point', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.56645, lon: 126.98502 } });
  assert.deepEqual(body.user_point, { lon: 126.98502, lat: 37.56645 });
});

test('buildRoutePoiSearchBody_without_route_omits_line_string_and_links', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.5, lon: 127.0 } });
  assert.equal(body.line_string, undefined);
  assert.equal(body.link_id, undefined);
  assert.equal(body.start_point, undefined);
  assert.equal(body.end_point, undefined);
});

test('buildRoutePoiSearchBody_with_route_builds_line_string_coordinates', () => {
  const coords = [
    { lat: 37.566, lon: 126.985 },
    { lat: 37.567, lon: 126.984 },
    { lat: 37.568, lon: 126.983 },
  ];
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.566, lon: 126.985 }, lineStringCoords: coords });
  assert.equal(Array.isArray(body.line_string), true);
  assert.equal(body.line_string.length, 1);
  assert.equal(body.line_string[0].road_type, 0);
  assert.deepEqual(body.line_string[0].coordinates[0], { lon: 126.985, lat: 37.566 });
  assert.equal(body.line_string[0].coordinates.length, 3);
});

test('buildRoutePoiSearchBody_with_route_sets_start_end_from_coords', () => {
  const coords = [
    { lat: 37.566, lon: 126.985 },
    { lat: 37.568, lon: 126.983 },
  ];
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.566, lon: 126.985 }, lineStringCoords: coords });
  assert.deepEqual(body.start_point, { lon: 126.985, lat: 37.566 });
  assert.deepEqual(body.end_point, { lon: 126.983, lat: 37.568 });
});

test('buildRoutePoiSearchBody_with_route_includes_link_ids', () => {
  const body = buildRoutePoiSearchBody({
    userPoint: { lat: 37.566, lon: 126.985 },
    lineStringCoords: [{ lat: 37.566, lon: 126.985 }, { lat: 37.567, lon: 126.984 }],
    linkIds: ['4787_763_0', '4787_765_1'],
  });
  assert.deepEqual(body.link_id, ['4787_763_0', '4787_765_1']);
});

test('buildRoutePoiSearchBody_header_svctype_113_with_empty_reqtime', () => {
  const body = buildRoutePoiSearchBody({ userPoint: { lat: 37.5, lon: 127.0 } });
  assert.ok(body.header, 'route poi body must carry a header');
  assert.equal(body.header.svcType, 113);
  assert.equal(body.header.reqTime, '');
});

// ---- parseRoutePoiResponse (poiSearches, snake_case) ---------------------- //

test('parseRoutePoiResponse_maps_poiSearches_to_normalized_pois', () => {
  const json = { poiSearches: [
    { poi_id: '11325648', pkey: '1132564801', name: '전기차충전소', org_name: '분데스언주 전기차충전소',
      center_x: 4573200, center_y: 1350142, full_address_road: '서울 강남구 봉은사로 151', rp_flag: 16 },
  ] };
  const pois = parseRoutePoiResponse(json);
  assert.equal(pois.length, 1);
  assert.equal(pois[0].name, '전기차충전소');
  assert.equal(pois[0].x, 4573200);
  assert.equal(pois[0].y, 1350142);
  assert.equal(pois[0].address, '서울 강남구 봉은사로 151');
  assert.equal(pois[0].poiId, '11325648');
  assert.equal(pois[0].pkey, '1132564801');
  assert.equal(pois[0].rpFlag, 16);
});

test('parseRoutePoiResponse_returns_empty_for_missing_data', () => {
  assert.deepEqual(parseRoutePoiResponse(null), []);
  assert.deepEqual(parseRoutePoiResponse({}), []);
  assert.deepEqual(parseRoutePoiResponse({ poiSearches: [] }), []);
});
