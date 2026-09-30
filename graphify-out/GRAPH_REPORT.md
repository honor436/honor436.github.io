# Graph Report - honor436.github.io  (2026-09-30)

## Corpus Check
- 68 files · ~161,299 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1248 nodes · 2325 edges · 71 communities (63 shown, 8 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 60 edges (avg confidence: 0.72)
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `5cc78f31`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- 좌표 변환·DLT 파서
- ImageMapper 앱
- DltLogViewer-beta/js/extractor.js
- SHP 앱 UI (beta)
- SHP 앱 UI
- POI 검색·서버 테스트
- DltLogViewer/js/tvas-renderer.js
- 지도·SHP 레이어 (beta)
- TVAS 바이너리 파서
- 좌표검색 앱 UI (js/app.js)
- POI 검색 (beta)
- DltLogViewer-beta/js/tvas-parser.js
- TVAS 코드 테이블 (beta)
- TeamRoulette 룰렛
- RTM 교통정보
- ADB 릴레이 (Python)
- ADB 릴레이 (beta)
- DltLogViewer/js/map-viewer.js
- 주유소·휴게소 팝업
- 지도 뷰어 마커 (beta)
- 경로요청·등시선
- SHP 레이어
- 뷰어 인라인 스크립트
- 정체·경로 세그먼트
- 인증 게이트·도구 페이지
- 경로요청·등시선 (beta)
- 도로·교차로 명칭 라벨
- 휴게소·경유지 (beta)
- 도움말 개념
- 도로·교차로 라벨 (beta)
- 주유소·EV 팝업
- 경로 세그먼트 (beta)
- package.json
- RO4·방향 화살표
- RO4·방향 화살표 (beta)
- CLAUDE.md TDD 지침
- GPS 분석 흐름
- GPS 분석 흐름 (beta)
- 안내점 겹침 처리
- SHP 워커 (beta)
- SHP 워커
- EV 충전소 (beta)
- MockGPS 재생 보간
- Beta 경로 POI 검색
- MockGPS 재생 (beta)
- 정체 라벨 (beta)
- 위험구간 렌더링
- 안내점 (beta)
- 설치 스크립트 sh (beta)
- 좌표검색·CORS
- Mock GPS 재생 UI
- Mock GPS 설치
- 설치 스크립트 sh
- GPS 분석 (app.js)
- 경유지 마커
- 차선 안내
- 경유지 마커 (beta)
- 차선 안내 (beta)
- 역지오코딩 (beta)
- 역지오코딩
- 파일 스캔 (app.js)
- 인덱스 도구 테스트
- DltLogViewer/js/build-info.js
- 샘플 캐러셀
- 서버테스트 응답 표시
- README
- 캐러셀 정지
- pre-commit

## God Nodes (most connected - your core abstractions)
1. `DltLogViewer index.html (내비 데이터 뷰어)` - 33 edges
2. `parseTvas()` - 26 edges
3. `renderTvasRoute()` - 26 edges
4. `parseTvas()` - 26 edges
5. `renderTvasRoute()` - 26 edges
6. `DltLogViewer help.html (도움말)` - 18 edges
7. `esc()` - 15 edges
8. `esc()` - 15 edges
9. `readString()` - 13 edges
10. `readString()` - 13 edges

## Surprising Connections (you probably didn't know these)
- `DltLogViewer-beta help.html (도움말)` --semantically_similar_to--> `DltLogViewer help.html (도움말)`  [INFERRED] [semantically similar]
  DltLogViewer-beta/help.html → DltLogViewer/help.html
- `DltLogViewer-beta index.html (내비 데이터 뷰어 [BETA])` --semantically_similar_to--> `DltLogViewer index.html (내비 데이터 뷰어)`  [INFERRED] [semantically similar]
  DltLogViewer-beta/index.html → DltLogViewer/index.html
- `doRoutePoiSearch` --semantically_similar_to--> `doKeywordSearch`  [INFERRED] [semantically similar]
  DltLogViewer-beta/index.html → DltLogViewer/index.html
- `CJ더마켓 TDS Sample` --semantically_similar_to--> `CJ더마켓 페이지`  [INFERRED] [semantically similar]
  sample.html → cjmarket.html
- `routePoiSample()` --calls--> `buildRoutePoiSearchBody()`  [EXTRACTED]
  server-test/tester.js → DltLogViewer/js/poi-search.js

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Mock GPS 릴레이 재생/설치 흐름** — dltlogviewer_index_mgstartplayback, dltlogviewer_index_mgplaybacktick, dltlogviewer_index_mgsendcoord, dltlogviewer_index_mg_relay, dltlogviewer_index_mgshowinstall, dltlogviewer_help_mock_gps [INFERRED 0.85]
- **POI 키워드 검색·상세 흐름** — dltlogviewer_index_dokeywordsearch, dltlogviewer_index_runsearch, dltlogviewer_index_renderresults, dltlogviewer_index_rundetail, dltlogviewer_index_renderdetail [INFERRED 0.85]
- **경유지 설정 → TVAS 경로요청 흐름** — dltlogviewer_index_setupmapcontextmenu, dltlogviewer_index_setwaypoint, dltlogviewer_index_applywaypointstobody, dltlogviewer_index_synctvasurl, dltlogviewer_index_rerenderroutelayers [INFERRED 0.75]
- **auth-gate.js로 보호되는 도구 페이지들** — auth_gate, index, imagemapper_index, teamroulette_index, server_test_index, cjmarket, sample [EXTRACTED 1.00]
- **내비 데이터 뷰어 회귀 테스트 필수 함정** — claude_rs7_route_summary_layout, claude_rd5_layout, claude_sk_to_wgs84_bessel, claude_initmap_reinit_contextmenu, claude_dom_null_guard [EXTRACTED 1.00]
- **Tmap 요청 편집 흐름 (타입 선택→URL/바디 채움→메뉴 갱신)** — server_test_index_selecttype, server_test_index_fillurl, server_test_index_fillbody, server_test_index_rendermenu [EXTRACTED 1.00]

## Communities (71 total, 8 thin omitted)

### Community 0 - "좌표 변환·DLT 파서"
Cohesion: 0.05
Nodes (48): affineFeatures(), applyAffineTransform(), bearingDeg(), besselToWgs84(), ecefToGeod(), fitAffineTransform(), geodToEcef(), quadraticFeatures() (+40 more)

### Community 1 - "ImageMapper 앱"
Cohesion: 0.10
Nodes (55): baseOptions(), buildSlices(), clearAll(), cutSlice(), downloadAllSlices(), downloadBlob(), downloadSlice(), el (+47 more)

### Community 2 - "DltLogViewer-beta/js/extractor.js"
Cohesion: 0.06
Nodes (33): fitAffineTransform(), solveLinear(), bytesContain(), concatUint8(), DLT_MARKER, encodeMarkers(), findAllMarkers(), isInterestingRecord() (+25 more)

### Community 3 - "SHP 앱 UI (beta)"
Cohesion: 0.04
Nodes (49): bottomAnalysisCount, browseBtn, browserWarn, coordClearBtn, coordLabelX, coordLabelY, coordResult, coordShowBtn (+41 more)

### Community 4 - "SHP 앱 UI"
Cohesion: 0.04
Nodes (49): bottomAnalysisCount, browseBtn, browserWarn, coordClearBtn, coordLabelX, coordLabelY, coordResult, coordShowBtn (+41 more)

### Community 5 - "POI 검색·서버 테스트"
Cohesion: 0.08
Nodes (47): ADDR_KEYS, buildDefaultHeaderText(), buildPoiDetailBody(), buildPoiSearchBody(), buildPoiSearchHeaders(), buildPoiSearchUrl(), buildRoutePoiSearchBody(), coerceJson() (+39 more)

### Community 6 - "DltLogViewer/js/tvas-renderer.js"
Cohesion: 0.05
Nodes (37): DANGER_TYPE_NAMES, GUIDANCE_CODE_NAMES, LANE_ANGLE_ARROWS, LANE_ANGLE_NAMES, ROAD_TYPE_NAMES, ROUTE_OPTION_NAMES, buildDangerPopup(), buildEndpointLabel() (+29 more)

### Community 7 - "지도·SHP 레이어 (beta)"
Cohesion: 0.08
Nodes (39): getMap(), clearShpLayers(), DEF_NODE_STYLE, ensureMapClickListener(), ensureMapMoveListener(), loadShpFiles(), queryBounds(), renderShpGroup() (+31 more)

### Community 8 - "TVAS 바이너리 파서"
Cohesion: 0.10
Nodes (36): evNameBlobStart(), FACILITY_CODE_NAMES, LINK_TYPE_NAMES, nameBlobEnd(), parseCityBoundary(), parseComplexIntersections(), parseCongestion(), parseDangerAreas() (+28 more)

### Community 9 - "좌표검색 앱 UI (js/app.js)"
Cohesion: 0.05
Nodes (36): bottomAnalysisCount, browseBtn, browserWarn, coordClearBtn, coordLabelX, coordLabelY, coordResult, coordShowBtn (+28 more)

### Community 10 - "POI 검색 (beta)"
Cohesion: 0.08
Nodes (29): ADDR_KEYS, buildDefaultHeaderText(), buildPoiDetailBody(), buildPoiSearchBody(), buildPoiSearchHeaders(), buildRoutePoiSearchBody(), COORD_PAIRS, deepFindCoordObject() (+21 more)

### Community 11 - "DltLogViewer-beta/js/tvas-parser.js"
Cohesion: 0.09
Nodes (44): affineFeatures(), applyAffineTransform(), besselToWgs84(), ecefToGeod(), geodToEcef(), quadraticFeatures(), skCoordToWgs84(), skTobesselDeg() (+36 more)

### Community 12 - "TVAS 코드 테이블 (beta)"
Cohesion: 0.06
Nodes (26): DANGER_TYPE_NAMES, GUIDANCE_CODE_NAMES, LANE_ANGLE_ARROWS, LANE_ANGLE_NAMES, ROAD_TYPE_NAMES, ROUTE_OPTION_NAMES, buildEndpointLabel(), CONGESTION_COLORS (+18 more)

### Community 13 - "TeamRoulette 룰렛"
Cohesion: 0.16
Nodes (30): burstConfetti(), closeOverlay(), currentNames(), doSpin(), el, finishSpin(), renderAll(), renderStatus() (+22 more)

### Community 14 - "RTM 교통정보"
Cohesion: 0.13
Nodes (26): ACCIDENT_COLORS, buildTrafficPopupHtml(), buildTrafficUrl(), CATEGORY_KEYS, CATEGORY_LABELS, categoryKeyForIncident(), categoryLabel(), cleanStr() (+18 more)

### Community 15 - "ADB 릴레이 (Python)"
Cohesion: 0.11
Nodes (21): adb_run(), AppSocket, download_apk(), find_adb(), install_apk(), is_app_installed(), is_service_running(), launch_app() (+13 more)

### Community 16 - "ADB 릴레이 (beta)"
Cohesion: 0.11
Nodes (21): adb_run(), AppSocket, download_apk(), find_adb(), install_apk(), is_app_installed(), is_service_running(), launch_app() (+13 more)

### Community 17 - "DltLogViewer/js/map-viewer.js"
Cohesion: 0.14
Nodes (24): formatTimestamp(), addAnchor(), addCoordMarker(), clearCoordMarkers(), clearRouteAnchors(), clearRuler(), divIcon(), esc() (+16 more)

### Community 18 - "주유소·휴게소 팝업"
Cohesion: 0.12
Nodes (23): buildGasStationPopup(), buildRestAreaPopup(), buildWaypointPopup(), formatPoiId(), gasBrandChip(), gasBrandColor(), gasBrandName(), gasFacilities() (+15 more)

### Community 19 - "지도 뷰어 마커 (beta)"
Cohesion: 0.14
Nodes (23): formatTimestamp(), addAnchor(), addCoordMarker(), clearCoordMarkers(), clearRouteAnchors(), clearRuler(), divIcon(), esc() (+15 more)

### Community 20 - "경로요청·등시선"
Cohesion: 0.17
Nodes (17): applyEvBatteryToIsochrone(), buildIsoBodyFromEvBattery(), buildIsochroneBody(), buildRouteUrl(), computeCurrentRange(), EV_BATTERY_FIELDS, EV_ISOCHRONE_SHARED_FIELDS, extractIsochroneRings() (+9 more)

### Community 21 - "SHP 레이어"
Cohesion: 0.22
Nodes (14): getMap(), clearShpLayers(), DEF_NODE_STYLE, ensureMapClickListener(), ensureMapMoveListener(), loadShpFiles(), queryBounds(), renderShpGroup() (+6 more)

### Community 22 - "뷰어 인라인 스크립트"
Cohesion: 0.17
Nodes (15): dlt-map-viewer_v3 리다이렉트 페이지, 지도 우클릭 메뉴, DltLogViewer index.html (내비 데이터 뷰어), applyWaypointsToBody, buildSummaryHtml, drawIsochrone, enableDepartHeadingDrag, lookupAddress (+7 more)

### Community 23 - "정체·경로 세그먼트"
Cohesion: 0.21
Nodes (15): buildCongestionLabels(), buildRangeSegments(), buildRpLinkPopup(), buildSummary(), clearTvasRoute(), congestionLabelHtml(), formatDistance(), formatTime() (+7 more)

### Community 24 - "인증 게이트·도구 페이지"
Cohesion: 0.22
Nodes (13): build(), maybeShowNotice(), sha256hex(), unlock(), CJ더마켓 페이지, 이미지 맵 생성기 (ImageMapper), 1MB 초과 이미지 자동 세로 분할, honor436 개발 도구 모음 (메인 인덱스) (+5 more)

### Community 25 - "경로요청·등시선 (beta)"
Cohesion: 0.19
Nodes (9): applyEvBatteryToIsochrone(), buildIsoBodyFromEvBattery(), buildIsochroneBody(), EV_BATTERY_FIELDS, EV_ISOCHRONE_SHARED_FIELDS, isochroneConsumptionParam(), isochroneHeader(), resolveRouteTypeSwitch() (+1 more)

### Community 26 - "도로·교차로 명칭 라벨"
Cohesion: 0.14
Nodes (14): buildDirectionNameLabels(), buildIntersectionNameLabels(), buildRoadNameLabels(), directionLabelIconHtml(), esc(), incidentIconHtml(), intersectionLabelIconHtml(), renderComplexIntersections() (+6 more)

### Community 27 - "휴게소·경유지 (beta)"
Cohesion: 0.19
Nodes (14): buildRestAreaPopup(), buildWaypointPopup(), formatPoiId(), gasBrandName(), renderRestAreas(), renderWaypoints(), restAreaBadgeIcons(), restAreaFacilities() (+6 more)

### Community 28 - "도움말 개념"
Cohesion: 0.14
Nodes (14): DltLogViewer help.html (도움말), DLT 폴더 로드, 지도 레이어, 복잡교차로(MC4) 이미지, RP-XX 마커, DLT 경로 요청 로그 (RpLog), 줄자 (거리 측정), SHP 링크/노드 (+6 more)

### Community 29 - "도로·교차로 라벨 (beta)"
Cohesion: 0.14
Nodes (14): buildDirectionNameLabels(), buildIntersectionNameLabels(), buildRoadNameLabels(), directionLabelIconHtml(), esc(), incidentIconHtml(), intersectionLabelIconHtml(), renderComplexIntersections() (+6 more)

### Community 30 - "주유소·EV 팝업"
Cohesion: 0.17
Nodes (13): buildEvPopup(), buildGasStationPopup(), evChargerColor(), evChargerLayerKey(), gasBrandChip(), gasBrandColor(), gasFacilities(), gasStationColor() (+5 more)

### Community 31 - "경로 세그먼트 (beta)"
Cohesion: 0.19
Nodes (13): buildRangeSegments(), buildRpLinkPopup(), cityBoundaryIconHtml(), clearTvasRoute(), renderCityBoundary(), renderForcedReroute(), renderHighwayMode(), renderRpLinks() (+5 more)

### Community 32 - "package.json"
Cohesion: 0.17
Nodes (11): engines, node, name, private, scripts, serve, start, test (+3 more)

### Community 33 - "RO4·방향 화살표"
Cohesion: 0.20
Nodes (11): addDirectionArrows(), buildBatteryDepletionPopup(), buildRo4InfoHtml(), buildRo4SegmentPopup(), buildRo4Segments(), buildRouteArrowSpecs(), findBatteryDepletion(), _num() (+3 more)

### Community 34 - "RO4·방향 화살표 (beta)"
Cohesion: 0.20
Nodes (11): addDirectionArrows(), buildBatteryDepletionPopup(), buildRo4InfoHtml(), buildRo4SegmentPopup(), buildRo4Segments(), buildRouteArrowSpecs(), findBatteryDepletion(), _num() (+3 more)

### Community 35 - "CLAUDE.md TDD 지침"
Cohesion: 0.22
Nodes (10): 변경 워크플로우 (실패 테스트→구현→리팩토링→실로그 수동검증→커밋), DOM 요소 null 가드 (stats-section, layer-panel), initMap 재호출 시 contextmenu 유실 → map-init 이벤트 재부착, 외부 I/O 격리 (FakeFile 폴리필·fixture), 내비 데이터 뷰어 (Navi Data Viewer) 프로젝트 지침, node:test 내장 러너 (npm test), RD5 레이아웃 (헤더 40B, 레코드 24B, tollgate blob), RS7 경로요약 레이아웃 (헤더 48B, 32B 세그먼트, 16B 주요도로, 명칭 blob) (+2 more)

### Community 36 - "GPS 분석 흐름"
Cohesion: 0.24
Nodes (10): analyzeGps(), collectFromEntry(), displayResults(), getAnalysisCount(), getFilesFromDataTransfer(), onScanProgress(), renderFileList(), setProgress() (+2 more)

### Community 37 - "GPS 분석 흐름 (beta)"
Cohesion: 0.24
Nodes (10): analyzeGps(), collectFromEntry(), displayResults(), getAnalysisCount(), getFilesFromDataTransfer(), onScanProgress(), renderFileList(), setProgress() (+2 more)

### Community 38 - "안내점 겹침 처리"
Cohesion: 0.28
Nodes (8): buildGuidanceChooserHtml(), groupGuidancePointsByCoord(), guidanceDetailHtml(), guidanceIconHtml(), guidanceName(), renderGuidancePoints(), coords, gps

### Community 39 - "SHP 워커 (beta)"
Cohesion: 0.29
Nodes (4): BESSEL_e2, besselToWgs84(), transformGeom(), WGS84_e2

### Community 40 - "SHP 워커"
Cohesion: 0.29
Nodes (4): BESSEL_e2, besselToWgs84(), transformGeom(), WGS84_e2

### Community 41 - "EV 충전소 (beta)"
Cohesion: 0.25
Nodes (8): buildEvPopup(), evChargerColor(), evChargerLayerKey(), gasStationColor(), gasStationTypeIcon(), renderEvChargers(), renderGasStations(), resolveEvCoord()

### Community 42 - "MockGPS 재생 보간"
Cohesion: 0.62
Nodes (5): dltSpeedKmh(), haversineM(), indexFromProgress(), interpolatePath(), reinterpolateFromCurrent()

### Community 43 - "Beta 경로 POI 검색"
Cohesion: 0.53
Nodes (6): DltLogViewer-beta help.html (도움말), DltLogViewer-beta index.html (내비 데이터 뷰어 [BETA]), buildRoutePoiUrl, currentCenterWgs, doRoutePoiSearch, readRouteOptions

### Community 44 - "MockGPS 재생 (beta)"
Cohesion: 0.53
Nodes (4): dltSpeedKmh(), haversineM(), interpolatePath(), reinterpolateFromCurrent()

### Community 45 - "정체 라벨 (beta)"
Cohesion: 0.53
Nodes (6): buildCongestionLabels(), buildSummary(), congestionLabelHtml(), formatDistance(), formatTime(), renderCongestion()

### Community 46 - "위험구간 렌더링"
Cohesion: 0.33
Nodes (6): buildDangerPopup(), dangerName(), formatTimeSlots(), getDangerIcon(), getDangerIconSvg(), renderDangerAreas()

### Community 47 - "안내점 (beta)"
Cohesion: 0.40
Nodes (6): buildGuidanceChooserHtml(), groupGuidancePointsByCoord(), guidanceDetailHtml(), guidanceIconHtml(), guidanceName(), renderGuidancePoints()

### Community 48 - "설치 스크립트 sh (beta)"
Cohesion: 0.60
Nodes (5): install.sh script, fail(), ok(), step(), warn()

### Community 49 - "좌표검색·CORS"
Cohesion: 0.33
Nodes (6): 좌표 검색, CORS 차단 / CORS Unblock 확장, doKeywordSearch, renderResults, runSearch, showCorsErrorModal

### Community 50 - "Mock GPS 재생 UI"
Cohesion: 0.33
Nodes (6): Mock GPS, Mock GPS 3가지 데이터 소스 모드, mgGetTvasCoords, mgPlaybackTick, mgSendCoord, mgStartPlayback

### Community 51 - "Mock GPS 설치"
Cohesion: 0.40
Nodes (6): Mock GPS 자동 설치, MG_RELAY (localhost:21234), mgShowInstall, mgWizInstallApp, refreshAdbStatus, refreshAppStatus

### Community 52 - "설치 스크립트 sh"
Cohesion: 0.60
Nodes (5): install.sh script, fail(), ok(), step(), warn()

### Community 53 - "GPS 분석 (app.js)"
Cohesion: 0.47
Nodes (6): analyzeGps(), displayResults(), getAnalysisCount(), renderFileList(), showError(), updateCountDisplay()

### Community 54 - "경유지 마커"
Cohesion: 0.60
Nodes (5): addViaMarker(), removeWp(), setWaypoint(), updateWpBar(), wpIconHtml()

### Community 55 - "차선 안내"
Cohesion: 0.50
Nodes (5): angleName(), angleToArrow(), buildLanePopup(), laneIconHtml(), renderLaneGuidance()

### Community 57 - "경유지 마커 (beta)"
Cohesion: 0.60
Nodes (5): addViaMarker(), removeWp(), setWaypoint(), updateWpBar(), wpIconHtml()

### Community 58 - "차선 안내 (beta)"
Cohesion: 0.50
Nodes (5): angleName(), angleToArrow(), buildLanePopup(), laneIconHtml(), renderLaneGuidance()

### Community 62 - "파일 스캔 (app.js)"
Cohesion: 0.50
Nodes (4): collectFromEntry(), getFilesFromDataTransfer(), onScanProgress(), setProgress()

### Community 65 - "샘플 캐러셀"
Cohesion: 0.67
Nodes (3): goToSlide, nextSlide, startAutoPlay

## Knowledge Gaps
- **292 isolated node(s):** `DLT_MARKER`, `_utf8`, `_markerPositions`, `GPS_INTERESTING_STRINGS`, `ROUTE_TTS_INTERESTING_STRINGS` (+287 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **8 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `DltLogViewer index.html (내비 데이터 뷰어)` connect `뷰어 인라인 스크립트` to `CLAUDE.md TDD 지침`, `Beta 경로 POI 검색`, `좌표검색·CORS`, `Mock GPS 재생 UI`, `Mock GPS 설치`, `인증 게이트·도구 페이지`, `도움말 개념`?**
  _High betweenness centrality (0.006) - this node is a cross-community bridge._
- **Why does `서버 테스트 — Tmap 요청 테스터` connect `인증 게이트·도구 페이지` to `POI 검색·서버 테스트`?**
  _High betweenness centrality (0.004) - this node is a cross-community bridge._
- **Why does `honor436 개발 도구 모음 (메인 인덱스)` connect `인증 게이트·도구 페이지` to `뷰어 인라인 스크립트`?**
  _High betweenness centrality (0.004) - this node is a cross-community bridge._
- **What connects `DLT_MARKER`, `_utf8`, `_markerPositions` to the rest of the system?**
  _292 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `좌표 변환·DLT 파서` be split into smaller, more focused modules?**
  _Cohesion score 0.05406746031746032 - nodes in this community are weakly interconnected._
- **Should `ImageMapper 앱` be split into smaller, more focused modules?**
  _Cohesion score 0.10344827586206896 - nodes in this community are weakly interconnected._
- **Should `DltLogViewer-beta/js/extractor.js` be split into smaller, more focused modules?**
  _Cohesion score 0.060129509713228495 - nodes in this community are weakly interconnected._