import test from 'node:test';
import assert from 'node:assert/strict';
import { parseRouteSummary, parseLaneGuidance, parseRpLinks } from '../DltLogViewer/js/tvas-parser.js';
import { laneIconHtml } from '../DltLogViewer/js/tvas-renderer.js';

// ---- RS7 parseRouteSummary ----------------------------------------------- //
//
// Fixture spec (TVAS v5.9 p.45-46):
//   Header (48B):
//     +0  UShort  count (n)
//     +2  Byte    infoType
//     +3  Byte    dataType
//     +4  Char[4] infoId
//     +8  Char[12] trafficTime
//     +20 UShort  tollFare10
//     +22 Byte    reserved
//     +23 Byte    predictType
//     +24 Char[12] predictTime
//     +36 Int     nameBlobSize
//     +40 Int     roadNameBlobSize
//     +44 Byte    roadAttr
//     +45 UShort  roadNameCount
//     +47 Byte    ecoSaving
//
//   경로요약 DATA (32B × n):
//     +0  Byte    구분 (1=Link, 2=Node)
//     +1  Byte    통제구분코드
//     +2  2B      reserved
//     +4  Int     명칭 Offset
//     +8  Int     구간거리(m)
//     +12 Int     구간시간(초)
//     +16 Byte    속도
//     +17 Char    혼잡도
//     +18 UShort  startVxIdx
//     +20 UShort  endVxIdx
//     +22 Byte    세도로 포함
//     +23 Byte    회전코드
//     +24 Int     에너지(W)
//     +28 Byte    수동충전소
//     +29 3B      reserved
//
//   Then: 주요도로 DATA(16B × m) → 경로요약 명칭 blob → 주요도로 명칭 blob

function buildRs7Fixture() {
  // 2 sections, 0 major roads, two names: "서울" (UTF-8 6B) and "부산" (UTF-8 6B)
  const nameBlob = new TextEncoder().encode('서울\0부산\0'); // 14 bytes incl NULs
  const section1NameOffset = 0;
  const section2NameOffset = 7; // '서울\0'.length in utf-8 = 3+3+1 = 7

  const headerSize = 48;
  const sectionSize = 32;
  const sectionCount = 2;
  const roadNameCount = 0;
  const nameBlobSize = nameBlob.length;
  const roadNameBlobSize = 0;

  const total = headerSize + sectionSize * sectionCount + nameBlobSize;
  const buf = new ArrayBuffer(total);
  const dv = new DataView(buf);
  const u8 = new Uint8Array(buf);

  // Header
  dv.setUint16(0, sectionCount, true);
  dv.setUint8(2, 0x02);             // infoType
  dv.setUint8(3, 0x01);             // dataType
  u8.set(new TextEncoder().encode('RS7\0'), 4);
  u8.set(new TextEncoder().encode('202604141030'), 8);
  dv.setUint16(20, 50, true);       // 500원 = 50 × 10
  // +22 reserved
  dv.setUint8(23, 0);                // predictType
  u8.set(new TextEncoder().encode('202604141100'), 24);
  dv.setInt32(36, nameBlobSize, true);
  dv.setInt32(40, roadNameBlobSize, true);
  dv.setUint8(44, 0x03);             // roadAttr
  dv.setUint16(45, roadNameCount, true);
  dv.setUint8(47, 15);                // ecoSaving 15%

  // Section 1 (Link, Seoul)
  let base = headerSize;
  dv.setUint8(base + 0, 1);                          // linkNodeType=1 (Link)
  dv.setUint8(base + 1, 0);                          // controlCode
  dv.setInt32(base + 4, section1NameOffset, true);  // nameOffset
  dv.setInt32(base + 8, 12000, true);                // distance 12km
  dv.setInt32(base + 12, 900, true);                 // time 900s
  dv.setUint8(base + 16, 60);                        // speed
  dv.setUint8(base + 17, 'A'.charCodeAt(0));         // congestion
  dv.setUint16(base + 18, 0, true);                  // startVxIdx
  dv.setUint16(base + 20, 100, true);                // endVxIdx
  dv.setUint8(base + 22, 0);                         // narrowRoad
  dv.setUint8(base + 23, 0);                         // turnCode
  dv.setInt32(base + 24, 25000, true);               // energy 25000W
  dv.setUint8(base + 28, 0);                         // manualStation

  // Section 2 (Link, Busan)
  base = headerSize + sectionSize;
  dv.setUint8(base + 0, 1);
  dv.setUint8(base + 1, 0);
  dv.setInt32(base + 4, section2NameOffset, true);
  dv.setInt32(base + 8, 34000, true);
  dv.setInt32(base + 12, 1800, true);
  dv.setUint8(base + 16, 80);
  dv.setUint8(base + 17, 'B'.charCodeAt(0));
  dv.setUint16(base + 18, 100, true);
  dv.setUint16(base + 20, 250, true);
  dv.setUint8(base + 22, 1);
  dv.setUint8(base + 23, 5);
  dv.setInt32(base + 24, 55000, true);
  dv.setUint8(base + 28, 1);

  // Name blob
  u8.set(nameBlob, headerSize + sectionSize * sectionCount);

  return { dv, size: total };
}

test('parseRouteSummary header parses count and blob sizes', () => {
  const { dv, size } = buildRs7Fixture();
  const result = parseRouteSummary(dv, 0, size, 1 /* utf-8 */);
  assert.equal(result.count, 2);
  assert.equal(result.tollFare, 500);
  assert.equal(result.ecoSaving, 15);
  assert.equal(result.roadAttr, 0x03);
  assert.equal(result.roadNameCount, 0);
});

test('parseRouteSummary section 1 fields at correct offsets', () => {
  const { dv, size } = buildRs7Fixture();
  const { items } = parseRouteSummary(dv, 0, size, 1);
  assert.equal(items[0].nameOffset, 0, 'nameOffset at +4');
  assert.equal(items[0].distance, 12000, 'distance at +8');
  assert.equal(items[0].time, 900, 'time at +12');
  assert.equal(items[0].speed, 60, 'speed at +16');
  assert.equal(items[0].congestion, 'A', 'congestion at +17');
  assert.equal(items[0].startVxIdx, 0, 'startVxIdx at +18');
  assert.equal(items[0].endVxIdx, 100, 'endVxIdx at +20');
  assert.equal(items[0].energy, 25000, 'energy at +24');
  assert.equal(items[0].manualStation, 0, 'manualStation at +28');
});

test('parseRouteSummary section 2 fields at correct offsets', () => {
  const { dv, size } = buildRs7Fixture();
  const { items } = parseRouteSummary(dv, 0, size, 1);
  assert.equal(items[1].nameOffset, 7);
  assert.equal(items[1].distance, 34000);
  assert.equal(items[1].time, 1800);
  assert.equal(items[1].speed, 80);
  assert.equal(items[1].congestion, 'B');
  assert.equal(items[1].startVxIdx, 100);
  assert.equal(items[1].endVxIdx, 250);
  assert.equal(items[1].energy, 55000);
  assert.equal(items[1].manualStation, 1);
});

test('parseRouteSummary reads section names from blob', () => {
  const { dv, size } = buildRs7Fixture();
  const { items } = parseRouteSummary(dv, 0, size, 1);
  assert.equal(items[0].name, '서울');
  assert.equal(items[1].name, '부산');
});

// ---- TL5 parseLaneGuidance: invalid lane pair order -------------------- //
//
// 실 TVAS 바이너리 검증 결과:
//   각 쌍(4B) = lane bitmap(UShort LE) + angle(UShort LE)
//     +0 (2B)  차로 비트맵  — bit n set ⇒ (n+1)차로가 invalid
//     +2 (2B)  각도         — 0/45/90/135/180/225/270/315
//   (스펙 문서 라벨은 반대로 쓰여 있으나, 실 저장 파일이 기준)

function buildTl5InvalidLaneFixture() {
  // TL5 layout in this parser:
  //   Header 20B (count=1, invalidBlobSize=8, busBlobSize=0)
  //   Data   32B × 1 (vxIdx=10, invalidCount=2, invalidOffset=0)
  //   Blob   4B × 2  (2쌍: lane+angle)
  const headerSize = 20;
  const dataSize = 32;
  const blobSize = 4 * 2; // 2 pairs
  const total = headerSize + dataSize + blobSize;
  const buf = new ArrayBuffer(total);
  const dv = new DataView(buf);

  // Header
  dv.setUint16(0, 1, true);       // count = 1
  dv.setInt32(8, blobSize, true); // invalidBlobSize
  dv.setInt32(12, 0, true);       // busBlobSize

  // Data #0
  const dBase = headerSize;
  dv.setUint16(dBase + 0, 10, true); // vxIdx
  dv.setUint8(dBase + 5, 2);          // invalidCount = 2
  dv.setInt32(dBase + 20, 0, true);   // invalidOffset = 0 (blob 시작)

  // Blob: 쌍 1 (+0 lane=0x0001 (1차로), +2 angle=90)
  const blobBase = headerSize + dataSize;
  dv.setUint16(blobBase + 0, 0x0001, true);
  dv.setUint16(blobBase + 2, 90, true);
  // 쌍 2 (+4 lane=0x0004 (3차로), +6 angle=0 (직진))
  dv.setUint16(blobBase + 4, 0x0004, true);
  dv.setUint16(blobBase + 6, 0, true);

  return { dv, size: total };
}

test('parseLaneGuidance invalid-lane pair #1: lane bitmap at +0, angle at +2', () => {
  const { dv, size } = buildTl5InvalidLaneFixture();
  const lanes = parseLaneGuidance(dv, 0, size);
  assert.equal(lanes[0].invalidLanes[0].lane, 0x0001, 'lane bitmap at +0');
  assert.equal(lanes[0].invalidLanes[0].angle, 90, 'angle at +2');
});

test('parseLaneGuidance invalid-lane pair #2: lane bitmap at +0, angle at +2', () => {
  const { dv, size } = buildTl5InvalidLaneFixture();
  const lanes = parseLaneGuidance(dv, 0, size);
  assert.equal(lanes[0].invalidLanes[1].lane, 0x0004, 'lane bitmap at +0');
  assert.equal(lanes[0].invalidLanes[1].angle, 0, 'angle at +2');
});

// ---- laneIconHtml: invalid-lane arrows on map icon --------------------- //

test('laneIconHtml renders invalid-lane arrows on map icon', () => {
  // 3차로, 권장/유효 없음. invalidLanes:
  //   1차로(bit0) 좌회전(270 → ←)
  //   3차로(bit2) 우회전(90  → →)
  const tl = {
    totalLanes: 3,
    leftPocket: 0, rightPocket: 0,
    recommendLane: 0, recommendAngle: 0,
    validLane: 0, validAngle: 0,
    overpassLane: 0, underpassLane: 0,
    busLaneCode: 0,
    invalidLanes: [
      { lane: 0b001, angle: 270 },
      { lane: 0b100, angle: 90 },
    ],
  };
  const html = laneIconHtml(tl);
  // Expect both invalid arrows present in the rendered HTML.
  assert.ok(html.includes('←'), `left-turn arrow missing: ${html}`);
  assert.ok(html.includes('→'), `right-turn arrow missing: ${html}`);
});

test('laneIconHtml does NOT render invalid arrow on lanes outside the bitmap', () => {
  // 3차로, invalidLanes는 1차로(bit0)에만 직진(↑) invalid
  const tl = {
    totalLanes: 3,
    leftPocket: 0, rightPocket: 0,
    recommendLane: 0, recommendAngle: 0,
    validLane: 0, validAngle: 0,
    overpassLane: 0, underpassLane: 0,
    busLaneCode: 0,
    invalidLanes: [{ lane: 0b001, angle: 0 }],
  };
  const html = laneIconHtml(tl);
  // ↑ should appear exactly once (only on lane 1)
  const upCount = (html.match(/↑/g) || []).length;
  assert.equal(upCount, 1, `expected exactly 1 ↑, got ${upCount} in ${html}`);
});

// ---- RD5 parseRpLinks ----------------------------------------------------- //
//
// Fixture spec (TVAS v5.9 — RD5 RPLINK 정보):
//   Header (40B):
//     +0  UShort  count (n)
//     +2  Byte    infoType (0x02)
//     +3  Byte    reserved
//     +4  Char[4] infoId
//     +8  Int     initDistance (초기탐색 직선거리)
//     +12 Char[24] sessionId (초기탐색 SessionID)
//     +36 Int     tollBlobSize (톨게이트ID 데이터 전체 크기)
//
//   RpLink DATA (24B × n):
//     +0  UShort  startVxIdx (시작 보간점 Idx)
//     +2  UShort  endVxIdx   (마지막 보간점 Idx)
//     +4  Int     rid
//     +8  Int     ridTime    (RID 소요시간, sec)
//     +12 UShort  meshCode
//     +14 Int     linkId
//     +18 Byte    direction      (0:정방향, 1:역방향)
//     +19 Byte    compareTarget  (경로비교대상)
//     +20 Byte    superCruise
//     +21 Byte[3] reserved

function buildRd5Fixture() {
  const HEADER = 40, REC = 24, count = 2;
  const total = HEADER + REC * count;
  const buf = new ArrayBuffer(total);
  const dv = new DataView(buf);

  // Header
  dv.setUint16(0, count, true);
  dv.setUint8(2, 0x02);                              // infoType
  dv.setUint8(3, 0);                                 // reserved
  'AB12'.split('').forEach((c, i) => dv.setUint8(4 + i, c.charCodeAt(0)));
  dv.setInt32(8, 123456, true);                      // initDistance
  'SESSION-XYZ'.split('').forEach((c, i) => dv.setUint8(12 + i, c.charCodeAt(0)));
  dv.setInt32(36, 0, true);                          // tollBlobSize

  // Record 0
  let b = HEADER;
  dv.setUint16(b + 0, 0, true);     // startVxIdx
  dv.setUint16(b + 2, 5, true);     // endVxIdx
  dv.setInt32(b + 4, 1001, true);   // rid
  dv.setInt32(b + 8, 60, true);     // ridTime
  dv.setUint16(b + 12, 45678, true);// meshCode
  dv.setInt32(b + 14, 888777, true);// linkId
  dv.setUint8(b + 18, 0);           // direction 정방향
  dv.setUint8(b + 19, 1);           // compareTarget
  dv.setUint8(b + 20, 0);           // superCruise

  // Record 1
  b = HEADER + REC;
  dv.setUint16(b + 0, 5, true);
  dv.setUint16(b + 2, 12, true);
  dv.setInt32(b + 4, 2002, true);
  dv.setInt32(b + 8, 180, true);
  dv.setUint16(b + 12, 45679, true);
  dv.setInt32(b + 14, 999000, true);
  dv.setUint8(b + 18, 1);           // direction 역방향
  dv.setUint8(b + 19, 0);
  dv.setUint8(b + 20, 1);           // superCruise

  return { dv, size: total };
}

test('parseRpLinks header is 40 bytes and reads count + infoType + infoId', () => {
  const { dv, size } = buildRd5Fixture();
  const { header } = parseRpLinks(dv, 0, size, 'utf-8');
  assert.equal(header.count, 2);
  assert.equal(header.infoType, 0x02);
  assert.equal(header.infoId, 'AB12');
  assert.equal(header.initDistance, 123456);
  assert.equal(header.tollBlobSize, 0);
});

test('parseRpLinks reads 24-byte records with correct field offsets', () => {
  const { dv, size } = buildRd5Fixture();
  const { items } = parseRpLinks(dv, 0, size, 'utf-8');
  assert.equal(items.length, 2);

  const [r0, r1] = items;
  assert.deepEqual(
    { s: r0.startVxIdx, e: r0.endVxIdx, rid: r0.rid, t: r0.ridTime,
      mesh: r0.meshCode, link: r0.linkId, dir: r0.direction, sc: r0.superCruise },
    { s: 0, e: 5, rid: 1001, t: 60, mesh: 45678, link: 888777, dir: 0, sc: 0 },
  );
  assert.deepEqual(
    { s: r1.startVxIdx, e: r1.endVxIdx, rid: r1.rid, t: r1.ridTime,
      mesh: r1.meshCode, link: r1.linkId, dir: r1.direction, sc: r1.superCruise },
    { s: 5, e: 12, rid: 2002, t: 180, mesh: 45679, link: 999000, dir: 1, sc: 1 },
  );
});

test('parseRpLinks stops cleanly when records exceed section size', () => {
  const { dv } = buildRd5Fixture();
  // Claim a size that only covers the header + 1 record (40 + 24 = 64).
  const { items } = parseRpLinks(dv, 0, 64, 'utf-8');
  assert.equal(items.length, 1);
});

// ---- parseNormalRouteResponse (일반 길안내 바이너리 응답 래퍼) ------------- //
//
// 일반(planningroutemultiformat) 응답은 EV 와 래퍼가 다르다. 실데이터로 확정한 레이아웃:
//   응답공통헤더(240) → roadCount(int32) → roadType(byte) → routeCount(byte)
//   → routePlanTypes(int32×n) → fareWeightOpt(int32×n) → routeSummaryCode(int32×n)
//   → destination(가변) → tvasSize(int32×n) → tvases(byte[])
// EV(routeCount@240, 직후 tvasSize)와 달리 roadCount/roadType/…/destination 이 앞에 있다.
import { parseNormalRouteResponse } from '../DltLogViewer/js/tvas-parser.js';

function buildNormalRespFixture() {
  const buf = new ArrayBuffer(330);
  const dv = new DataView(buf);
  const b = new Uint8Array(buf);
  const put = (off, s) => { for (let i = 0; i < s.length; i++) b[off + i] = s.charCodeAt(i); };
  put(0, '000000');       // errorCode
  put(6, 'success');      // errorMessage
  put(216, 'SESSION-XYZ');// sessionId
  dv.setInt32(240, 1, false);  // roadCount (deprecated)
  b[244] = 0x20;               // roadType (deprecated)
  b[245] = 1;                  // routeCount
  dv.setInt32(246, 7, false);  // routePlanTypes[0]
  dv.setInt32(250, 2, false);  // fareWeightOpt[0]
  dv.setInt32(254, 0, false);  // routeSummaryCode[0]
  b[258] = 0;                  // compressFlag
  put(259, '9999999');         // destPoiId String(10)
  b[269] = 16;                 // destRpFlag
  b[270] = 3;                  // departCoordType (0x03 fix)
  dv.setInt32(271, 4573150, false); // destXPos
  dv.setInt32(275, 1351635, false); // destYPos
  b[279] = 2;                  // destNameSize
  b[280] = 1;                  // tvasCount
  put(281, 'AB');              // destName String(2)
  put(283, 'SKY_O00');         // skyCode String(7)
  put(290, 'sunny');           // skyName String(20)
  put(310, '0');               // rainTypeCode String(1)
  put(311, '0.00');            // rainSinceOnTime String(6)
  put(317, '20.0');            // tempC1h String(5)
  dv.setInt32(322, 4, false);  // tvasSize[0]
  b[326] = 0xDE; b[327] = 0xAD; b[328] = 0xBE; b[329] = 0xEF; // tvases
  return buf;
}

test('parseNormalRouteResponse_reads_routeCount_after_roadCount_roadType', () => {
  const w = parseNormalRouteResponse(buildNormalRespFixture());
  assert.equal(w.roadCount, 1);
  assert.equal(w.roadType, 0x20);
  assert.equal(w.routeCount, 1); // offset 245, not 240
});

test('parseNormalRouteResponse_reads_header_strings', () => {
  const w = parseNormalRouteResponse(buildNormalRespFixture());
  assert.equal(w.errorCode, '000000');
  assert.equal(w.errorMessage, 'success');
  assert.equal(w.sessionId, 'SESSION-XYZ');
});

test('parseNormalRouteResponse_reads_int_arrays', () => {
  const w = parseNormalRouteResponse(buildNormalRespFixture());
  assert.deepEqual(w.routePlanTypes, [7]);
  assert.deepEqual(w.fareWeightOpt, [2]);
  assert.deepEqual(w.routeSummaryCode, [0]);
});

test('parseNormalRouteResponse_parses_variable_destination', () => {
  const w = parseNormalRouteResponse(buildNormalRespFixture());
  assert.equal(w.destination.departCoordType, 3);
  assert.equal(w.destination.destRpFlag, 16);
  assert.equal(w.destination.destXPos, 4573150);
  assert.equal(w.destination.destYPos, 1351635);
  assert.equal(w.destination.destNameSize, 2);
  assert.equal(w.destination.destName, 'AB');
  assert.equal(w.destination.skyCode, 'SKY_O00');
});

test('parseNormalRouteResponse_locates_tvasSize_and_tvasStart_after_destination', () => {
  const w = parseNormalRouteResponse(buildNormalRespFixture());
  assert.deepEqual(w.tvasSize, [4]);
  assert.equal(w.tvasStart, 326); // destination 가변 길이를 정확히 넘긴 위치
});

// ---- parseGasStations (GAS3 주유소정보) ----------------------------------- //
//
// 일반 경로는 ES3(충전소) 대신 GAS3(주유소)를 표시한다. 스펙(TVAS 규격서):
//   헤더 12B: UShort count(n), Byte type(0x01), Byte reserved, Char[4] "GAS3",
//             Int nameSize(명칭 blob 전체 크기)
//   레코드 40B×n: UShort vxIdx, Byte stationType, Byte brandCode, Byte facilityBits,
//     Byte roadType, Byte premiumGasSale, Byte isLowestPrice, Int locX, Int locY,
//     Int nameOffset, UShort gasoline, UShort diesel, UShort kerosene, UShort lpg,
//     UShort premiumGasoline, Byte truckDiscount, Byte owinMember, Byte muffinMember,
//     Byte reserved, Int poiId, Byte cardDiscount, Byte isSelf
//   명칭 blob: 주유소명칭(NULL 포함), nameOffset 기준.
import { parseGasStations } from '../DltLogViewer/js/tvas-parser.js';

function buildGas3Fixture() {
  // header(12) + record(40)×2 + nameBlob("AAA\0BB\0"=7) = 99
  const buf = new ArrayBuffer(99);
  const dv = new DataView(buf);
  const b = new Uint8Array(buf);
  // header
  dv.setUint16(0, 2, true);   // count
  b[2] = 0x01;                // type
  b[3] = 0;                   // reserved
  b[4] = 0x47; b[5] = 0x41; b[6] = 0x53; b[7] = 0x33; // "GAS3"
  dv.setInt32(8, 7, true);    // nameSize
  const rec = (base, o) => {
    dv.setUint16(base + 0, o.vxIdx, true);
    b[base + 2] = o.stationType; b[base + 3] = o.brandCode; b[base + 4] = o.facilityBits;
    b[base + 5] = o.roadType; b[base + 6] = o.premiumGasSale; b[base + 7] = o.isLowestPrice;
    dv.setInt32(base + 8, o.locX, true);
    dv.setInt32(base + 12, o.locY, true);
    dv.setInt32(base + 16, o.nameOffset, true);
    dv.setUint16(base + 20, o.gasoline, true);
    dv.setUint16(base + 22, o.diesel, true);
    dv.setUint16(base + 24, o.kerosene, true);
    dv.setUint16(base + 26, o.lpg, true);
    dv.setUint16(base + 28, o.premiumGasoline, true);
    b[base + 30] = o.truckDiscount; b[base + 31] = o.owinMember; b[base + 32] = o.muffinMember;
    b[base + 33] = 0; // reserved
    dv.setInt32(base + 34, o.poiId, true);
    b[base + 38] = o.cardDiscount; b[base + 39] = o.isSelf;
  };
  rec(12, { vxIdx: 39, stationType: 0, brandCode: 1, facilityBits: 9, roadType: 5,
    premiumGasSale: 1, isLowestPrice: 1, locX: 45732470, locY: 13502790, nameOffset: 0,
    gasoline: 1999, diesel: 1995, kerosene: 0, lpg: 0, premiumGasoline: 2548,
    truckDiscount: 0, owinMember: 0, muffinMember: 0, poiId: 203924, cardDiscount: 0, isSelf: 1 });
  rec(52, { vxIdx: 100, stationType: 0, brandCode: 2, facilityBits: 0, roadType: 3,
    premiumGasSale: 2, isLowestPrice: 0, locX: 45700000, locY: 13500000, nameOffset: 4,
    gasoline: 1650, diesel: 1600, kerosene: 0, lpg: 990, premiumGasoline: 0,
    truckDiscount: 1, owinMember: 0, muffinMember: 0, poiId: 999, cardDiscount: 1, isSelf: 0 });
  // name blob @ 12 + 80 = 92 : "AAA\0BB\0"
  const nb = 92;
  b[nb + 0] = 65; b[nb + 1] = 65; b[nb + 2] = 65; b[nb + 3] = 0;  // "AAA\0"
  b[nb + 4] = 66; b[nb + 5] = 66; b[nb + 6] = 0;                  // "BB\0"
  return { dv, size: 99 };
}

test('parseGasStations_reads_all_records_with_40B_stride', () => {
  const { dv, size } = buildGas3Fixture();
  const gs = parseGasStations(dv, 0, size, 1);
  assert.equal(gs.length, 2);
  assert.equal(gs[0].vxIdx, 39);
  assert.equal(gs[1].vxIdx, 100);
});

test('parseGasStations_parses_location_and_prices', () => {
  const { dv, size } = buildGas3Fixture();
  const g = parseGasStations(dv, 0, size, 1)[0];
  assert.equal(g.locX, 45732470);
  assert.equal(g.locY, 13502790);
  assert.equal(g.gasoline, 1999);
  assert.equal(g.diesel, 1995);
  assert.equal(g.premiumGasoline, 2548);
  assert.equal(g.poiId, 203924);
});

test('parseGasStations_parses_flags', () => {
  const { dv, size } = buildGas3Fixture();
  const gs = parseGasStations(dv, 0, size, 1);
  assert.equal(gs[0].isSelf, 1);
  assert.equal(gs[0].isLowestPrice, 1);
  assert.equal(gs[0].brandCode, 1);
  assert.equal(gs[1].isSelf, 0);
  assert.equal(gs[1].truckDiscount, 1);
  assert.equal(gs[1].cardDiscount, 1);
});

test('parseGasStations_reads_names_by_offset', () => {
  const { dv, size } = buildGas3Fixture();
  const gs = parseGasStations(dv, 0, size, 1);
  assert.equal(gs[0].name, 'AAA'); // nameOffset 0
  assert.equal(gs[1].name, 'BB');  // nameOffset 4
});
