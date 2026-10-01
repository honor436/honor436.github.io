// 로그 태그 [pid:tid] 형식 지원 테스트
// IDCEVO25 로그(2026-09)부터 태그가 [sgId] → [pid:tid] 로 바뀌었다.
//   예) TMAP(330250)^#RpLog[4939:15740]:[RP-SIMPLE-1686-Traffic_Recommend] REQ: {...}
//       [MM][4939:6852]:[MM_RESULT] GPS    Pos = 38.266270 128.511993, 177
import test from 'node:test';
import assert from 'node:assert/strict';
import { extractLogs } from '../DltLogViewer/js/extractor.js';

// 최소 DLT 레코드: "DLT\x01" + storage header 나머지(12B) + 페이로드 텍스트
function fakeDltFile(lines, name = 'fake.dlt') {
  const enc = new TextEncoder();
  const parts = lines.map(text => {
    const head = new Uint8Array(16);
    head.set([0x44, 0x4C, 0x54, 0x01]);
    return new Uint8Array([...head, ...enc.encode(text + '\0')]);
  });
  const blob = new Blob(parts);
  blob.name = name;
  return blob;
}

const REQ_JSON = '{"departName":"출발","departXPos":4626513,"departYPos":1377487,'
  + '"destName":"BMW R&D센터","destXPos":4558656,"destYPos":1350436,"tvas":"5.9","wayPoints":[]}';

test('extractLogs RpLog with [pid:tid] tag yields route request with depart/dest coords', async () => {
  const file = fakeDltFile([
    'TMAP(330250)^#RpLog[4939:15740]:[RP-SIMPLE-1686-Traffic_Recommend] --> POST https://tmap-channel-aws.tmobiapi.com/tmap-channel/rsd/route/planningroutemultiformat',
    `TMAP(330250)^#RpLog[4939:15740]:[RP-SIMPLE-1686-Traffic_Recommend] REQ: ${REQ_JSON}`,
    'TMAP(330250)^#RpLog[4939:15740]:[RP-SIMPLE-1686-Traffic_Recommend] <-- 200 (681ms) SessionID: TP5d20260917100623065tCH',
    'TMAP(330250)^#RpLog[4939:15740]:[RP-SIMPLE-1686-Traffic_Recommend] RES: [000000 success] (size: 213.8KB)',
  ]);
  const { routeRequests } = await extractLogs([file]);
  assert.equal(routeRequests.length, 1);
  const rr = routeRequests[0];
  assert.equal(rr.rpId, 1686);
  assert.equal(rr.destName, 'BMW R&D센터');
  assert.equal(rr.responseCode, 200);
  assert.equal(rr.sessionId, 'TP5d20260917100623065tCH');
  assert.ok(Math.abs(rr.departLat - 38.2662) < 0.001);
  assert.ok(Math.abs(rr.destLon - 126.6273) < 0.001);
});

test('extractLogs MM_RESULT with [pid:tid] tag yields gps/match pair', async () => {
  const file = fakeDltFile([
    '[MM][4939:6852]:[MM_RESULT] ############# Result(LocalMatch) #################',
    '[MM][4939:6852]:[MM_RESULT] GPS = GPS, Hdop = 20.000000 Speed:10',
    '[MM][4939:6852]:[MM_RESULT] State = MATCH_OK_GOOD',
    '[MM][4939:6852]:[MM_RESULT] GPS    Pos = 38.266270 128.511993, 177',
    '[MM][4939:6852]:[MM_RESULT] Match  Pos = 38.266516 128.511621, 177',
  ]);
  const { mmLogs } = await extractLogs([file]);
  assert.equal(mmLogs.length, 2);
  assert.deepEqual(mmLogs.map(m => m.sourceType), ['mm_gps', 'mm_match']);
  assert.equal(mmLogs[1].lat, 38.266516);
  assert.equal(mmLogs[1].lon, 128.511621);
  assert.equal(mmLogs[0].details.state, 'MATCH_OK_GOOD');
  assert.equal(mmLogs[0].details.hdop, 20);
});

test('extractLogs requestTTS with [pid:tid] tag yields tts entry with preceding status', async () => {
  const file = fakeDltFile([
    'TMAP(330250)^[4939:6852]:requestTTS status : 0',
    'TMAP(330250)^[4939:6852]:requestTTS[1085] script : 300미터 앞에서 우회전입니다',
  ]);
  const { ttsLogs } = await extractLogs([file]);
  assert.equal(ttsLogs.length, 1);
  assert.equal(ttsLogs[0].requestId, '1085');
  assert.equal(ttsLogs[0].script, '300미터 앞에서 우회전입니다');
  assert.equal(ttsLogs[0].status, '0');
});

test('extractLogs legacy [sgId] RpLog and TmapAutoExternalVoicePlayer TTS still parsed', async () => {
  const file = fakeDltFile([
    'TMAP^#RpLog[218]:[RP-218-Traffic_MinTime] --> POST https://example/rsd/route',
    `TMAP^#RpLog[218]:[RP-218-Traffic_MinTime] REQ: ${REQ_JSON}`,
    'TMAP^#RpLog[218]:[RP-218-Traffic_MinTime] RES: [000000 success] (size: 1KB)',
    'TmapAutoExternalVoicePlayer:requestTTS[7]:requestTTS status : PLAYING',
    'TmapAutoExternalVoicePlayer:requestTTS[7]:requestTTS script : 안내를 시작합니다',
  ]);
  const { routeRequests, ttsLogs } = await extractLogs([file]);
  assert.equal(routeRequests.length, 1);
  assert.equal(routeRequests[0].rpId, 218);
  assert.equal(ttsLogs.length, 1);
  assert.equal(ttsLogs[0].requestId, '7');
  assert.equal(ttsLogs[0].status, 'PLAYING');
});
