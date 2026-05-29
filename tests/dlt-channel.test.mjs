// DLT 채널 ID (APID+CTID) 추출 회귀 테스트
import test from 'node:test';
import assert from 'node:assert/strict';
import { cleanAndParseRpReqBuffer } from '../DltLogViewer/js/extractor.js';

// 인터리빙된 비-JSON 로그가 끼었을 때 첫 균형 JSON 만 추출되는지
test('cleanAndParseRpReqBuffer 뒤에 잡음 붙어도 첫 JSON 추출', () => {
  const buf = '{"a":1,"b":[1,2,3]}+CID Tx 0x35 garbage trailing';
  const r = cleanAndParseRpReqBuffer(buf);
  assert.equal(r.parseError, null);
  assert.deepEqual(r.parsed, { a: 1, b: [1, 2, 3] });
});

test('cleanAndParseRpReqBuffer 정상 JSON 은 그대로', () => {
  const r = cleanAndParseRpReqBuffer('{"x":10}');
  assert.deepEqual(r.parsed, { x: 10 });
});

test('cleanAndParseRpReqBuffer 문자열 내 중괄호 보존 (consumptionParam)', () => {
  const buf = '{"consumptionParam":"{\\"aux\\":950.0,\\"mass\\":1000}","angle":325}';
  const r = cleanAndParseRpReqBuffer(buf);
  assert.equal(r.parseError, null);
  assert.equal(r.parsed.angle, 325);
  assert.equal(r.parsed.consumptionParam, '{"aux":950.0,"mass":1000}');
});
