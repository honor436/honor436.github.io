// RP 라벨 파싱 테스트 (기존 RP-숫자 + 변경 RP-ROUTE-숫자)
import test from 'node:test';
import assert from 'node:assert/strict';
import { parseRpLabel } from '../DltLogViewer/js/extractor.js';

test('parseRpLabel 기존 형식 RP-62-Traffic_Recommend', () => {
  const r = parseRpLabel('RP-62-Traffic_Recommend');
  assert.equal(r.rpPrefix, null);
  assert.equal(r.rpId, 62);
  assert.equal(r.rpOption, 'Traffic_Recommend');
  assert.equal(r.rawLabel, 'RP-62-Traffic_Recommend');
});

test('parseRpLabel 변경 형식 RP-ROUTE-132-Traffic_Recommend', () => {
  const r = parseRpLabel('RP-ROUTE-132-Traffic_Recommend');
  assert.equal(r.rpPrefix, 'ROUTE');
  assert.equal(r.rpId, 132);
  assert.equal(r.rpOption, 'Traffic_Recommend');
  assert.equal(r.rawLabel, 'RP-ROUTE-132-Traffic_Recommend');
});

test('parseRpLabel 기존 RP-218-Traffic_MinTime', () => {
  const r = parseRpLabel('RP-218-Traffic_MinTime');
  assert.equal(r.rpId, 218);
  assert.equal(r.rpOption, 'Traffic_MinTime');
});

test('parseRpLabel 옵션에 하이픈 포함돼도 OK', () => {
  const r = parseRpLabel('RP-ROUTE-5-A-B-C');
  assert.equal(r.rpPrefix, 'ROUTE');
  assert.equal(r.rpId, 5);
  assert.equal(r.rpOption, 'A-B-C');
});

test('parseRpLabel 형식 불일치 시 null', () => {
  assert.equal(parseRpLabel('NOT-A-LABEL'), null);
  assert.equal(parseRpLabel('RP-Traffic_Recommend'), null);   // 숫자 없음
  assert.equal(parseRpLabel(''), null);
});
