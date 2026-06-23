// #onLocationChanged Location[...] 라인 파싱 테스트
import test from 'node:test';
import assert from 'node:assert/strict';
import { parseLocationLine } from '../DltLogViewer/js/extractor.js';

const GPS_LINE =
  'NAVD[820]: #onLocationChanged Location[gps 37.123456,127.234567 hAcc=8.0 ' +
  'et=+1d2h3m4s5ms alt=50.0 vel=16.7 bear=180.5 vAcc=2.0 sAcc=1.0 bAcc=5.0 {Bundle}]';

test('parseLocationLine extracts sourceType/lat/lon/bearing from gps line', () => {
  const r = parseLocationLine(GPS_LINE);
  assert.equal(r.sourceType, 'gps');
  assert.equal(r.lat, 37.123456);
  assert.equal(r.lon, 127.234567);
  assert.equal(r.bearing, 180.5);
});

test('parseLocationLine extracts recorded speed (m/s) from vel= field', () => {
  const r = parseLocationLine(GPS_LINE);
  assert.equal(r.speed, 16.7);
});

test('parseLocationLine speed is null when vel= absent', () => {
  const line =
    'NAVD[820]: #onLocationChanged Location[dr_gps 37.5,127.0 hAcc=8.0 bear=90.0 {Bundle}]';
  const r = parseLocationLine(line);
  assert.equal(r.sourceType, 'dr_gps');
  assert.equal(r.speed, null);
});

test('parseLocationLine returns null for non-location line', () => {
  assert.equal(parseLocationLine('some unrelated log line'), null);
});
