import test from 'node:test';
import assert from 'node:assert/strict';
import {
  groupGuidancePointsByCoord,
  buildGuidanceChooserHtml,
} from '../DltLogViewer/js/tvas-renderer.js';

// 안내점(GP)이 같은 좌표(=같은 vxIndex)에 겹칠 때, 그룹으로 묶어 한 마커에서
// 여러 안내점을 선택해 볼 수 있게 한다. (DltLogViewer-beta 전용)

const coords = [
  { lat: 37.5000, lon: 127.0000 },
  { lat: 37.5010, lon: 127.0010 },
];
const gps = [
  { vxIndex: 0, guidanceCode: 11, continuousTurnCode: 0 },
  { vxIndex: 0, guidanceCode: 42, continuousTurnCode: 0 },
  { vxIndex: 1, guidanceCode: 5,  continuousTurnCode: 0 },
];

test('groupGuidancePointsByCoord_groups_overlapping_by_coord', () => {
  const groups = groupGuidancePointsByCoord(gps, coords);
  assert.equal(groups.length, 2);      // 좌표 2종
  assert.equal(groups[0].length, 2);   // vx0 에 겹친 2개
  assert.equal(groups[1].length, 1);   // vx1 단독
});

test('groupGuidancePointsByCoord_skips_out_of_range_vx', () => {
  const groups = groupGuidancePointsByCoord([{ vxIndex: 99, guidanceCode: 1 }], coords);
  assert.equal(groups.length, 0);
});

test('groupGuidancePointsByCoord_preserves_encounter_order', () => {
  const groups = groupGuidancePointsByCoord(gps, coords);
  assert.equal(groups[0][0].guidanceCode, 11);
  assert.equal(groups[0][1].guidanceCode, 42);
});

test('buildGuidanceChooserHtml_shows_tabs_and_selected_detail', () => {
  const group = groupGuidancePointsByCoord(gps, coords)[0]; // 겹친 2개
  const html = buildGuidanceChooserHtml(group, 1, coords, null, null);
  assert.match(html, /겹친 안내점 2개/);
  assert.match(html, /data-gp-idx="0"/);
  assert.match(html, /data-gp-idx="1"/);
  assert.match(html, /코드: 42/);   // 선택(idx 1)된 안내점 상세
});

test('buildGuidanceChooserHtml_clamps_index', () => {
  const group = groupGuidancePointsByCoord(gps, coords)[0];
  // 범위를 벗어난 idx 는 마지막으로 클램프되어 오류 없이 렌더
  const html = buildGuidanceChooserHtml(group, 9, coords, null, null);
  assert.match(html, /코드: 42/);
});
