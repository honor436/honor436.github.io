import test from 'node:test';
import assert from 'node:assert/strict';
import { formatBuildDate } from '../DltLogViewer/js/build-info.js';

// ---- formatBuildDate ------------------------------------------------------ //
//
// document.lastModified(또는 Date)를 'YYYY-MM-DD' 로 포맷. 빌드(배포) 날짜 표시용.

test('formatBuildDate_from_lastModified_string', () => {
  assert.equal(formatBuildDate('06/19/2026 15:30:00'), '2026-06-19');
});

test('formatBuildDate_from_date_object', () => {
  assert.equal(formatBuildDate(new Date(2026, 0, 5, 9, 0, 0)), '2026-01-05');
});

test('formatBuildDate_invalid_falls_back_to_today_pattern', () => {
  assert.match(formatBuildDate(''), /^\d{4}-\d{2}-\d{2}$/);
  assert.match(formatBuildDate('not a date'), /^\d{4}-\d{2}-\d{2}$/);
});
