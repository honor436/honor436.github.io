import test from 'node:test';
import assert from 'node:assert/strict';
import {
  normalizeRect,
  clampRect,
  isValidRect,
  toNaturalRect,
  toDisplayRect,
  createArea,
  addArea,
  removeArea,
  updateArea,
  hitTest,
  areaLabel,
  areaCoords,
  sanitizeMapName,
  escapeHtml,
  renderAreaListHtml,
  renderImageMapHtml,
  renderPreviewDocument,
  needsSplit,
  planSlices,
  clipAreaToSlice,
  sliceFileName,
  renderSlicedImageMapHtml,
  renderSliceListHtml,
  formatBytes,
} from '../ImageMapper/js/imagemap.js';

// ---- normalizeRect (드래그 시작/끝 → 좌상단 기준 사각형) ------------------ //

test('normalizeRect_left_to_right_drag_keeps_origin', () => {
  assert.deepEqual(normalizeRect(10, 20, 50, 60), { x: 10, y: 20, w: 40, h: 40 });
});

test('normalizeRect_right_to_left_drag_flips_origin', () => {
  assert.deepEqual(normalizeRect(50, 60, 10, 20), { x: 10, y: 20, w: 40, h: 40 });
});

test('normalizeRect_zero_drag_returns_empty_rect', () => {
  assert.deepEqual(normalizeRect(30, 30, 30, 30), { x: 30, y: 30, w: 0, h: 0 });
});

// ---- clampRect (이미지 경계 밖으로 나가지 않게) --------------------------- //

test('clampRect_inside_bounds_is_unchanged', () => {
  const rect = { x: 10, y: 10, w: 30, h: 30 };
  assert.deepEqual(clampRect(rect, { width: 100, height: 100 }), rect);
});

test('clampRect_cuts_right_and_bottom_overflow', () => {
  assert.deepEqual(
    clampRect({ x: 80, y: 90, w: 40, h: 40 }, { width: 100, height: 100 }),
    { x: 80, y: 90, w: 20, h: 10 }
  );
});

test('clampRect_pulls_negative_origin_to_zero', () => {
  assert.deepEqual(
    clampRect({ x: -10, y: -5, w: 30, h: 30 }, { width: 100, height: 100 }),
    { x: 0, y: 0, w: 20, h: 25 }
  );
});

// ---- isValidRect (너무 작은 드래그는 영역으로 치지 않는다) ---------------- //

test('isValidRect_big_enough_returns_true', () => {
  assert.equal(isValidRect({ x: 0, y: 0, w: 12, h: 12 }, 8), true);
});

test('isValidRect_too_small_returns_false', () => {
  assert.equal(isValidRect({ x: 0, y: 0, w: 12, h: 3 }, 8), false);
});

// ---- 표시 좌표 ↔ 원본 이미지 좌표 ---------------------------------------- //
//
// 영역은 항상 원본 이미지 픽셀 기준으로 저장한다. 화면에 축소 표시되어도
// 생성되는 이미지 맵 좌표가 원본과 어긋나면 안 된다.

test('toNaturalRect_scales_display_rect_up_to_image_pixels', () => {
  assert.deepEqual(
    toNaturalRect({ x: 25, y: 50, w: 100, h: 40 }, 0.5),
    { x: 50, y: 100, w: 200, h: 80 }
  );
});

test('toNaturalRect_rounds_to_integer_pixels', () => {
  assert.deepEqual(
    toNaturalRect({ x: 10, y: 10, w: 10, h: 10 }, 0.3),
    { x: 33, y: 33, w: 33, h: 33 }
  );
});

test('toDisplayRect_is_inverse_of_toNaturalRect', () => {
  const display = { x: 25, y: 50, w: 100, h: 40 };
  assert.deepEqual(toDisplayRect(toNaturalRect(display, 0.5), 0.5), display);
});

// ---- 영역 모델 ------------------------------------------------------------ //

test('createArea_has_id_rect_and_meta', () => {
  const area = createArea({ x: 1, y: 2, w: 3, h: 4 }, { name: '로그인', href: '/login' });
  assert.equal(area.shape, 'rect');
  assert.deepEqual([area.x, area.y, area.w, area.h], [1, 2, 3, 4]);
  assert.equal(area.name, '로그인');
  assert.equal(area.href, '/login');
  assert.ok(area.id);
});

test('createArea_without_meta_uses_empty_defaults', () => {
  const area = createArea({ x: 0, y: 0, w: 10, h: 10 });
  assert.equal(area.name, '');
  assert.equal(area.href, '');
});

test('createArea_ids_are_unique', () => {
  const a = createArea({ x: 0, y: 0, w: 10, h: 10 });
  const b = createArea({ x: 0, y: 0, w: 10, h: 10 });
  assert.notEqual(a.id, b.id);
});

test('addArea_returns_new_array_without_mutating', () => {
  const areas = [];
  const next = addArea(areas, createArea({ x: 0, y: 0, w: 5, h: 5 }));
  assert.equal(areas.length, 0);
  assert.equal(next.length, 1);
});

test('removeArea_drops_matching_id', () => {
  const a = createArea({ x: 0, y: 0, w: 5, h: 5 });
  const b = createArea({ x: 9, y: 9, w: 5, h: 5 });
  assert.deepEqual(removeArea([a, b], a.id), [b]);
});

test('removeArea_unknown_id_keeps_list', () => {
  const a = createArea({ x: 0, y: 0, w: 5, h: 5 });
  assert.deepEqual(removeArea([a], 'nope'), [a]);
});

test('updateArea_patches_only_target', () => {
  const a = createArea({ x: 0, y: 0, w: 5, h: 5 }, { name: '가' });
  const b = createArea({ x: 9, y: 9, w: 5, h: 5 }, { name: '나' });
  const next = updateArea([a, b], b.id, { name: '다', href: '#x' });
  assert.equal(next[0].name, '가');
  assert.equal(next[1].name, '다');
  assert.equal(next[1].href, '#x');
  assert.equal(b.name, '나', '원본은 그대로여야 한다');
});

// ---- hitTest (편집 화면에서 기존 영역 클릭) ------------------------------- //

test('hitTest_point_inside_returns_area', () => {
  const a = createArea({ x: 10, y: 10, w: 20, h: 20 });
  assert.equal(hitTest([a], { x: 15, y: 15 }), a);
});

test('hitTest_point_outside_returns_null', () => {
  const a = createArea({ x: 10, y: 10, w: 20, h: 20 });
  assert.equal(hitTest([a], { x: 5, y: 5 }), null);
});

test('hitTest_overlapping_returns_topmost_last_added', () => {
  const under = createArea({ x: 0, y: 0, w: 50, h: 50 });
  const over = createArea({ x: 10, y: 10, w: 20, h: 20 });
  assert.equal(hitTest([under, over], { x: 15, y: 15 }), over);
});

test('hitTest_edge_is_inside', () => {
  const a = createArea({ x: 10, y: 10, w: 20, h: 20 });
  assert.equal(hitTest([a], { x: 30, y: 30 }), a);
});

// ---- 라벨 / 좌표 문자열 --------------------------------------------------- //

test('areaLabel_uses_name_when_present', () => {
  assert.equal(areaLabel(createArea({ x: 0, y: 0, w: 1, h: 1 }, { name: '메뉴' }), 0), '메뉴');
});

test('areaLabel_falls_back_to_index', () => {
  assert.equal(areaLabel(createArea({ x: 0, y: 0, w: 1, h: 1 }), 2), '영역 3');
});

test('areaCoords_is_left_top_right_bottom', () => {
  assert.equal(areaCoords({ x: 10, y: 20, w: 30, h: 40 }), '10,20,40,60');
});

// ---- map name / 이스케이프 ------------------------------------------------ //

test('sanitizeMapName_replaces_spaces_and_symbols', () => {
  assert.equal(sanitizeMapName('내 이미지 맵!'), '내-이미지-맵');
});

test('sanitizeMapName_empty_uses_default', () => {
  assert.equal(sanitizeMapName(''), 'image-map');
  assert.equal(sanitizeMapName('   '), 'image-map');
});

test('escapeHtml_escapes_quotes_and_brackets', () => {
  assert.equal(escapeHtml('<a href="x">&</a>'), '&lt;a href=&quot;x&quot;&gt;&amp;&lt;/a&gt;');
});

// ---- 영역 목록 렌더 ------------------------------------------------------- //

test('renderAreaListHtml_empty_returns_empty_string', () => {
  assert.equal(renderAreaListHtml([]), '');
});

test('renderAreaListHtml_contains_label_and_coords', () => {
  const html = renderAreaListHtml([createArea({ x: 10, y: 20, w: 30, h: 40 }, { name: '홈' })]);
  assert.match(html, /홈/);
  assert.match(html, /10,20,40,60/);
});

test('renderAreaListHtml_marks_row_with_area_id', () => {
  const a = createArea({ x: 0, y: 0, w: 5, h: 5 });
  assert.match(renderAreaListHtml([a]), new RegExp(`data-id="${a.id}"`));
});

test('renderAreaListHtml_escapes_name', () => {
  const html = renderAreaListHtml([createArea({ x: 0, y: 0, w: 5, h: 5 }, { name: '<b>x</b>' })]);
  assert.doesNotMatch(html, /<b>x<\/b>/);
  assert.match(html, /&lt;b&gt;x&lt;\/b&gt;/);
});

// ---- 이미지 맵 HTML 생성 (완료 버튼 결과물) ------------------------------- //

test('renderImageMapHtml_has_img_with_usemap', () => {
  const html = renderImageMapHtml({ src: 'photo.png', mapName: 'my map', areas: [] });
  assert.match(html, /<img[^>]+src="photo\.png"/);
  assert.match(html, /usemap="#my-map"/);
});

test('renderImageMapHtml_map_tag_name_matches_usemap', () => {
  const html = renderImageMapHtml({ src: 'photo.png', mapName: 'my map', areas: [] });
  assert.match(html, /<map name="my-map">/);
});

test('renderImageMapHtml_renders_one_area_per_region', () => {
  const html = renderImageMapHtml({
    src: 'photo.png',
    mapName: 'm',
    areas: [
      createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '가', href: '/a' }),
      createArea({ x: 20, y: 20, w: 10, h: 10 }, { name: '나', href: '/b' }),
    ],
  });
  assert.equal([...html.matchAll(/<area\b/g)].length, 2);
  assert.match(html, /coords="0,0,10,10"/);
  assert.match(html, /coords="20,20,30,30"/);
  assert.match(html, /href="\/a"/);
  assert.match(html, /alt="가"/);
});

test('renderImageMapHtml_area_without_href_uses_hash', () => {
  const html = renderImageMapHtml({
    src: 'p.png',
    mapName: 'm',
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '버튼' })],
  });
  assert.match(html, /href="#"/);
});

test('renderImageMapHtml_area_shape_is_rect', () => {
  const html = renderImageMapHtml({
    src: 'p.png',
    mapName: 'm',
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 })],
  });
  assert.match(html, /shape="rect"/);
});

test('renderImageMapHtml_escapes_attribute_values', () => {
  const html = renderImageMapHtml({
    src: 'p.png',
    mapName: 'm',
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '"큰따옴표"', href: '"><script>' })],
  });
  assert.doesNotMatch(html, /<script>/);
  assert.match(html, /&quot;/);
});

test('renderImageMapHtml_uses_alt_for_image', () => {
  const html = renderImageMapHtml({ src: 'p.png', mapName: 'm', areas: [], alt: '안내도' });
  assert.match(html, /<img[^>]+alt="안내도"/);
});

// ---- 테스트 페이지 문서 (테스트 버튼 결과물) ------------------------------ //
//
// "테스트" 버튼은 생성된 이미지 맵이 실제로 동작하는 독립 HTML 페이지를 띄운다.
// 새 창/blob URL 어디에 넣어도 그대로 열리도록 완결된 문서여야 한다.

test('renderPreviewDocument_is_a_full_html_document', () => {
  const doc = renderPreviewDocument({ src: 'p.png', mapName: 'm', areas: [] });
  assert.match(doc, /^<!DOCTYPE html>/i);
  assert.match(doc, /<html lang="ko">/);
  assert.match(doc, /<\/html>\s*$/);
});

test('renderPreviewDocument_embeds_the_image_map', () => {
  const doc = renderPreviewDocument({
    src: 'data:image/png;base64,AAA',
    mapName: '안내도 맵',
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '홈', href: '/home' })],
  });
  assert.match(doc, /usemap="#안내도-맵"/);
  assert.match(doc, /<map name="안내도-맵">/);
  assert.match(doc, /coords="0,0,10,10"/);
  assert.match(doc, /src="data:image\/png;base64,AAA"/);
});

test('renderPreviewDocument_uses_title', () => {
  const doc = renderPreviewDocument({ src: 'p.png', mapName: 'm', areas: [], title: '층별 안내도' });
  assert.match(doc, /<title>층별 안내도<\/title>/);
});

test('renderPreviewDocument_escapes_title', () => {
  const doc = renderPreviewDocument({ src: 'p.png', mapName: 'm', areas: [], title: '<script>' });
  assert.doesNotMatch(doc, /<title><script><\/title>/);
  assert.match(doc, /&lt;script&gt;/);
});

test('renderPreviewDocument_includes_click_handler_script', () => {
  const doc = renderPreviewDocument({
    src: 'p.png',
    mapName: 'm',
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '버튼' })],
  });
  assert.match(doc, /<script>/);
  assert.match(doc, /addEventListener\('click'/);
});

test('renderPreviewDocument_keeps_area_map_responsive_to_image_size', () => {
  // 원본 크기를 넘겨주면 축소 표시돼도 좌표가 맞도록 스케일 보정 코드가 들어간다.
  const doc = renderPreviewDocument({
    src: 'p.png',
    mapName: 'm',
    width: 1200,
    height: 800,
    areas: [createArea({ x: 0, y: 0, w: 10, h: 10 })],
  });
  assert.match(doc, /data-natural-width="1200"/);
  assert.match(doc, /data-natural-height="800"/);
});

// ---- 대용량 이미지 분할 ---------------------------------------------------- //
//
// 1MB 를 넘는 이미지는 세로로 잘라 여러 장으로 내보낸다. 화면에서는 틈 없이
// 이어 붙여 한 장처럼 보이고, 클릭 영역도 잘린 경계를 넘어 그대로 동작해야 한다.

test('needsSplit_small_image_returns_false', () => {
  assert.equal(needsSplit(500 * 1024), false);
});

test('needsSplit_over_one_megabyte_returns_true', () => {
  assert.equal(needsSplit(1024 * 1024 + 1), true);
});

test('needsSplit_custom_limit', () => {
  assert.equal(needsSplit(300, 200), true);
  assert.equal(needsSplit(100, 200), false);
});

test('planSlices_small_image_is_one_slice', () => {
  assert.deepEqual(planSlices({ height: 900, byteSize: 100 * 1024 }), [
    { index: 0, y: 0, height: 900 },
  ]);
});

test('planSlices_count_follows_byte_size_over_limit', () => {
  const slices = planSlices({ height: 900, byteSize: 300, limit: 100 });
  assert.equal(slices.length, 3);
});

test('planSlices_splits_height_and_last_takes_remainder', () => {
  assert.deepEqual(planSlices({ height: 1000, byteSize: 300, limit: 100 }), [
    { index: 0, y: 0, height: 334 },
    { index: 1, y: 334, height: 334 },
    { index: 2, y: 668, height: 332 },
  ]);
});

test('planSlices_covers_full_height_without_gap', () => {
  const slices = planSlices({ height: 1234, byteSize: 700, limit: 100 });
  assert.equal(slices[0].y, 0);
  const last = slices[slices.length - 1];
  assert.equal(last.y + last.height, 1234);
  for (let i = 1; i < slices.length; i++) {
    assert.equal(slices[i].y, slices[i - 1].y + slices[i - 1].height, '조각 사이에 틈이 있다');
  }
});

test('planSlices_never_returns_zero_height_slice', () => {
  for (const s of planSlices({ height: 5, byteSize: 1000, limit: 100 })) {
    assert.ok(s.height > 0, `높이 0 조각: ${JSON.stringify(s)}`);
  }
});

// ---- 영역을 조각 좌표로 자르기 -------------------------------------------- //

test('clipAreaToSlice_inside_slice_translates_local_y', () => {
  const area = createArea({ x: 10, y: 350, w: 40, h: 20 });
  const clipped = clipAreaToSlice(area, { index: 1, y: 300, height: 300 });
  assert.deepEqual([clipped.x, clipped.y, clipped.w, clipped.h], [10, 50, 40, 20]);
});

test('clipAreaToSlice_outside_slice_returns_null', () => {
  const area = createArea({ x: 10, y: 50, w: 40, h: 20 });
  assert.equal(clipAreaToSlice(area, { index: 1, y: 300, height: 300 }), null);
});

test('clipAreaToSlice_spanning_boundary_keeps_only_overlap', () => {
  const area = createArea({ x: 0, y: 280, w: 40, h: 60 }); // 280~340
  const upper = clipAreaToSlice(area, { index: 0, y: 0, height: 300 });
  const lower = clipAreaToSlice(area, { index: 1, y: 300, height: 300 });
  assert.deepEqual([upper.y, upper.h], [280, 20]);
  assert.deepEqual([lower.y, lower.h], [0, 40]);
});

test('clipAreaToSlice_keeps_name_and_href', () => {
  const area = createArea({ x: 0, y: 0, w: 10, h: 10 }, { name: '홈', href: '/home' });
  const clipped = clipAreaToSlice(area, { index: 0, y: 0, height: 300 });
  assert.equal(clipped.name, '홈');
  assert.equal(clipped.href, '/home');
});

// ---- 조각 파일명 ---------------------------------------------------------- //

test('sliceFileName_inserts_index_before_extension', () => {
  assert.equal(sliceFileName('photo.png', 0), 'photo-1.png');
  assert.equal(sliceFileName('photo.png', 2), 'photo-3.png');
});

test('sliceFileName_without_extension', () => {
  assert.equal(sliceFileName('photo', 1), 'photo-2');
});

// ---- 분할 이미지 맵 HTML --------------------------------------------------- //

const SLICES_3 = [
  { index: 0, y: 0, height: 300 },
  { index: 1, y: 300, height: 300 },
  { index: 2, y: 600, height: 200 },
];

test('renderSlicedImageMapHtml_one_img_per_slice', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [],
    mapName: 'm',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.equal([...html.matchAll(/<img\b/g)].length, 3);
  assert.match(html, /src="p-1\.png"/);
  assert.match(html, /src="p-3\.png"/);
});

test('renderSlicedImageMapHtml_each_slice_has_its_own_map', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [createArea({ x: 0, y: 10, w: 10, h: 10 })],
    mapName: 'shop',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.match(html, /usemap="#shop-1"/);
  assert.match(html, /<map name="shop-1">/);
  assert.match(html, /usemap="#shop-3"/);
  assert.match(html, /<map name="shop-3">/);
});

test('renderSlicedImageMapHtml_area_only_in_its_own_slice', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [createArea({ x: 0, y: 320, w: 50, h: 40 }, { name: '가운데' })],
    mapName: 'm',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.equal([...html.matchAll(/<area\b/g)].length, 1);
  assert.match(html, /coords="0,20,50,60"/); // 2번째 조각 로컬 좌표
});

test('renderSlicedImageMapHtml_area_spanning_boundary_appears_in_both_slices', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [createArea({ x: 0, y: 280, w: 50, h: 60 }, { name: '경계' })],
    mapName: 'm',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.equal([...html.matchAll(/<area\b/g)].length, 2);
  assert.match(html, /coords="0,280,50,300"/); // 1번 조각 아래쪽
  assert.match(html, /coords="0,0,50,40"/);    // 2번 조각 위쪽
});

test('renderSlicedImageMapHtml_slices_stack_without_gap', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [],
    mapName: 'm',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.match(html, /display:\s*block/);
  assert.match(html, /font-size:\s*0/);
});

test('renderSlicedImageMapHtml_img_carries_slice_natural_size', () => {
  const html = renderSlicedImageMapHtml({
    slices: SLICES_3,
    areas: [],
    mapName: 'm',
    width: 600,
    srcFor: (s) => `p-${s.index + 1}.png`,
  });
  assert.match(html, /data-natural-width="600"[^>]*data-natural-height="300"/);
  assert.match(html, /data-natural-height="200"/);
});

// ---- 분할 상태의 테스트 페이지 --------------------------------------------- //

test('renderPreviewDocument_with_slices_renders_every_slice', () => {
  const doc = renderPreviewDocument({
    slices: SLICES_3,
    srcFor: (s) => `data:image/png;base64,S${s.index}`,
    mapName: 'm',
    width: 600,
    areas: [createArea({ x: 0, y: 280, w: 50, h: 60 })],
  });
  assert.equal([...doc.matchAll(/<img\b/g)].length, 3);
  assert.match(doc, /base64,S2"/);
});

test('renderPreviewDocument_without_slices_keeps_single_image', () => {
  const doc = renderPreviewDocument({ src: 'p.png', mapName: 'm', areas: [] });
  assert.equal([...doc.matchAll(/<img\b/g)].length, 1);
});

// ---- 조각 목록 표시 -------------------------------------------------------- //

test('formatBytes_kilobytes', () => {
  assert.equal(formatBytes(1536), '1.5KB');
});

test('formatBytes_megabytes', () => {
  assert.equal(formatBytes(2 * 1024 * 1024), '2.0MB');
});

test('formatBytes_bytes', () => {
  assert.equal(formatBytes(512), '512B');
});

test('renderSliceListHtml_empty_returns_empty_string', () => {
  assert.equal(renderSliceListHtml([]), '');
});

test('renderSliceListHtml_shows_index_size_and_height', () => {
  const html = renderSliceListHtml([
    { index: 0, y: 0, height: 900, size: 700 * 1024, dataUrl: 'data:,a', name: 'p-1.png' },
  ]);
  assert.match(html, /p-1\.png/);
  assert.match(html, /900/);
  assert.match(html, /700\.0KB/);
});

test('renderSliceListHtml_row_has_download_button_with_index', () => {
  const html = renderSliceListHtml([
    { index: 0, y: 0, height: 10, size: 10, dataUrl: 'data:,a', name: 'p-1.png' },
    { index: 1, y: 10, height: 10, size: 10, dataUrl: 'data:,b', name: 'p-2.png' },
  ]);
  assert.match(html, /data-slice="0"/);
  assert.match(html, /data-slice="1"/);
});

test('renderSliceListHtml_marks_oversized_slice', () => {
  const html = renderSliceListHtml([
    { index: 0, y: 0, height: 10, size: 2 * 1024 * 1024, dataUrl: 'data:,a', name: 'p-1.png' },
  ]);
  assert.match(html, /over/);
});
