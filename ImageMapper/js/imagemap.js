// 이미지 맵 생성기 — 순수 로직 (DOM 의존 없음)
//
// 영역(area)은 항상 "원본 이미지 픽셀" 기준으로 저장한다. 화면에는 축소/확대되어
// 표시될 수 있으므로, 표시 좌표 ↔ 원본 좌표 변환은 scale 로만 처리한다.
//
// 용량이 큰 이미지는 세로로 잘라 여러 장으로 내보낸다(쇼핑몰 상세페이지 방식).
// 조각들은 틈 없이 이어 붙여 한 장처럼 보이고, 클릭 영역은 조각 경계를 넘어도
// 각 조각의 로컬 좌표로 나뉘어 그대로 동작한다.

const DEFAULT_MAP_NAME = 'image-map';

/** 조각 하나의 목표 최대 용량 (1MB) */
export const MAX_SLICE_BYTES = 1024 * 1024;

// ---------- 사각형 ---------------------------------------------------------- //

/** 드래그 시작점/끝점 → 좌상단 기준 사각형 (역방향 드래그 허용) */
export function normalizeRect(x1, y1, x2, y2) {
  return {
    x: Math.min(x1, x2),
    y: Math.min(y1, y2),
    w: Math.abs(x2 - x1),
    h: Math.abs(y2 - y1),
  };
}

/** 이미지 경계 안쪽으로 잘라낸다. */
export function clampRect(rect, bounds) {
  const left = Math.max(0, rect.x);
  const top = Math.max(0, rect.y);
  const right = Math.min(bounds.width, rect.x + rect.w);
  const bottom = Math.min(bounds.height, rect.y + rect.h);
  return {
    x: left,
    y: top,
    w: Math.max(0, right - left),
    h: Math.max(0, bottom - top),
  };
}

/** 최소 크기 미만의 드래그(오클릭)는 영역으로 만들지 않는다. */
export function isValidRect(rect, min = 8) {
  return !!rect && rect.w >= min && rect.h >= min;
}

/** 표시 좌표 → 원본 이미지 좌표 */
export function toNaturalRect(rect, scale) {
  const s = scale || 1;
  return {
    x: Math.round(rect.x / s),
    y: Math.round(rect.y / s),
    w: Math.round(rect.w / s),
    h: Math.round(rect.h / s),
  };
}

/** 원본 이미지 좌표 → 표시 좌표 */
export function toDisplayRect(rect, scale) {
  const s = scale || 1;
  return {
    x: Math.round(rect.x * s),
    y: Math.round(rect.y * s),
    w: Math.round(rect.w * s),
    h: Math.round(rect.h * s),
  };
}

// ---------- 영역 모델 ------------------------------------------------------- //

let seq = 0;

export function createArea(rect, meta = {}) {
  seq += 1;
  return {
    id: `a${Date.now().toString(36)}-${seq}`,
    shape: 'rect',
    x: rect.x,
    y: rect.y,
    w: rect.w,
    h: rect.h,
    name: meta.name || '',
    href: meta.href || '',
  };
}

export function addArea(areas, area) {
  return [...areas, area];
}

export function removeArea(areas, id) {
  return areas.filter((a) => a.id !== id);
}

export function updateArea(areas, id, patch) {
  return areas.map((a) => (a.id === id ? { ...a, ...patch } : a));
}

/** 위에 그려진(나중에 추가된) 영역이 우선. 없으면 null. */
export function hitTest(areas, point) {
  for (let i = areas.length - 1; i >= 0; i--) {
    const a = areas[i];
    if (point.x >= a.x && point.x <= a.x + a.w && point.y >= a.y && point.y <= a.y + a.h) {
      return a;
    }
  }
  return null;
}

export function areaLabel(area, index) {
  return area.name && area.name.trim() ? area.name : `영역 ${index + 1}`;
}

/** <area coords> 형식: left,top,right,bottom */
export function areaCoords(rect) {
  return [
    Math.round(rect.x),
    Math.round(rect.y),
    Math.round(rect.x + rect.w),
    Math.round(rect.y + rect.h),
  ].join(',');
}

// ---------- 대용량 이미지 분할 ---------------------------------------------- //

/** 1MB(기본)를 넘으면 분할 대상 */
export function needsSplit(byteSize, limit = MAX_SLICE_BYTES) {
  return Number(byteSize || 0) > limit;
}

/**
 * 세로 분할 계획. 용량이 limit 의 n 배면 n 조각으로 균등하게 나눈다.
 * 조각 사이에 틈이나 겹침이 없도록 마지막 조각이 나머지를 가져간다.
 */
export function planSlices({ height, byteSize = 0, limit = MAX_SLICE_BYTES, count }) {
  const total = Math.max(1, Math.round(height));
  const wanted = count || Math.ceil(byteSize / limit) || 1;
  const n = Math.max(1, Math.min(total, wanted));
  const sliceHeight = Math.ceil(total / n);

  const slices = [];
  for (let y = 0, index = 0; y < total; y += sliceHeight, index++) {
    slices.push({ index, y, height: Math.min(sliceHeight, total - y) });
  }
  return slices;
}

/** 영역을 조각 좌표계로 자른다. 겹치지 않으면 null. */
export function clipAreaToSlice(area, slice) {
  const top = Math.max(area.y, slice.y);
  const bottom = Math.min(area.y + area.h, slice.y + slice.height);
  if (bottom - top <= 0) return null;
  return { ...area, y: top - slice.y, h: bottom - top };
}

/** photo.png → photo-1.png */
export function sliceFileName(fileName, index) {
  const name = String(fileName || 'image');
  const n = index + 1;
  const dot = name.lastIndexOf('.');
  if (dot <= 0) return `${name}-${n}`;
  return `${name.slice(0, dot)}-${n}${name.slice(dot)}`;
}

/** 사람이 읽는 용량 표기 */
export function formatBytes(bytes) {
  const n = Number(bytes) || 0;
  if (n >= 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)}MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(1)}KB`;
  return `${n}B`;
}

// ---------- 문자열 유틸 ----------------------------------------------------- //

export function sanitizeMapName(name) {
  const cleaned = String(name || '')
    .trim()
    .replace(/\s+/g, '-')
    .replace(/[^\p{L}\p{N}_-]/gu, '')
    .replace(/-{2,}/g, '-')
    .replace(/^-+|-+$/g, '');
  return cleaned || DEFAULT_MAP_NAME;
}

export function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

// ---------- 렌더 ------------------------------------------------------------ //

/** 편집 화면 오른쪽의 영역 목록 */
export function renderAreaListHtml(areas) {
  if (!areas.length) return '';
  return areas
    .map((area, i) => {
      const label = escapeHtml(areaLabel(area, i));
      return `<li class="area-row" data-id="${escapeHtml(area.id)}">
  <span class="area-index">${i + 1}</span>
  <div class="area-fields">
    <input class="area-name" type="text" value="${escapeHtml(area.name)}" placeholder="${label}" data-field="name">
    <input class="area-href" type="text" value="${escapeHtml(area.href)}" placeholder="링크 (비우면 버튼 동작)" data-field="href">
    <span class="area-coords">${areaCoords(area)}</span>
  </div>
  <button class="area-del" type="button" data-action="delete" title="영역 삭제">✕</button>
</li>`;
    })
    .join('\n');
}

/** 분할 조각 목록 (썸네일 · 크기 · 다운로드) */
export function renderSliceListHtml(slices, limit = MAX_SLICE_BYTES) {
  if (!slices.length) return '';
  return slices
    .map((slice, i) => {
      const over = slice.size > limit ? ' over' : '';
      return `<li class="slice-row${over}" data-slice="${i}">
  <img class="slice-thumb" src="${escapeHtml(slice.dataUrl)}" alt="">
  <div class="slice-meta">
    <b class="slice-name">${escapeHtml(slice.name || sliceFileName('image.png', i))}</b>
    <span class="slice-size">높이 ${Math.round(slice.height)}px · ${formatBytes(slice.size)}</span>
  </div>
  <button class="slice-download" type="button" data-slice="${i}" title="이 조각 다운로드">↓</button>
</li>`;
    })
    .join('\n');
}

function areaTag(rect, label, href) {
  const coords = areaCoords(rect);
  const link = href && href.trim() ? href.trim() : '#';
  return `  <area shape="rect" coords="${coords}" data-coords="${coords}" href="${escapeHtml(link)}" alt="${escapeHtml(label)}" title="${escapeHtml(label)}">`;
}

function imgTag({ src, alt, mapId, width, height, extra = '' }) {
  const size = [
    Number.isFinite(width) ? ` data-natural-width="${Math.round(width)}"` : '',
    Number.isFinite(height) ? ` data-natural-height="${Math.round(height)}"` : '',
  ].join('');
  return `<img${extra} src="${escapeHtml(src)}" alt="${escapeHtml(alt)}" usemap="#${mapId}"${size}>`;
}

/** 완료 버튼 결과물(단일 이미지): <img usemap> + <map><area> */
export function renderImageMapHtml({ src, mapName, areas = [], alt = '', width, height }) {
  const name = sanitizeMapName(mapName);
  const areaTags = areas.map((area, i) => areaTag(area, areaLabel(area, i), area.href)).join('\n');

  return `${imgTag({ src, alt, mapId: name, width, height, extra: ' id="map-image"' })}
<map name="${name}">
${areaTags}
</map>`;
}

/**
 * 완료 버튼 결과물(분할 이미지): 조각마다 <img>+<map> 을 두고 틈 없이 쌓는다.
 * 화면에서는 한 장처럼 보이고, 경계에 걸친 영역은 양쪽 조각에 나뉘어 들어간다.
 */
export function renderSlicedImageMapHtml({ slices, areas = [], mapName, width, alt = '', srcFor }) {
  const name = sanitizeMapName(mapName);

  const blocks = slices.map((slice, i) => {
    const mapId = `${name}-${i + 1}`;
    const tags = areas
      .map((area, ai) => {
        const clipped = clipAreaToSlice(area, slice);
        return clipped ? areaTag(clipped, areaLabel(area, ai), area.href) : null;
      })
      .filter(Boolean)
      .join('\n');

    const img = imgTag({
      src: srcFor(slice, i),
      alt: i === 0 ? alt : '',
      mapId,
      width,
      height: slice.height,
      extra: ' class="image-map-slice" style="display:block;width:100%;height:auto"',
    });
    return `${img}\n<map name="${mapId}">\n${tags}\n</map>`;
  });

  const maxWidth = Number.isFinite(width) ? `max-width:${Math.round(width)}px;` : '';
  return `<div class="image-map-stack" style="${maxWidth}margin:0 auto;font-size:0;line-height:0">
${blocks.join('\n')}
</div>`;
}

const PREVIEW_SCRIPT = `(function () {
  var imgs = document.querySelectorAll('img[usemap]');
  var toast = document.getElementById('toast');
  var timer = null;

  // 표시 크기가 원본과 다르면 <area> 좌표를 그만큼 다시 계산한다.
  function rescale() {
    for (var i = 0; i < imgs.length; i++) {
      var img = imgs[i];
      var name = (img.getAttribute('usemap') || '').replace('#', '');
      var map = document.getElementsByName(name)[0];
      if (!map) continue;
      var natural = Number(img.getAttribute('data-natural-width')) || img.naturalWidth;
      if (!natural || !img.clientWidth) continue;
      var scale = img.clientWidth / natural;
      var areas = map.getElementsByTagName('area');
      for (var j = 0; j < areas.length; j++) {
        var base = (areas[j].getAttribute('data-coords') || '').split(',');
        var next = [];
        for (var k = 0; k < base.length; k++) next.push(Math.round(Number(base[k]) * scale));
        areas[j].setAttribute('coords', next.join(','));
      }
    }
  }

  function show(message) {
    if (!toast) return;
    toast.textContent = message;
    toast.hidden = false;
    clearTimeout(timer);
    timer = setTimeout(function () { toast.hidden = true; }, 2200);
  }

  document.addEventListener('click', function (e) {
    var area = e.target && e.target.closest ? e.target.closest('area') : null;
    if (!area) return;
    var href = area.getAttribute('href') || '#';
    var label = area.getAttribute('alt') || '영역';
    if (href === '#') {
      e.preventDefault();
      show('클릭됨 · ' + label);
    } else {
      show('이동 · ' + label + ' → ' + href);
    }
  });

  for (var i = 0; i < imgs.length; i++) imgs[i].addEventListener('load', rescale);
  window.addEventListener('resize', rescale);
  window.addEventListener('load', rescale);
  rescale();
})();`;

/** 테스트 버튼 결과물: 이미지 맵이 실제로 동작하는 독립 HTML 문서 */
export function renderPreviewDocument({
  src,
  mapName,
  areas = [],
  alt = '',
  title = '이미지 맵 테스트',
  width,
  height,
  slices,
  srcFor,
}) {
  const sliced = Array.isArray(slices) && slices.length > 0 && typeof srcFor === 'function';
  const body = sliced
    ? renderSlicedImageMapHtml({ slices, areas, mapName, width, alt, srcFor })
    : renderImageMapHtml({ src, mapName, areas, alt, width, height });

  const sub = sliced
    ? `영역 ${areas.length}개 · 분할 이미지 ${slices.length}장을 이어 붙였습니다`
    : `영역 ${areas.length}개 · 클릭하면 동작합니다`;

  return `<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(title)}</title>
<style>
  :root { color-scheme: light; }
  body {
    margin: 0; min-height: 100vh; background: #f5f5f5; color: #1a1a1a;
    font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Noto Sans KR", sans-serif;
    display: flex; flex-direction: column; align-items: center; gap: 14px; padding: 20px 16px 60px;
  }
  header { text-align: center; }
  h1 { margin: 0 0 4px; font-size: 18px; font-weight: 800; letter-spacing: -0.3px; }
  p.hint { margin: 0; color: #999; font-size: 13px; }
  .frame {
    background: #fff; border-radius: 14px; padding: 10px; max-width: 100%;
    box-shadow: 0 2px 10px rgba(0, 0, 0, .06);
  }
  img { max-width: 100%; height: auto; border-radius: 6px; }
  #toast {
    position: fixed; left: 50%; bottom: 26px; transform: translateX(-50%);
    background: #2b9e3f; color: #fff; padding: 11px 20px; border-radius: 999px;
    font-size: 14px; font-weight: 700; box-shadow: 0 6px 18px rgba(43, 158, 63, .35);
  }
  #toast[hidden] { display: none; }
</style>
</head>
<body>
<header>
  <h1>${escapeHtml(title)}</h1>
  <p class="hint">${sub}</p>
</header>
<div class="frame">
${body}
</div>
<div id="toast" hidden></div>
<script>
${PREVIEW_SCRIPT}
</script>
</body>
</html>`;
}
