// 경로 요청 패널의 출발지/경유지/목적지 상태 ↔ 요청 바디(JSON) 반영 로직.
// DOM·지도에 의존하지 않는 순수 함수만 둔다 (index.html 인라인 스크립트에서 사용).

import { resolveRpFlag, resolvePoiId } from './poi-search.js';

/**
 * 출발/경유/목적지 상태를 경로 요청 바디에 반영한 새 객체를 돌려준다.
 * @param {object} body  현재 요청 바디 (변경하지 않음)
 * @param {{depart:object|null, dest:object|null, vias:object[]}} wps
 * @param {(lat:number, lon:number) => [number, number]} toSk  WGS84 → SK 좌표 변환
 */
export function applyWaypointsToRouteBody(body, wps, toSk) {
  const out = { ...body };
  const { depart, dest, vias } = wps;
  if (depart) {
    [out.departXPos, out.departYPos] = toSk(depart.lat, depart.lon);
    // 출발지 명칭: POI name 또는 역지오코딩 buildingName → departName
    if (depart.name) out.departName = depart.name;
    // 출발 방위각(드래그/직접 입력) → angle
    if (depart.angle != null) out.angle = depart.angle;
  }
  if (dest) {
    [out.destXPos, out.destYPos] = toSk(dest.lat, dest.lon);
    if (dest.name) out.destName = dest.name;
    // 목적지 rpFlag: POI(검색/상세) 값 우선, 없으면 16
    out.destRpFlag = resolveRpFlag('dest', dest.rpFlag);
    // POI 가 아닌 지점이면 기본 바디의 샘플 ID 가 남지 않도록 비운다.
    out.destPoiId = resolvePoiId(dest.poiId) ?? '';
    out.destEVChargerFlag = dest.evChargerFlag === true;
  }
  if (vias.length > 0) {
    out.wayPoints = vias.map((v, i) => {
      const [x, y] = toSk(v.lat, v.lon);
      const wp = {
        x,
        y,
        wayPointName: v.name || ('경유지' + (i + 1)),
        wayPointSearchFlag: 'WaypointSearch',
        // 경유지 rpFlag: POI(검색/상세) 값 우선, 없으면 18
        rpFlag: resolveRpFlag('via', v.rpFlag),
        // 경유지 poiID(String): POI 검색 결과로 설정한 경우 그 ID, 아니면 빈 문자열
        poiID: resolvePoiId(v.poiId) ?? '',
        evChargerFlag: v.evChargerFlag === true,
      };
      return wp;
    });
  } else {
    delete out.wayPoints;
  }
  return out;
}

/**
 * 검색/상세 결과 POI → 경로 지점 상태.
 * 전기차 충전소(p.isEvCharger)면 evChargerFlag 를 켠다
 * → 경유지 wayPoints[].evChargerFlag / 목적지 destEVChargerFlag 로 전송.
 */
export function waypointFromPoi(p, lat, lon) {
  return { lat, lon, name: p.name, rpFlag: p.rpFlag, poiId: p.poiId, evChargerFlag: p.isEvCharger === true };
}

/**
 * 패널 카드 입력값을 지점 객체에 반영한다. 반영하면 true, 잘못된 값이면 false(변경 없음).
 * @param {object} wp  출발/경유/목적지 지점 (직접 변경)
 * @param {string} field  lat | lon | name | angle | rpFlag | poiId | evChargerFlag
 * @param {string|boolean} raw  input.value 또는 checkbox.checked
 */
export function updateWaypointField(wp, field, raw) {
  if (field === 'lat' || field === 'lon') {
    const n = String(raw).trim() === '' ? NaN : Number(raw);
    const limit = field === 'lat' ? 90 : 180;
    if (!Number.isFinite(n) || Math.abs(n) > limit) return false;
    wp[field] = n;
    return true;
  }
  if (field === 'name' || field === 'poiId') {
    const s = String(raw).trim();
    if (s) wp[field] = s; else delete wp[field];
    return true;
  }
  if (field === 'rpFlag') {
    const s = String(raw).trim();
    if (s === '') { delete wp.rpFlag; return true; }   // 비우면 기본값(목적지 16 / 경유지 18)
    if (!/^\d+$/.test(s)) return false;
    wp.rpFlag = Number(s);
    return true;
  }
  if (field === 'angle') {
    const s = String(raw).trim();
    if (s === '') { delete wp.angle; return true; }
    if (!/^\d+$/.test(s) || Number(s) > 359) return false;
    wp.angle = Number(s);
    return true;
  }
  if (field === 'evChargerFlag') {
    wp.evChargerFlag = raw === true;
    return true;
  }
  return false;
}

// ---- 패널 카드 렌더링 ------------------------------------------------------ //

/** 출발 → 경유(순서대로) → 목적지 순서의 [key, kind, 경유지 idx, 지점] 목록 */
function orderedWaypoints(wps) {
  const list = [];
  if (wps.depart) list.push(['depart', 'depart', -1, wps.depart]);
  wps.vias.forEach((v, i) => list.push(['via:' + i, 'via', i, v]));
  if (wps.dest) list.push(['dest', 'dest', -1, wps.dest]);
  return list;
}

/**
 * 출발/경유/목적지 세로 카드 목록 HTML.
 * @param {{depart:object|null, dest:object|null, vias:object[]}} wps
 * @param {Set<string>} expanded  펼친 카드 key (depart | via:N | dest)
 * @param {(lat:number, lon:number) => [number, number]} toSk
 */
export function buildWaypointEditorHtml(wps, expanded, toSk) {
  const list = orderedWaypoints(wps);
  if (list.length === 0) {
    return '<div class="wp-empty">지도 우클릭으로 출발/경유/목적지를 설정하세요</div>';
  }
  return list.map(([key, kind, idx, wp]) => cardHtml(key, kind, idx, wp, expanded.has(key), toSk)).join('');
}

const KIND_META = {
  depart: { chip: 'S', label: '출발지' },
  via:    { chip: 'W', label: '경유지' },
  dest:   { chip: 'E', label: '목적지' },
};

function escHtml(s) {
  return String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function textRow(label, field, value, placeholder = '') {
  return `<label class="wp-row"><span>${label}</span>` +
    `<input type="text" data-wp-field="${field}" value="${escHtml(value ?? '')}" placeholder="${escHtml(placeholder)}"></label>`;
}

function checkRow(label, field, checked) {
  return `<label class="wp-row wp-check"><span>${label}</span>` +
    `<input type="checkbox" data-wp-field="${field}"${checked ? ' checked' : ''}></label>`;
}

/** 종류별 요청 옵션 입력 행 */
function kindRows(kind, wp) {
  if (kind === 'depart') {
    return textRow('방위각 angle', 'angle', wp.angle, '0~359');
  }
  // 비우면 기본 rpFlag (목적지 16 / 경유지 18) 가 요청에 들어간다.
  const poiRows = textRow('rpFlag', 'rpFlag', wp.rpFlag, kind === 'dest' ? '16' : '18') +
                  textRow(kind === 'dest' ? 'destPoiId' : 'poiID', 'poiId', wp.poiId);
  const flagName = kind === 'dest' ? 'destEVChargerFlag' : 'evChargerFlag';
  return poiRows + checkRow(flagName, 'evChargerFlag', wp.evChargerFlag === true);
}

function cardHtml(key, kind, idx, wp, open, toSk) {
  const meta = KIND_META[kind];
  const chip = kind === 'via' ? meta.chip + (idx + 1) : meta.chip;
  const title = wp.name ? escHtml(wp.name) : `${wp.lat.toFixed(5)}, ${wp.lon.toFixed(5)}`;
  const head =
    `<div class="wp-card-head" data-wp-toggle>` +
      `<span class="wp-caret">${open ? '▾' : '▸'}</span>` +
      `<span class="wp-chip">${chip}</span>` +
      `<span class="wp-card-title">${title}</span>` +
      `<button type="button" class="wp-del" data-wp-delete title="${meta.label} 삭제">&times;</button>` +
    `</div>`;
  let body = '';
  if (open) {
    const [skX, skY] = toSk(wp.lat, wp.lon);
    body =
      `<div class="wp-card-body">` +
        textRow('명칭', 'name', wp.name) +
        textRow('위도', 'lat', wp.lat.toFixed(6)) +
        textRow('경도', 'lon', wp.lon.toFixed(6)) +
        `<div class="wp-row"><span>SK 좌표</span><code>${skX}, ${skY}</code></div>` +
        kindRows(kind, wp) +
      `</div>`;
  }
  return `<div class="wp-card ${kind}${open ? ' open' : ''}" data-wp-key="${key}">${head}${body}</div>`;
}
