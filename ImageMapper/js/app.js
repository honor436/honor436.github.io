// 이미지 맵 생성기 — DOM 연결
import {
  MAX_SLICE_BYTES,
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
  escapeHtml,
  formatBytes,
  needsSplit,
  planSlices,
  sliceFileName,
  renderAreaListHtml,
  renderSliceListHtml,
  renderImageMapHtml,
  renderSlicedImageMapHtml,
  renderPreviewDocument,
} from './imagemap.js';

const MIN_DRAG_PX = 8;   // 표시 기준 최소 드래그 크기
const MAX_SLICES = 40;   // 조각 수 상한 (재분할 폭주 방지)

const $ = (id) => document.getElementById(id);
const el = {
  fileInput: $('file-input'),
  fileChip: $('file-chip'),
  areaChip: $('area-chip'),
  sliceChip: $('slice-chip'),
  wrap: $('canvas-wrap'),
  empty: $('empty-state'),
  imageBox: $('image-box'),
  preview: $('preview'),
  overlay: $('overlay'),
  guides: $('slice-guides'),
  mapName: $('map-name'),
  imgSrc: $('img-src'),
  docTitle: $('doc-title'),
  areaList: $('area-list'),
  areaEmpty: $('area-empty'),
  slicePanel: $('slice-panel'),
  sliceSummary: $('slice-summary'),
  sliceList: $('slice-list'),
  downloadSlicesBtn: $('download-slices'),
  doneBtn: $('done-btn'),
  testBtn: $('test-btn'),
  clearBtn: $('clear-btn'),
  warn: $('warn'),
  codePanel: $('code-panel'),
  codeOutput: $('code-output'),
  copyBtn: $('copy-btn'),
  downloadBtn: $('download-btn'),
  modal: $('preview-modal'),
  modalClose: $('modal-close'),
  modalNote: $('modal-note'),
  frame: $('preview-frame'),
};

const state = {
  dataUrl: '',
  fileName: '',
  fileSize: 0,
  mimeType: 'image/png',
  naturalWidth: 0,
  naturalHeight: 0,
  areas: [],
  slices: [],       // 1MB 초과 시에만 채워진다
  selectedId: null,
  generated: false,
};

let drag = null;
let draftEl = null;

// ---------- 좌표 ---------------------------------------------------------- //

/** 표시 크기 / 원본 크기 */
function scale() {
  if (!state.naturalWidth) return 1;
  return el.preview.clientWidth / state.naturalWidth;
}

function pointerPos(e) {
  const box = el.overlay.getBoundingClientRect();
  return { x: e.clientX - box.left, y: e.clientY - box.top };
}

function naturalBounds() {
  return { width: state.naturalWidth, height: state.naturalHeight };
}

function isSliced() {
  return state.slices.length > 1;
}

// ---------- 렌더 ---------------------------------------------------------- //

function renderRegions() {
  if (!state.dataUrl) {
    el.overlay.innerHTML = '';
    return;
  }
  const s = scale();
  el.overlay.innerHTML = state.areas
    .map((area, i) => {
      const r = toDisplayRect(area, s);
      const selected = area.id === state.selectedId ? ' selected' : '';
      return `<div class="region${selected}" data-id="${escapeHtml(area.id)}"
        style="left:${r.x}px;top:${r.y}px;width:${r.w}px;height:${r.h}px">
        <span class="tag">${escapeHtml(areaLabel(area, i))}</span>
      </div>`;
    })
    .join('');
  draftEl = null;
}

/** 분할 경계선 — 편집 화면에서는 한 장이지만 어디서 잘리는지 보여준다. */
function renderGuides() {
  if (!isSliced()) {
    el.guides.innerHTML = '';
    return;
  }
  const s = scale();
  el.guides.innerHTML = state.slices
    .slice(1)
    .map((slice) => `<div class="slice-guide" style="top:${Math.round(slice.y * s)}px"></div>`)
    .join('');
}

function renderList() {
  el.areaList.innerHTML = renderAreaListHtml(state.areas);
  el.areaEmpty.hidden = state.areas.length > 0;
  const selected = el.areaList.querySelector(`[data-id="${CSS.escape(state.selectedId || '')}"]`);
  if (selected) selected.classList.add('selected');
}

function renderSlices() {
  const sliced = isSliced();
  el.slicePanel.hidden = !sliced;
  el.sliceChip.hidden = !sliced;
  if (!sliced) {
    el.sliceList.innerHTML = '';
    return;
  }
  const total = state.slices.reduce((n, s) => n + s.size, 0);
  el.sliceChip.textContent = `분할 ${state.slices.length}장`;
  el.sliceSummary.textContent =
    `원본 ${formatBytes(state.fileSize)} · 1MB를 넘어 ${state.slices.length}장으로 잘랐습니다 (합계 ${formatBytes(total)})`;
  el.sliceList.innerHTML = renderSliceListHtml(state.slices);
}

function renderStatus() {
  el.areaChip.textContent = `영역 ${state.areas.length}개`;
  el.doneBtn.disabled = !state.dataUrl || state.areas.length === 0;
  el.testBtn.disabled = !state.generated;
}

function renderAll() {
  renderRegions();
  renderGuides();
  renderList();
  renderSlices();
  renderStatus();
}

function invalidate() {
  // 영역이 바뀌면 이전에 생성한 코드는 낡은 것이 된다.
  state.generated = false;
  el.codePanel.hidden = true;
  renderStatus();
}

function warn(message) {
  el.warn.textContent = message || '';
  el.warn.hidden = !message;
}

// ---------- 이미지 입력 ---------------------------------------------------- //

function readAsDataUrl(blob) {
  return new Promise((resolve) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result));
    reader.readAsDataURL(blob);
  });
}

function loadFile(file) {
  if (!file || !file.type.startsWith('image/')) {
    warn('이미지 파일만 열 수 있습니다.');
    return;
  }
  warn('');
  const reader = new FileReader();
  reader.onload = async () => {
    state.dataUrl = String(reader.result);
    state.fileName = file.name;
    state.fileSize = file.size;
    state.mimeType = file.type === 'image/jpeg' ? 'image/jpeg' : 'image/png';
    state.areas = [];
    state.slices = [];
    state.selectedId = null;

    el.preview.onload = async () => {
      state.naturalWidth = el.preview.naturalWidth;
      state.naturalHeight = el.preview.naturalHeight;
      el.empty.hidden = true;
      el.imageBox.hidden = false;
      el.fileChip.textContent =
        `${file.name} · ${state.naturalWidth}×${state.naturalHeight}px · ${formatBytes(file.size)}`;
      if (!el.imgSrc.value.trim() || el.imgSrc.dataset.auto === '1') {
        el.imgSrc.value = file.name;
        el.imgSrc.dataset.auto = '1';
      }
      invalidate();
      renderAll();

      if (needsSplit(file.size)) {
        el.sliceChip.hidden = false;
        el.sliceChip.textContent = '분할 중…';
        state.slices = await buildSlices();
        renderAll();
      }
    };
    el.preview.src = state.dataUrl;
  };
  reader.readAsDataURL(file);
}

// ---------- 1MB 초과 이미지 분할 ------------------------------------------- //

async function cutSlice(slice, index) {
  const canvas = document.createElement('canvas');
  canvas.width = state.naturalWidth;
  canvas.height = slice.height;
  canvas
    .getContext('2d')
    .drawImage(el.preview, 0, slice.y, canvas.width, slice.height, 0, 0, canvas.width, slice.height);

  const blob = await new Promise((r) => canvas.toBlob(r, state.mimeType, 0.92));
  return {
    ...slice,
    blob,
    size: blob ? blob.size : 0,
    dataUrl: blob ? await readAsDataUrl(blob) : '',
    name: sliceFileName(state.fileName || 'image.png', index),
  };
}

/**
 * 조각을 실제로 잘라낸다. 재인코딩 결과가 여전히 1MB를 넘으면 조각 수를 늘려
 * 다시 자른다(최대 3회).
 */
async function buildSlices() {
  let count = Math.ceil(state.fileSize / MAX_SLICE_BYTES);

  for (let attempt = 0; attempt < 3; attempt++) {
    const plan = planSlices({ height: state.naturalHeight, byteSize: state.fileSize, count });
    const built = [];
    for (const slice of plan) built.push(await cutSlice(slice, built.length));

    const biggest = built.reduce((m, s) => Math.max(m, s.size), 0);
    if (biggest <= MAX_SLICE_BYTES || plan.length >= MAX_SLICES) return built;

    const next = Math.ceil(plan.length * (biggest / MAX_SLICE_BYTES)) + 1;
    if (next <= plan.length) return built;
    count = Math.min(next, MAX_SLICES);
  }
  const plan = planSlices({ height: state.naturalHeight, count });
  const built = [];
  for (const slice of plan) built.push(await cutSlice(slice, built.length));
  return built;
}

function downloadBlob(blob, name) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = name;
  a.click();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

function downloadSlice(index) {
  const slice = state.slices[index];
  if (slice && slice.blob) downloadBlob(slice.blob, slice.name);
}

async function downloadAllSlices() {
  for (let i = 0; i < state.slices.length; i++) {
    downloadSlice(i);
    await new Promise((r) => setTimeout(r, 350)); // 연속 다운로드 차단 회피
  }
}

el.fileInput.addEventListener('change', () => {
  if (el.fileInput.files && el.fileInput.files[0]) loadFile(el.fileInput.files[0]);
});

el.imgSrc.addEventListener('input', () => { el.imgSrc.dataset.auto = '0'; });

['dragenter', 'dragover'].forEach((type) => {
  el.wrap.addEventListener(type, (e) => {
    e.preventDefault();
    el.wrap.classList.add('dragover');
  });
});
['dragleave', 'drop'].forEach((type) => {
  el.wrap.addEventListener(type, (e) => {
    e.preventDefault();
    el.wrap.classList.remove('dragover');
  });
});
el.wrap.addEventListener('drop', (e) => {
  const file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
  if (file) loadFile(file);
});

document.addEventListener('paste', (e) => {
  const items = e.clipboardData ? e.clipboardData.files : null;
  if (items && items[0]) loadFile(items[0]);
});

el.sliceList.addEventListener('click', (e) => {
  const btn = e.target.closest('[data-slice]');
  if (btn) downloadSlice(Number(btn.dataset.slice));
});
el.downloadSlicesBtn.addEventListener('click', downloadAllSlices);

// ---------- 영역 드래그(생성/이동) ---------------------------------------- //

el.overlay.addEventListener('pointerdown', (e) => {
  if (!state.dataUrl || e.button !== 0) return;
  const p = pointerPos(e);
  const s = scale();
  const natural = { x: p.x / s, y: p.y / s };
  const hit = hitTest(state.areas, natural);

  el.overlay.setPointerCapture(e.pointerId);

  if (hit) {
    state.selectedId = hit.id;
    drag = {
      mode: 'move',
      id: hit.id,
      offsetX: natural.x - hit.x,
      offsetY: natural.y - hit.y,
      moved: false,
    };
    renderRegions();
    renderList();
  } else {
    state.selectedId = null;
    drag = { mode: 'draw', startX: p.x, startY: p.y };
    renderRegions();
    renderList();
    draftEl = document.createElement('div');
    draftEl.className = 'region draft';
    el.overlay.appendChild(draftEl);
  }
  e.preventDefault();
});

el.overlay.addEventListener('pointermove', (e) => {
  if (!drag) return;
  const p = pointerPos(e);
  const s = scale();

  if (drag.mode === 'draw' && draftEl) {
    const r = normalizeRect(drag.startX, drag.startY, p.x, p.y);
    draftEl.style.left = `${r.x}px`;
    draftEl.style.top = `${r.y}px`;
    draftEl.style.width = `${r.w}px`;
    draftEl.style.height = `${r.h}px`;
    return;
  }

  if (drag.mode === 'move') {
    const target = state.areas.find((a) => a.id === drag.id);
    if (!target) return;
    // 크기는 그대로 두고 위치만 이미지 경계 안으로 민다.
    const x = Math.min(Math.max(0, p.x / s - drag.offsetX), Math.max(0, state.naturalWidth - target.w));
    const y = Math.min(Math.max(0, p.y / s - drag.offsetY), Math.max(0, state.naturalHeight - target.h));
    state.areas = updateArea(state.areas, drag.id, { x: Math.round(x), y: Math.round(y) });
    drag.moved = true;
    renderRegions();
  }
});

function endDrag(e) {
  if (!drag) return;
  const finished = drag;
  drag = null;
  if (el.overlay.hasPointerCapture(e.pointerId)) el.overlay.releasePointerCapture(e.pointerId);

  if (finished.mode === 'draw') {
    const p = pointerPos(e);
    const display = normalizeRect(finished.startX, finished.startY, p.x, p.y);
    if (draftEl) draftEl.remove();
    draftEl = null;
    if (!isValidRect(display, MIN_DRAG_PX)) {
      renderAll();
      return;
    }
    const rect = clampRect(toNaturalRect(display, scale()), naturalBounds());
    const area = createArea(rect);
    state.areas = addArea(state.areas, area);
    state.selectedId = area.id;
    invalidate();
    renderAll();
    const input = el.areaList.querySelector(`[data-id="${CSS.escape(area.id)}"] .area-name`);
    if (input) input.focus({ preventScroll: true }); // 포커스로 화면이 튀지 않게
    return;
  }

  if (finished.mode === 'move' && finished.moved) invalidate();
  renderAll();
}

el.overlay.addEventListener('pointerup', endDrag);
el.overlay.addEventListener('pointercancel', endDrag);

// ---------- 영역 목록 편집 ------------------------------------------------- //

el.areaList.addEventListener('input', (e) => {
  const row = e.target.closest('.area-row');
  const field = e.target.dataset.field;
  if (!row || !field) return;
  state.areas = updateArea(state.areas, row.dataset.id, { [field]: e.target.value });
  invalidate();
  renderRegions();
});

el.areaList.addEventListener('click', (e) => {
  const row = e.target.closest('.area-row');
  if (!row) return;
  if (e.target.dataset.action === 'delete') {
    state.areas = removeArea(state.areas, row.dataset.id);
    if (state.selectedId === row.dataset.id) state.selectedId = null;
    invalidate();
    renderAll();
    return;
  }
  state.selectedId = row.dataset.id;
  renderRegions();
  renderList();
});

document.addEventListener('keydown', (e) => {
  const typing = ['INPUT', 'TEXTAREA'].includes(e.target.tagName);
  if (e.key === 'Escape') {
    if (!el.modal.hidden) closeModal();
    else if (state.selectedId) { state.selectedId = null; renderRegions(); renderList(); }
    return;
  }
  if (typing || !state.selectedId) return;
  if (e.key === 'Delete' || e.key === 'Backspace') {
    e.preventDefault();
    state.areas = removeArea(state.areas, state.selectedId);
    state.selectedId = null;
    invalidate();
    renderAll();
  }
});

// ---------- 완료 / 테스트 -------------------------------------------------- //

function baseOptions() {
  return {
    mapName: el.mapName.value,
    areas: state.areas,
    alt: state.fileName,
    width: state.naturalWidth,
    height: state.naturalHeight,
  };
}

function generate() {
  if (!state.dataUrl || state.areas.length === 0) {
    warn('이미지를 올리고 클릭 영역을 최소 1개 이상 그려 주세요.');
    return;
  }
  warn('');
  const src = el.imgSrc.value.trim() || state.fileName || 'image.png';

  el.codeOutput.value = isSliced()
    ? renderSlicedImageMapHtml({
        ...baseOptions(),
        slices: state.slices,
        srcFor: (slice, i) => sliceFileName(src, i),
      })
    : renderImageMapHtml({ ...baseOptions(), src });

  el.codePanel.hidden = false;
  state.generated = true;
  renderStatus();
  el.codePanel.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function previewDocument() {
  const common = { ...baseOptions(), title: el.docTitle.value.trim() || '이미지 맵 테스트' };
  // 테스트 페이지에는 이미지를 그대로 담아 단독으로 열리게 한다.
  return isSliced()
    ? renderPreviewDocument({ ...common, slices: state.slices, srcFor: (s, i) => state.slices[i].dataUrl })
    : renderPreviewDocument({ ...common, src: state.dataUrl });
}

function openTestPage() {
  if (!state.generated) return;
  const doc = previewDocument();
  const url = URL.createObjectURL(new Blob([doc], { type: 'text/html' }));
  // 'noopener' 를 주면 성공해도 null 이 돌아와 팝업 차단과 구분되지 않는다.
  const win = window.open(url, '_blank');
  if (win) {
    setTimeout(() => URL.revokeObjectURL(url), 60000);
    return;
  }
  // 팝업 차단 시: 페이지 안에서 그대로 확인
  URL.revokeObjectURL(url);
  el.frame.srcdoc = doc;
  el.modal.hidden = false;
  el.modalNote.hidden = false;
}

function closeModal() {
  el.modal.hidden = true;
  el.frame.srcdoc = '';
}

function clearAll() {
  state.areas = [];
  state.selectedId = null;
  invalidate();
  renderAll();
}

el.doneBtn.addEventListener('click', generate);
el.testBtn.addEventListener('click', openTestPage);
el.clearBtn.addEventListener('click', clearAll);
el.modalClose.addEventListener('click', closeModal);
el.modal.addEventListener('click', (e) => { if (e.target === el.modal) closeModal(); });

el.copyBtn.addEventListener('click', async () => {
  try {
    await navigator.clipboard.writeText(el.codeOutput.value);
    el.copyBtn.textContent = '복사됨 ✓';
  } catch {
    el.codeOutput.select();
    document.execCommand('copy');
    el.copyBtn.textContent = '복사됨 ✓';
  }
  setTimeout(() => { el.copyBtn.textContent = '코드 복사'; }, 1500);
});

el.downloadBtn.addEventListener('click', () => {
  downloadBlob(new Blob([previewDocument()], { type: 'text/html' }), 'image-map.html');
});

['input', 'change'].forEach((type) => {
  el.mapName.addEventListener(type, invalidate);
  el.imgSrc.addEventListener(type, invalidate);
});

window.addEventListener('resize', () => { renderRegions(); renderGuides(); });

// ---------- 초기화 -------------------------------------------------------- //

el.imgSrc.dataset.auto = '1';
renderAll();
