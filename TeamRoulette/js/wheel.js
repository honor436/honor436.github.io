// 룰렛 휠 캔버스 렌더링
import { segmentAngle, segmentColor, shouldFlipLabel } from './roulette.js';

const DEG = Math.PI / 180;

/**
 * 이름 목록으로 룰렛 원판을 그린다.
 * 각도 규약: 12시가 0도, 시계방향 증가. roulette.js 의 indexAtPointer 와 동일.
 */
export function drawWheel(canvas, names) {
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const size = canvas.clientWidth || 520;
  if (canvas.width !== size * dpr) {
    canvas.width = size * dpr;
    canvas.height = size * dpr;
  }
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, size, size);

  const cx = size / 2;
  const cy = size / 2;
  const r = size / 2 - 6;
  const count = names.length;

  if (count === 0) {
    ctx.beginPath();
    ctx.arc(cx, cy, r, 0, Math.PI * 2);
    ctx.fillStyle = '#1b2130';
    ctx.fill();
    ctx.strokeStyle = '#39415a';
    ctx.lineWidth = 2;
    ctx.stroke();
    return;
  }

  const seg = segmentAngle(count);

  for (let i = 0; i < count; i++) {
    const start = (-90 + i * seg) * DEG;
    const end = (-90 + (i + 1) * seg) * DEG;

    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, start, end);
    ctx.closePath();
    ctx.fillStyle = segmentColor(i, count);
    ctx.fill();
    if (count <= 40) {
      ctx.strokeStyle = 'rgba(10,13,20,.45)';
      ctx.lineWidth = 1;
      ctx.stroke();
    }

    // 이름: 세그먼트 중앙에서 바깥쪽을 향해
    const arc = (seg * DEG) * r;
    const fontSize = Math.max(10, Math.min(22, arc * 0.55));
    const flip = shouldFlipLabel(i, count);
    ctx.save();
    ctx.translate(cx, cy);
    ctx.rotate((-90 + (i + 0.5) * seg + (flip ? 180 : 0)) * DEG);
    ctx.textAlign = flip ? 'left' : 'right';
    ctx.textBaseline = 'middle';
    ctx.font = `700 ${fontSize}px system-ui, -apple-system, "Apple SD Gothic Neo", sans-serif`;
    ctx.fillStyle = '#10131c';
    ctx.fillText(names[i], flip ? -(r - 14) : r - 14, 0, r * 0.62);
    ctx.restore();
  }

  // 테두리 + 중앙 허브
  ctx.beginPath();
  ctx.arc(cx, cy, r, 0, Math.PI * 2);
  ctx.strokeStyle = 'rgba(255,255,255,.28)';
  ctx.lineWidth = 4;
  ctx.stroke();

  ctx.beginPath();
  ctx.arc(cx, cy, r * 0.17, 0, Math.PI * 2);
  ctx.fillStyle = '#0f1320';
  ctx.fill();
  ctx.strokeStyle = 'rgba(255,255,255,.22)';
  ctx.lineWidth = 2;
  ctx.stroke();
}
