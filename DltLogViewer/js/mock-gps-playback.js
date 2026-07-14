// Mock GPS 재생 보간 모듈 (순수 로직, 테스트 가능).
// - haversineM:   두 점(lat/lng) 사이 거리(m)
// - interpolatePath: 원본 궤적을 속도(km/h) 기준 1초 간격 좌표로 보간
// - reinterpolateFromCurrent: 현재 위치 + 남은 원본 좌표를 새 속도로 재보간
// - indexFromProgress: 진행률(0~1)을 좌표 인덱스로 변환

export function haversineM(a, b) {
  const R = 6371000;
  const dLat = (b.lat - a.lat) * Math.PI / 180;
  const dLng = (b.lng - a.lng) * Math.PI / 180;
  const s = Math.sin(dLat / 2) ** 2 +
            Math.cos(a.lat * Math.PI / 180) * Math.cos(b.lat * Math.PI / 180) *
            Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.asin(Math.sqrt(s));
}

// 보간된 좌표는 { lat, lng, srcNextIdx } 형태.
// srcNextIdx 는 원본 points 배열에서 "다음으로 향하고 있는" 인덱스.
// 마지막(엔드포인트) 좌표의 srcNextIdx 는 points.length (= 더 이상 다음 없음).
//
// 좌표는 세그먼트 단위가 아니라 전체 경로의 누적 거리 기준으로 stepM 간격마다 찍는다.
// (세그먼트별로 최소 1스텝을 강제하면, 촘촘한 점들이 몰려있는 구간에서 실제 이동 거리와
//  무관하게 세그먼트 수만큼 좌표가 그 자리에 추가로 생성되어 차량이 멈춘 것처럼 보인다.)
export function interpolatePath(points, speedKmh) {
  if (!Array.isArray(points) || points.length === 0) return [];
  if (points.length === 1) {
    return [{ lat: points[0].lat, lng: points[0].lng, srcNextIdx: 1 }];
  }
  const stepM = Math.max(1, speedKmh * 1000 / 3600);
  const segDist = [];
  let total = 0;
  for (let i = 0; i < points.length - 1; i++) {
    const d = haversineM(points[i], points[i + 1]);
    segDist.push(d);
    total += d;
  }
  const coords = [{ lat: points[0].lat, lng: points[0].lng, srcNextIdx: 1 }];
  if (total > 0) {
    const steps = Math.max(1, Math.round(total / stepM));
    const actualStep = total / steps;
    let segIdx = 0;
    let segStart = 0; // 현재 세그먼트 시작점까지의 누적 거리
    for (let k = 1; k < steps; k++) {
      const target = k * actualStep;
      while (segIdx < segDist.length - 1 && segStart + segDist[segIdx] < target) {
        segStart += segDist[segIdx];
        segIdx++;
      }
      const segLen = segDist[segIdx];
      const t = segLen > 0 ? (target - segStart) / segLen : 0;
      const a = points[segIdx], b = points[segIdx + 1];
      coords.push({
        lat: a.lat + (b.lat - a.lat) * t,
        lng: a.lng + (b.lng - a.lng) * t,
        srcNextIdx: segIdx + 1,
      });
    }
  }
  const last = points[points.length - 1];
  coords.push({ lat: last.lat, lng: last.lng, srcNextIdx: points.length });
  return coords;
}

// 현재 위치(currentPoint) + drawPoints[srcNextIdx..] 를 새 속도로 다시 보간.
// 반환 좌표의 srcNextIdx 는 원본 drawPoints 인덱스 체계로 매핑되어 다음 재보간에서도 안전.
export function reinterpolateFromCurrent(currentPoint, drawPoints, srcNextIdx, speedKmh) {
  if (!Array.isArray(drawPoints) || srcNextIdx >= drawPoints.length) {
    return [{ lat: currentPoint.lat, lng: currentPoint.lng, srcNextIdx: (drawPoints || []).length }];
  }
  const remaining = [
    { lat: currentPoint.lat, lng: currentPoint.lng },
    ...drawPoints.slice(srcNextIdx).map(p => ({
      lat: p.lat,
      lng: p.lng != null ? p.lng : p.lon,
    })),
  ];
  const localCoords = interpolatePath(remaining, speedKmh);
  // local srcNextIdx=1 → drawPoints[srcNextIdx], local k → drawPoints[srcNextIdx + k - 1]
  return localCoords.map(c => ({
    lat: c.lat,
    lng: c.lng,
    srcNextIdx: srcNextIdx + Math.max(0, c.srcNextIdx - 1),
  }));
}

export function indexFromProgress(total, pct) {
  if (!Number.isFinite(total) || total <= 0) return 0;
  const clamped = Math.max(0, Math.min(1, pct));
  const idx = Math.round(clamped * total);
  return Math.max(0, Math.min(total - 1, idx));
}
