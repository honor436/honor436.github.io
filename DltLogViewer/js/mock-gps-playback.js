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
export function interpolatePath(points, speedKmh) {
  if (!Array.isArray(points) || points.length === 0) return [];
  if (points.length === 1) {
    return [{ lat: points[0].lat, lng: points[0].lng, srcNextIdx: 1 }];
  }
  const stepM = Math.max(1, speedKmh * 1000 / 3600);
  const coords = [];
  for (let i = 0; i < points.length - 1; i++) {
    const a = points[i], b = points[i + 1];
    const dist = haversineM(a, b);
    const steps = Math.max(1, Math.round(dist / stepM));
    for (let s = 0; s < steps; s++) {
      const t = s / steps;
      coords.push({
        lat: a.lat + (b.lat - a.lat) * t,
        lng: a.lng + (b.lng - a.lng) * t,
        srcNextIdx: i + 1,
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
