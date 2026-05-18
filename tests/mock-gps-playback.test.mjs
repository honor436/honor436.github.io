import test from "node:test";
import assert from "node:assert/strict";
import {
  haversineM,
  interpolatePath,
  reinterpolateFromCurrent,
  indexFromProgress,
} from "../DltLogViewer/js/mock-gps-playback.js";

// ---- haversineM ---- //

test("haversineM identical points returns 0", () => {
  const a = { lat: 37.5, lng: 127.0 };
  assert.equal(haversineM(a, a), 0);
});

test("haversineM ~1km between known points", () => {
  // 1도 위도 ≈ 111km, 0.009도 ≈ 1km
  const a = { lat: 37.5, lng: 127.0 };
  const b = { lat: 37.509, lng: 127.0 };
  const d = haversineM(a, b);
  assert.ok(d > 990 && d < 1010, `expected ~1000m, got ${d}`);
});

// ---- interpolatePath ---- //

test("interpolatePath empty/single returns no interpolation", () => {
  assert.deepEqual(interpolatePath([], 40), []);
  const one = [{ lat: 37.5, lng: 127.0 }];
  const r = interpolatePath(one, 40);
  assert.equal(r.length, 1);
  assert.equal(r[0].lat, 37.5);
});

test("interpolatePath produces ~11m steps for 40km/h", () => {
  // 40km/h * 1s = 11.111m
  const pts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.509, lng: 127.0 }, // ~1km away
  ];
  const coords = interpolatePath(pts, 40);
  // 1000m / 11.11m ≈ 90 steps + 1 endpoint = 91 coords
  assert.ok(coords.length >= 88 && coords.length <= 95,
    `expected ~91 coords, got ${coords.length}`);
});

test("interpolatePath tags each coord with srcNextIdx pointing into original points", () => {
  const pts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.0 },
    { lat: 37.52, lng: 127.0 },
  ];
  const coords = interpolatePath(pts, 40);
  // first coord on segment 0->1: srcNextIdx = 1
  assert.equal(coords[0].srcNextIdx, 1);
  // last coord is the endpoint, srcNextIdx = pts.length
  assert.equal(coords[coords.length - 1].srcNextIdx, pts.length);
  // mid coords should have srcNextIdx in {1,2,3}
  for (const c of coords) {
    assert.ok(c.srcNextIdx >= 1 && c.srcNextIdx <= pts.length);
  }
});

test("interpolatePath first coord equals first point", () => {
  const pts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.01 },
  ];
  const coords = interpolatePath(pts, 40);
  assert.equal(coords[0].lat, 37.5);
  assert.equal(coords[0].lng, 127.0);
});

test("interpolatePath last coord equals last point", () => {
  const pts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.01 },
  ];
  const coords = interpolatePath(pts, 40);
  const last = coords[coords.length - 1];
  assert.equal(last.lat, 37.51);
  assert.equal(last.lng, 127.01);
});

// ---- reinterpolateFromCurrent ---- //

test("reinterpolateFromCurrent starts at currentPoint", () => {
  const drawPts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.0 },
    { lat: 37.52, lng: 127.0 },
  ];
  const cur = { lat: 37.505, lng: 127.0 };
  const r = reinterpolateFromCurrent(cur, drawPts, 1, 40);
  assert.equal(r[0].lat, 37.505);
  assert.equal(r[0].lng, 127.0);
});

test("reinterpolateFromCurrent ends at last drawPoint", () => {
  const drawPts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.0 },
    { lat: 37.52, lng: 127.0 },
  ];
  const cur = { lat: 37.505, lng: 127.0 };
  const r = reinterpolateFromCurrent(cur, drawPts, 1, 40);
  const last = r[r.length - 1];
  assert.equal(last.lat, 37.52);
});

test("reinterpolateFromCurrent srcNextIdx maps back to original drawPoints", () => {
  const drawPts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.0 },
    { lat: 37.52, lng: 127.0 },
    { lat: 37.53, lng: 127.0 },
  ];
  // Resuming from a point partway between drawPts[1] and drawPts[2]
  const cur = { lat: 37.515, lng: 127.0 };
  const r = reinterpolateFromCurrent(cur, drawPts, 2, 40);
  // first interpolated coord targets drawPts[2] => srcNextIdx = 2
  assert.equal(r[0].srcNextIdx, 2);
  // last coord is endpoint = drawPts[3], srcNextIdx = drawPts.length
  assert.equal(r[r.length - 1].srcNextIdx, drawPts.length);
});

test("reinterpolateFromCurrent at end returns just current point", () => {
  const drawPts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.51, lng: 127.0 },
  ];
  const cur = { lat: 37.51, lng: 127.0 };
  const r = reinterpolateFromCurrent(cur, drawPts, 2, 40);
  assert.equal(r.length, 1);
  assert.equal(r[0].lat, 37.51);
});

test("reinterpolateFromCurrent faster speed yields fewer coords", () => {
  const drawPts = [
    { lat: 37.5, lng: 127.0 },
    { lat: 37.55, lng: 127.0 }, // ~5.5km
  ];
  const cur = { lat: 37.5, lng: 127.0 };
  const slow = reinterpolateFromCurrent(cur, drawPts, 1, 20);   // 20 km/h
  const fast = reinterpolateFromCurrent(cur, drawPts, 1, 100);  // 100 km/h
  assert.ok(fast.length < slow.length,
    `fast(${fast.length}) should be < slow(${slow.length})`);
});

// ---- indexFromProgress ---- //

test("indexFromProgress 0% returns 0", () => {
  assert.equal(indexFromProgress(100, 0), 0);
});

test("indexFromProgress 100% returns last index", () => {
  assert.equal(indexFromProgress(100, 1), 99);
});

test("indexFromProgress 50% returns middle index", () => {
  assert.equal(indexFromProgress(100, 0.5), 50);
});

test("indexFromProgress clamps negative to 0", () => {
  assert.equal(indexFromProgress(100, -0.5), 0);
});

test("indexFromProgress clamps >1 to last", () => {
  assert.equal(indexFromProgress(100, 2), 99);
});

test("indexFromProgress empty returns 0", () => {
  assert.equal(indexFromProgress(0, 0.5), 0);
});
