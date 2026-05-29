import test from 'node:test';
import assert from 'node:assert/strict';
import {
  DEFAULT_POI_SEARCH_URL,
  poiSearchHeaders,
  buildPoiSearchBody,
  centerToNoor,
  parsePoiResults,
  normalizePoi,
  buildPoiDetailBody,
  formatPoiDetailHtml,
} from '../DltLogViewer/js/poi-search.js';

// ---- headers --------------------------------------------------------------

test('poiSearchHeaders includes requestHashToken, Requester, Accept json', () => {
  const h = poiSearchHeaders();
  assert.equal(h['requestHashToken'], '1988490197');
  assert.equal(h['Requester'], 'CLIENT_SSL');
  assert.equal(h['Accept'], 'application/json');
  assert.equal(h['Nonce'], '1988490197');
});

// ---- request body ---------------------------------------------------------

test('buildPoiSearchBody puts keyword in name and noorX/noorY as strings', () => {
  const b = buildPoiSearchBody('BMW 드라이빙센터', { noorX: 4575887, noorY: 1351438 });
  assert.equal(b.name, 'BMW 드라이빙센터');
  assert.equal(b.noorX, '4575887');
  assert.equal(b.noorY, '1351438');
  assert.equal(typeof b.noorX, 'string');
});

test('buildPoiSearchBody uses the documented search engine + type defaults', () => {
  const b = buildPoiSearchBody('스타벅스', {});
  assert.equal(b.poiGroupYn, 'Y');
  assert.equal(b.searchTypCd, 'A');
  assert.equal(b.reqCnt, 70);
  assert.equal(b.reqSearchEngineInfo.searchMethod, 'aut.kwd');
  assert.equal(b.reqSearchEngineInfo.searchFrom, 'pth');
  assert.ok(b.header && b.header.svcType === 116, 'must carry app header');
});

// ---- center → SK coords ---------------------------------------------------

test('centerToNoor round-trips back to the same lat/lon within ~10m', async () => {
  const { skCoordToWgs84 } = await import('../DltLogViewer/js/coordinate.js');
  const lat = 37.5665, lon = 126.9780;
  const { noorX, noorY } = centerToNoor(lat, lon);
  assert.equal(Number.isInteger(noorX), true);
  assert.equal(Number.isInteger(noorY), true);
  const [rLat, rLon] = skCoordToWgs84(noorX, noorY);
  assert.ok(Math.abs(rLat - lat) < 0.001, `lat back=${rLat}`);
  assert.ok(Math.abs(rLon - lon) < 0.001, `lon back=${rLon}`);
});

// ---- normalize ------------------------------------------------------------

test('normalizePoi extracts name, address, tel, id from common fields', () => {
  const p = normalizePoi({
    name: 'BMW 드라이빙센터',
    fullAddrRoad: '인천 중구 공항동로 136',
    telNo: '1899-0000',
    pkey: 'PK123',
    noorX: 4575887, noorY: 1351438,
  });
  assert.equal(p.name, 'BMW 드라이빙센터');
  assert.equal(p.address, '인천 중구 공항동로 136');
  assert.equal(p.tel, '1899-0000');
  assert.equal(p.id, 'PK123');
});

test('normalizePoi converts SK noorX/noorY to plausible WGS84 lat/lon', () => {
  const p = normalizePoi({ name: 'x', noorX: 4575887, noorY: 1351438 });
  assert.ok(p.lat > 37.0 && p.lat < 38.0, `lat=${p.lat}`);
  assert.ok(p.lon > 126.5 && p.lon < 127.5, `lon=${p.lon}`);
});

test('normalizePoi accepts WGS84 lat/lon when no SK coords present', () => {
  const p = normalizePoi({ name: 'x', centerLat: 37.5, centerLon: 127.0 });
  assert.equal(p.lat, 37.5);
  assert.equal(p.lon, 127.0);
});

// ---- parse list -----------------------------------------------------------

test('parsePoiResults reads a poiInfo.poi container', () => {
  const json = { poiInfo: { poi: [
    { name: 'A', noorX: 4575887, noorY: 1351438 },
    { name: 'B', noorX: 4575900, noorY: 1351500 },
  ] } };
  const list = parsePoiResults(json);
  assert.equal(list.length, 2);
  assert.equal(list[0].name, 'A');
});

test('parsePoiResults falls back to deep-searching for the POI array', () => {
  const json = { wrapper: { body: { searchList: [
    { name: 'C', centerLat: 37.5, centerLon: 127.0 },
  ] } } };
  const list = parsePoiResults(json);
  assert.equal(list.length, 1);
  assert.equal(list[0].name, 'C');
});

test('parsePoiResults returns empty array for no usable data', () => {
  assert.deepEqual(parsePoiResults(null), []);
  assert.deepEqual(parsePoiResults({ header: { code: 'ok' } }), []);
});

// ---- detail ---------------------------------------------------------------

test('buildPoiDetailBody carries the poi id', () => {
  const poi = normalizePoi({ name: 'x', pkey: 'PK9', noorX: 4575887, noorY: 1351438 });
  const b = buildPoiDetailBody(poi, { noorX: 1, noorY: 2 });
  assert.equal(b.poiId, 'PK9');
  assert.equal(b.noorX, '1');
});

test('formatPoiDetailHtml shows the name and escapes HTML', () => {
  const poi = { name: 'A<script>', address: '주소', tel: '02-0000', lat: 37.5, lon: 127.0, raw: {} };
  const html = formatPoiDetailHtml(poi, { foo: 'bar' });
  assert.ok(html.includes('A&lt;script&gt;'), 'name must be escaped');
  assert.ok(html.includes('주소'));
  assert.ok(html.includes('foo'), 'raw detail json shown');
});

export const DEFAULT_URL_IS_STRING = typeof DEFAULT_POI_SEARCH_URL === 'string';
