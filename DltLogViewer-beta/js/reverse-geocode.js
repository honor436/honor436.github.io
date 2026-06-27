/**
 * TMAP ReverseGeocoding (좌표 -> 주소) 헬퍼.
 *
 * Endpoint: https://topopen.tmap.co.kr/tmapv20/geo/reversegeocoding
 *   예: ?lat=37.565125&lon=126.987642&coordType=WGS84GEO&addressType=A10&keyInfo=Y
 *
 * 같은 top-open 호스트의 traffic API 와 동일하게 appKey 없이 CORS 로 호출한다.
 * 응답의 addressInfo 에서 표시용 주소와 buildingName(경로 요청의 departName/
 * destName 용) 을 추출한다.
 */

'use strict';

const RG_ENDPOINT = 'https://topopen.tmap.co.kr/tmapv20/geo/reversegeocoding';

/**
 * 좌표(WGS84)로 역지오코딩 요청 URL 생성.
 * @param {number} lat
 * @param {number} lon
 * @returns {string}
 */
export function buildReverseGeocodeUrl(lat, lon) {
  const p = new URLSearchParams({
    lat: String(lat),
    lon: String(lon),
    coordType: 'WGS84GEO',
    addressType: 'A10',
    keyInfo: 'Y',
  });
  return `${RG_ENDPOINT}?${p.toString()}`;
}

// null 패딩 제거 + 연속 공백을 단일 공백으로 정규화 + 양끝 trim
function clean(s) {
  if (s == null) return '';
  return String(s).replace(/\x00+/g, '').replace(/\s+/g, ' ').trim();
}

/**
 * 역지오코딩 응답 파싱.
 * @param {any} json TMAP 응답 ({ addressInfo: {...} })
 * @returns {{buildingName:string, address:string}|null}
 *   buildingName: 건물명 (없으면 '')
 *   address: 표시용 주소 (fullAddress 우선, 없으면 구성요소 조합)
 */
export function parseReverseGeocode(json) {
  const a = json && json.addressInfo;
  if (!a || typeof a !== 'object') return null;

  const buildingName = clean(a.buildingName);

  let address = clean(a.fullAddress);
  if (!address) {
    const road = clean(a.roadName);
    if (road) {
      // 도로명 주소
      address = [clean(a.city_do), clean(a.gu_gun), clean(a.eup_myun), road, clean(a.buildingIndex)]
        .filter(Boolean).join(' ');
    } else {
      // 지번 주소
      const dong = clean(a.legalDong) || clean(a.adminDong);
      address = [clean(a.city_do), clean(a.gu_gun), clean(a.eup_myun), dong, clean(a.ri), clean(a.bunji)]
        .filter(Boolean).join(' ');
    }
  }

  return { buildingName, address };
}
