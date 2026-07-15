/**
 * 경로 탐색(RSD) 요청 URL 빌더.
 * 서버(상용/준상용/개발) × 경로 타입(전기차/일반) 조합으로 엔드포인트 URL 생성.
 */

'use strict';

export const ROUTE_ENVS = {
  prod: { label: '상용',   host: 'ntmap.tmap.co.kr' },
  stg:  { label: '준상용', host: 'ntmapstg.tmap.co.kr' },
  dev:  { label: '개발',   host: 'ntmapdev.tmap.co.kr' },
};

const PORT = 9443;

/**
 * 경로 타입별 엔드포인트 URL.
 * @param {string} env  'prod' | 'stg' | 'dev'
 * @param {string|boolean} routeType  'ev' | 'normal' | 'isochrone'
 *   (구버전 호환: true=ev, false=normal)
 */
export function buildRouteUrl(env, routeType) {
  const e = ROUTE_ENVS[env];
  if (!e) throw new Error('unknown route env: ' + env);
  if (routeType === true) routeType = 'ev';
  else if (routeType === false) routeType = 'normal';
  const path = routeType === 'isochrone' ? '/tmap-channel/rsd/route/isochrone'
             : routeType === 'ev'        ? '/tmap-channel/rsd/ev/route'
             :                             '/tmap-channel/rsd/route/planningroutemultiformat';
  return `https://${e.host}:${PORT}${path}`;
}

/**
 * 현재 도달거리(currentRange) = 80% 도달거리 × (현재 잔량 / 80% 전력).
 * chargedEnergy 가 0 이하이면 0.
 */
export function computeCurrentRange(chargedRange, chargedEnergy, currentEnergy) {
  const cr = Number(chargedRange), ce = Number(chargedEnergy), cur = Number(currentEnergy);
  if (!(ce > 0) || !Number.isFinite(cr) || !Number.isFinite(cur)) return 0;
  return Math.round(cr * cur / ce);
}

/**
 * 도달 가능 범위(isochrone) 응답에서 GeoJSON Polygon 링 배열 추출.
 * isochrone.geometry.coordinates = int[][][] (Polygon: 링들의 배열, 각 링 = [x,y] 좌표쌍 배열).
 * 좌표는 SK 정규화(8자리). 반환: [[ [x,y], ... ], ...]
 */
export function extractIsochroneRings(json) {
  const coords = json && json.isochrone && json.isochrone.geometry && json.isochrone.geometry.coordinates;
  if (!Array.isArray(coords)) return [];
  return coords.filter(ring => Array.isArray(ring) && ring.length > 0);
}

// EV 경로 → 도달 가능 범위로 그대로 가져올 차량/소비/전력 관련 필드.
export const EV_ISOCHRONE_SHARED_FIELDS = ['consumptionParam', 'slopeFlag', 'vehicleId', 'vehicleMass', 'vendor', 'auxiliaryPower', 'efficientSpeed'];
// EV 배터리 4필드(EV·도달가능범위 공통 단일 데이터로 그대로 보존).
export const EV_BATTERY_FIELDS = ['chargedEnergy', 'chargedRange', 'currentEnergy', 'currentRange'];

/**
 * EV 경로 파라미터를 도달 가능 범위(isochrone) 요청 바디에 그대로 반영(공통 데이터).
 * EV 현재 잔량(currentEnergy) → contoursEnergy, 현재 도달거리(currentRange) → contoursMeters.
 * 또한 차량/소비/전력(EV_ISOCHRONE_SHARED_FIELDS)과 배터리 4필드(EV_BATTERY_FIELDS)를
 * 그대로 가져와 EV·도달가능범위가 동일한 EV 파라미터를 쓰도록 한다(값이 있을 때만).
 */
export function applyEvBatteryToIsochrone(isoBody, evBody) {
  const out = { ...(isoBody || {}) };
  const ce = Number(evBody && evBody.currentEnergy);
  const cm = Number(evBody && evBody.currentRange);
  if (Number.isFinite(ce)) out.contoursEnergy = ce;
  if (Number.isFinite(cm)) out.contoursMeters = cm;
  for (const k of [...EV_ISOCHRONE_SHARED_FIELDS, ...EV_BATTERY_FIELDS]) {
    if (evBody && evBody[k] != null) out[k] = evBody[k];
  }
  return out;
}

/**
 * "EV 경로 데이터 가져오기": EV 경로 바디의 현재 배터리(currentEnergy/
 * currentRange)를 isochrone 바디에 적용한 결과를 반환한다.
 * 저장된 EV 바디(evBody)가 없으면 fallbackEvBody 를 사용한다.
 */
export function buildIsoBodyFromEvBattery(isoBody, evBody, fallbackEvBody) {
  return applyEvBatteryToIsochrone(isoBody, evBody || fallbackEvBody);
}

// ---- 도달 가능 거리 조회(isochrone) 요청 바디 ----------------------------- //
//
// 제공된 샘플(BMW RV11) 기준. consumptionParam 은 차량 소비 모델 JSON 을
// 문자열로 직렬화해 담는다(서버 규약). header.reqTime 은 전송 시 채우도록 비워둔다.

// isochrone 요청 헤더(샘플 기준 — svcType 113, BMW 에뮬레이터).
function isochroneHeader() {
  return {
    appLaunchCount: 1,
    appVersion: '3.20.403',
    buildNo: '320403',
    carrier: 'SKT',
    deviceId: '0-1-c42f91826d949fef2f2b5006e2f8404127e13189656dcec41311b1c4dce78430',
    modelNo: 'BMW Flavoured AOSP on arm64 Emulator',
    osType: 'AND',
    osVersion: '14',
    pushDeviceKey: '',
    reqTime: '',
    resolution: 'QXGA',
    screenHeight: 1660,
    screenWidth: 1752,
    svcType: 113,
    using: 'MAIN',
  };
}

// 차량 소비 모델 파라미터(consumptionParam 의 원본 객체).
function isochroneConsumptionParam() {
  return {
    aux: 1200.0,
    batteryCapacity: 80000,
    batteryTemperature: 0,
    chargingModeGen6: 'PERFORMANCE',
    csc: [20.46, 12.3, 12.2, 11.7, 13.5, 15.2, 18.2, 20.2, 23.0, 27.0, 31.9],
    firstCharging: false,
    kAcc: 2412000,
    kDec: 3600000,
    kDown: 3384000,
    kUp: 3132000,
    mass: 2000,
    vehicleId: 'RV11',
    vendor: 'BMW',
  };
}

/**
 * 경로 타입(ev/normal/isochrone) 전환 시 표시할 요청 바디를 계산한다.
 *
 * ev / normal / isochrone 은 서로 다른 요청 스키마다(특히 일반은 EV 와 포맷이 다름).
 * 사용자가 설정한 값이 타입 이동 후에도 유지되도록:
 * - 같은 타입이면 현재 바디를 그대로 둔다(body=null = 변경 없음).
 * - 타입이 바뀌면 떠나는 타입의 현재 바디를 saved[prevType] 에 저장하고,
 *   들어가는 타입의 저장본이 있으면 복원한다(사용자 설정 보존).
 * - 저장본이 없으면 기본 템플릿을 쓴다(EV·일반은 서로 독립된 별도 템플릿):
 *   · ev       → defaultBody
 *   · normal   → normalBody (EV 와 무관한 일반 전용 독립 템플릿)
 *   · isochrone→ 항상 EV 경로 파라미터를 그대로 반영(EV·도달범위 단일 데이터)
 *
 * @param {{ prevType:string, nextType:string, currentBody:object,
 *   saved:{ev:?object, normal:?object, isochrone:?object},
 *   defaultBody:object, normalBody:object, isochroneBody:object }} opts
 * @returns {{ body: object|null, saved: {ev:?object, normal:?object, isochrone:?object} }}
 *   body=null 이면 현재 바디 유지(교체하지 않음).
 */
export function resolveRouteTypeSwitch(opts) {
  const { prevType, nextType, currentBody, saved, defaultBody, normalBody, isochroneBody } = opts;
  const nextSaved = { ev: saved.ev, normal: saved.normal, isochrone: saved.isochrone };
  if (prevType === nextType) {
    return { body: null, saved: nextSaved }; // 같은 타입 → 변경 없음
  }
  // 떠나는 타입의 현재 바디 저장(사용자 설정 보존)
  nextSaved[prevType] = currentBody;
  if (nextType === 'isochrone') {
    // 도달 가능 범위는 항상 EV 경로 파라미터를 그대로 사용(EV·도달범위 단일 데이터).
    // EV 소스: 저장된 EV 바디 → 떠나는 게 EV 면 그 바디 → 기본.
    const evSource = nextSaved.ev || (prevType === 'ev' ? currentBody : defaultBody);
    return { body: applyEvBatteryToIsochrone(isochroneBody, evSource), saved: nextSaved };
  }
  if (nextType === 'normal') {
    // 일반은 EV 와 별개로 관리 → 저장된 일반 바디 복원, 없으면 일반 전용 기본 템플릿.
    return { body: nextSaved.normal || normalBody, saved: nextSaved };
  }
  // nextType === 'ev': 저장된 EV 바디 복원, 없으면 기본.
  return { body: nextSaved.ev || defaultBody, saved: nextSaved };
}

/**
 * 도달 가능 거리 조회(isochrone) 전송 헤더 보정.
 * 인증 헤더(AccessKey/AccessToken/CIH/Nonce/requestHashToken/Client_ReqTime/
 * Requester 등)는 그대로 유지하고, 응답이 JSON 이므로 Accept/Content-Type 만
 * application/json 으로 맞춘다.
 *
 * 과거 버그: 이 단계에서 인증 헤더를 전부 버리고 Accept/Content-Type 만 보내
 * 서버가 요청을 거부했다(지도화면 도달 가능 조회 에러). 인증 헤더를 유지한다.
 */
export function resolveIsochroneHeaders(headers) {
  return {
    ...(headers || {}),
    'Accept': 'application/json',
    'Content-Type': 'application/json; charset=UTF-8',
  };
}

/**
 * 도달 가능 거리 조회(isochrone) 요청 바디 생성.
 * consumptionParam 은 stringified JSON 으로 담긴다.
 */
export function buildIsochroneBody() {
  return {
    auxiliaryPower: 1200,
    consumptionParam: JSON.stringify(isochroneConsumptionParam()),
    contoursEnergy: 56000,
    contoursMeters: 300000,
    departXPos: 4575789,
    departYPos: 1345346,
    efficientSpeed: 0,
    slopeFlag: 1,
    vehicleId: 'RV11',
    vehicleMass: 2000,
    vendor: 'BMW',
    version: '1.1',
    header: isochroneHeader(),
  };
}
