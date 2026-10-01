import test from 'node:test';
import assert from 'node:assert/strict';
import { buildEvPopup } from '../DltLogViewer/js/tvas-renderer.js';

// ---- 필수 충전소 팝업 SoC ------------------------------------------------- //
//
// 충전소 도착 시 배터리량 + 예상 충전량 = 목표 충전량 으로 표시한다.

const text = (html) => html.replace(/<[^>]+>/g, '');

const mustEv = (arrivalSoc, expectedSoc) => ({
  name: '필수 충전소', mustCharge: 1, onRoute: 0, chargeSpeed: 2, availChargers: 1, totalChargers: 2,
  chargeTime: 1200, chargePower: 100, arrivalSoc, expectedSoc, poiId: 1, vxIdx: 10,
});

test('buildEvPopup_soc_shows_arrival_plus_charge_equals_target', () => {
  const html = buildEvPopup(mustEv(20, 60), 37.5, 127.1, 0);   // 도달 SoC 20, 예상 충전량 60
  assert.match(text(html), /도착 20% \+ 충전 60% = 목표 80%/);
});

test('buildEvPopup_soc_uses_given_plan', () => {
  const html = buildEvPopup(mustEv(15, 50), 37.5, 127.1, 1, { arrivalSoc: 15, chargeSoc: 50, targetSoc: 65 });
  assert.match(text(html), /도착 15% \+ 충전 50% = 목표 65%/);
});

// ---- 총 에너지 소모량 ------------------------------------------------------ //

import { sumRoadEnergy, sumRouteSummaryEnergy } from '../DltLogViewer/js/tvas-renderer.js';

test('sumRoadEnergy_adds_all_RO4_energyConsumption_Wh', () => {
  const roads = [{ energyConsumption: 120 }, { energyConsumption: -30 }, { energyConsumption: 410 }];
  assert.equal(sumRoadEnergy(roads), 500);
});

test('sumRouteSummaryEnergy_adds_all_RS7_energy_W', () => {
  assert.equal(sumRouteSummaryEnergy([{ energy: 1500 }, { energy: 2500 }, { energy: 0 }]), 4000);
});

test('energy_sums_are_zero_for_missing_data', () => {
  assert.deepEqual([sumRoadEnergy(null), sumRouteSummaryEnergy(undefined), sumRoadEnergy([{}])], [0, 0, 0]);
});

// ---- 경유지/목적지 도착 예상 배터리 --------------------------------------- //
//
// 현재 배터리(currentEnergy, Wh) - 지점까지 RO4 누적 소모(lastVxIdx ≤ 지점 vx 인 구간 합).

import { predictArrivalBattery } from '../DltLogViewer/js/tvas-renderer.js';

const roads3 = [
  { lastVxIdx: 10, energyConsumption: 1000 },
  { lastVxIdx: 20, energyConsumption: 2000 },
  { lastVxIdx: 30, energyConsumption: 3000 },
];

test('predictArrivalBattery_subtracts_cumulative_RO4_energy_up_to_each_point', () => {
  const r = predictArrivalBattery({
    roads: roads3, currentEnergy: 10000,
    points: [{ label: '경유지1', vxIdx: 20 }, { label: '목적지', vxIdx: 30 }],
  });
  assert.deepEqual(r.map(p => [p.label, p.consumedWh, p.remainingWh]), [
    ['경유지1', 3000, 7000],
    ['목적지', 6000, 4000],
  ]);
});

// 배터리 용량(capacityWh)을 알면 SoC(%)도 계산한다.
test('predictArrivalBattery_reports_percent_when_capacity_known', () => {
  const r = predictArrivalBattery({
    roads: roads3, currentEnergy: 10000, capacityWh: 20000,
    points: [{ label: '목적지', vxIdx: 30 }],
  });
  assert.equal(r[0].percent, 20);
});

test('predictArrivalBattery_percent_null_without_capacity', () => {
  const r = predictArrivalBattery({ roads: roads3, currentEnergy: 10000, points: [{ label: '목적지', vxIdx: 30 }] });
  assert.equal(r[0].percent, null);
});

// 충전소를 지나면 그 충전소의 목표 충전량(targetSoc)으로 충전된 상태에서 다시 소모한다.
test('predictArrivalBattery_recharges_to_target_soc_at_must_charger_before_point', () => {
  const r = predictArrivalBattery({
    roads: roads3, currentEnergy: 4000, capacityWh: 20000,
    chargers: [{ vxIdx: 10, targetSoc: 80 }],        // VX10 에서 목표 80%(16000Wh)로 충전
    points: [{ label: '경유지1', vxIdx: 20 }, { label: '목적지', vxIdx: 30 }],
  });
  assert.deepEqual(r.map(p => [p.remainingWh, p.percent]), [[14000, 70], [11000, 55]]);
});

test('predictArrivalBattery_ignores_chargers_after_point_and_without_capacity', () => {
  const after = predictArrivalBattery({
    roads: roads3, currentEnergy: 10000, capacityWh: 20000,
    chargers: [{ vxIdx: 25, targetSoc: 80 }],
    points: [{ label: '경유지1', vxIdx: 20 }],
  });
  assert.equal(after[0].remainingWh, 7000);
  const noCap = predictArrivalBattery({
    roads: roads3, currentEnergy: 10000,
    chargers: [{ vxIdx: 10, targetSoc: 80 }],
    points: [{ label: '목적지', vxIdx: 30 }],
  });
  assert.equal(noCap[0].remainingWh, 4000);
});

// ---- 도착 예상 배터리 마커 ------------------------------------------------- //

import { arrivalBatteryLabelHtml, buildArrivalBatteryPopup } from '../DltLogViewer/js/tvas-renderer.js';

test('arrivalBatteryLabelHtml_is_side_bubble_with_label_percent_and_wh', () => {
  const html = arrivalBatteryLabelHtml({ label: '목적지', remainingWh: 11000, percent: 55 }, 'right');
  assert.match(html, /data-side="right"/);
  assert.match(text(html), /목적지.*55%/);
  assert.match(text(html), /11,000 Wh/);
});

test('buildArrivalBatteryPopup_shows_consumed_remaining_and_shortage_warning', () => {
  const ok = text(buildArrivalBatteryPopup({ label: '경유지1', vxIdx: 20, consumedWh: 3000, remainingWh: 7000, percent: 35 }));
  assert.match(ok, /경유지1 도착 예상 배터리/);
  assert.match(ok, /누적 소모: 3,000 Wh/);
  assert.match(ok, /7,000 Wh \(35%\)/);
  assert.doesNotMatch(ok, /부족/);
  const short = text(buildArrivalBatteryPopup({ label: '목적지', vxIdx: 30, consumedWh: 12000, remainingWh: -2000, percent: null }));
  assert.match(short, /배터리 부족/);
});

// ---- 계산 대상 지점: WP2 경유지 + 목적지(마지막 보간점) ------------------------ //

import { arrivalBatteryPoints, chargingStops } from '../DltLogViewer/js/tvas-renderer.js';

test('arrivalBatteryPoints_lists_wp2_vias_then_destination_at_last_vx', () => {
  const coords = new Array(31).fill({ lat: 0, lon: 0 });
  assert.deepEqual(arrivalBatteryPoints([{ vxIdx: 12 }, { vxIdx: 20 }], coords), [
    { label: '경유지1', vxIdx: 12 },
    { label: '경유지2', vxIdx: 20 },
    { label: '목적지', vxIdx: 30 },
  ]);
});

// 예상 충전량(expectedSoc)이 있는 충전소에서 목표 충전량(targetSoc)까지 충전한다.
test('chargingStops_lists_chargers_with_expected_charge_and_their_target_soc', () => {
  const evs = [
    { mustCharge: 1, vxIdx: 10, arrivalSoc: 20, expectedSoc: 60 },
    { mustCharge: 0, vxIdx: 15, arrivalSoc: 40, expectedSoc: 0 },
    { mustCharge: 1, vxIdx: 25, arrivalSoc: 10, expectedSoc: 30 },
  ];
  assert.deepEqual(chargingStops(evs), [
    { vxIdx: 10, arrivalSoc: 20, chargeSoc: 60, targetSoc: 80 },
    { vxIdx: 25, arrivalSoc: 10, chargeSoc: 30, targetSoc: 40 },    // ES3 도달 10 + 30 = 40
  ]);
});

// ---- 충전소 SoC 계획 (ES3) ------------------------------------------------ //
//
// arrivalSoc(offset 41) = 충전소 도달 시 SoC(%), expectedSoc(offset 42) = 예상 충전량(%).
// 도착 예상 SoC = ES3 충전소 도달 시 SoC(arrivalSoc) 그대로 — 앞선 충전소 충전은 이미 반영된 값
//   (실 응답: 도곡2동 22%+8% 충전 후 기흥휴게소 도달 21% = 에너지 계산 21.9% 와 일치).
// 목표 충전량   = 도착 예상 SoC + 이 충전소 예상 충전량.

import { buildChargerSocPlan } from '../DltLogViewer/js/tvas-renderer.js';

test('buildChargerSocPlan_uses_server_arrival_soc_without_adding_previous_charges', () => {
  const evs = [
    { vxIdx: 50, arrivalSoc: 15, expectedSoc: 50 },   // 경로상 두 번째
    { vxIdx: 10, arrivalSoc: 20, expectedSoc: 60 },   // 경로상 첫 번째
  ];
  const plan = buildChargerSocPlan(evs);
  assert.deepEqual(plan.map(p => [p.vxIdx, p.arrivalSoc, p.chargeSoc, p.targetSoc]), [
    [50, 15, 50, 65],    // ES3 도달 15, 15 + 50 = 65 (앞 충전소 60 은 더하지 않음)
    [10, 20, 60, 80],    // 20 + 60 = 80
  ]);
});

// ---- 말풍선 위치 (경로선을 가리지 않게 좌/우) ------------------------------- //
//
// 지점에서 이어지는 경로(다음 보간점, 마지막 점이면 이전 보간점에서 온 방향)가
// 동쪽이면 왼쪽, 서쪽이면 오른쪽에 말풍선을 둔다.

import { bubbleSide } from '../DltLogViewer/js/tvas-renderer.js';

test('bubbleSide_left_when_route_continues_east', () => {
  const coords = [{ lat: 37, lon: 127 }, { lat: 37.001, lon: 127.01 }];
  assert.equal(bubbleSide(coords, 0), 'left');
});

test('bubbleSide_right_when_route_continues_west', () => {
  const coords = [{ lat: 37, lon: 127 }, { lat: 37.001, lon: 126.99 }];
  assert.equal(bubbleSide(coords, 0), 'right');
});

// 마지막 점(목적지)은 다음 점이 없으므로 들어온 방향으로 판단: 서→동으로 도착하면 경로는 왼쪽에 있다 → 오른쪽.
test('bubbleSide_last_point_uses_incoming_direction', () => {
  const coords = [{ lat: 37, lon: 126.99 }, { lat: 37, lon: 127 }];
  assert.equal(bubbleSide(coords, 1), 'right');
  const fromEast = [{ lat: 37, lon: 127.01 }, { lat: 37, lon: 127 }];
  assert.equal(bubbleSide(fromEast, 1), 'left');
});

// ---- 충전소 도착/목표 SoC 말풍선 ------------------------------------------- //

import { chargerSocBubbleHtml } from '../DltLogViewer/js/tvas-renderer.js';

test('chargerSocBubbleHtml_shows_arrival_and_target_soc_on_given_side', () => {
  const html = chargerSocBubbleHtml({ name: '광장 충전소' }, { arrivalSoc: 20, chargeSoc: 60, targetSoc: 80 }, 'left');
  assert.match(html, /data-side="left"/);
  assert.match(text(html), /도착 20%/);
  assert.match(text(html), /목표 80%/);
  assert.match(text(html), /\+60%/);
});

// ---- 충전소와 같은 자리의 경유지 (실 응답: 경유지 VX450 = 기흥휴게소 충전소 VX449) ---- //
//
// 경유지 "도착" 배터리는 그 자리 충전소에서 충전하기 전 값이어야 한다.

test('predictArrivalBattery_point_at_charger_uses_battery_before_that_charge', () => {
  const r = predictArrivalBattery({
    roads: roads3, currentEnergy: 4000, capacityWh: 20000,
    chargers: [{ vxIdx: 10, targetSoc: 80 }, { vxIdx: 19, targetSoc: 100 }],
    points: [{ label: '경유지1', vxIdx: 20, atChargerVx: 19 }],
  });
  // VX19 충전소는 경유지 자신 → VX10 충전(16000Wh) 후 VX20 까지 2000Wh 소모
  assert.equal(r[0].remainingWh, 14000);
});

// 지점에서 50m 이내의 충전 지점이 있으면 atChargerVx 로 표시한다.
test('arrivalBatteryPoints_marks_waypoint_at_charging_stop_within_50m', () => {
  const coords = Array.from({ length: 31 }, (_, i) => ({ lat: 37 + i * 0.001, lon: 127 }));   // 약 111m 간격
  coords[19] = { lat: 37.02 - 0.00009, lon: 127 };                                            // VX20 과 약 10m
  const pts = arrivalBatteryPoints([{ vxIdx: 20 }], coords, [{ vxIdx: 10, targetSoc: 80 }, { vxIdx: 19, targetSoc: 100 }]);
  assert.deepEqual(pts[0], { label: '경유지1', vxIdx: 20, atChargerVx: 19 });
  assert.equal(pts[1].atChargerVx, undefined);   // 목적지 VX30 근처엔 충전소 없음
});

// 렌더러가 같은 자리 충전소 말풍선의 반대편에 두려면 결과에 atChargerVx 가 남아 있어야 한다.
test('predictArrivalBattery_keeps_atChargerVx_in_result', () => {
  const r = predictArrivalBattery({
    roads: roads3, currentEnergy: 4000, capacityWh: 20000,
    chargers: [{ vxIdx: 19, targetSoc: 100 }],
    points: [{ label: '경유지1', vxIdx: 20, atChargerVx: 19 }, { label: '목적지', vxIdx: 30 }],
  });
  assert.deepEqual(r.map(p => p.atChargerVx), [19, undefined]);
});

// ---- RO4 에너지 소모량 기준 (ES3 SoC 없이 직접 계산) --------------------------------- //
//
// 출발 currentEnergy 에서 RO4 소모를 빼고, 충전소를 지날 때 예상 충전량% × 용량을 더한다.

import { predictEnergyChain } from '../DltLogViewer/js/tvas-renderer.js';

test('predictEnergyChain_adds_expected_charge_percent_of_capacity_at_each_stop', () => {
  const r = predictEnergyChain({
    roads: roads3, currentEnergy: 4000, capacityWh: 20000,
    chargers: [{ vxIdx: 10, chargeSoc: 60 }],                 // +12000Wh
    points: [{ label: '경유지1', vxIdx: 20 }],
  });
  // 4000 - 1000(VX10까지) + 12000 - 2000(VX20까지) = 13000 = 65%
  assert.deepEqual([r[0].remainingWh, r[0].percent], [13000, 65]);
});

test('predictEnergyChain_point_at_charger_is_before_that_charge_and_no_capacity_skips_charges', () => {
  const at = predictEnergyChain({
    roads: roads3, currentEnergy: 10000, capacityWh: 20000,
    chargers: [{ vxIdx: 19, chargeSoc: 50 }],
    points: [{ label: '경유지1', vxIdx: 20, atChargerVx: 19 }, { label: '목적지', vxIdx: 30 }],
  });
  assert.deepEqual(at.map(p => p.remainingWh), [7000, 14000]);   // 경유지: 충전 전 / 목적지: +10000
  const noCap = predictEnergyChain({
    roads: roads3, currentEnergy: 10000,
    chargers: [{ vxIdx: 10, chargeSoc: 50 }],
    points: [{ label: '목적지', vxIdx: 30 }],
  });
  assert.deepEqual([noCap[0].remainingWh, noCap[0].percent], [4000, null]);
});

// ---- ES3 도달 SoC 기준 / RO4 에너지 소모량 기준 구분 표시 -------------------------------------- //

test('arrivalBatteryLabelHtml_shows_es3_soc_and_ro4_energy_values_separately', () => {
  const server = { label: '목적지', remainingWh: 10209, percent: 13.5 };
  const energy = { label: '목적지', remainingWh: 9461, percent: 12.5 };
  const t = text(arrivalBatteryLabelHtml(server, 'right', energy));
  assert.match(t, /ES3 충전소 도달 시 SoC 기준 13\.5%/);
  assert.match(t, /RO4 에너지 소모량 기준 12\.5%/);
  assert.doesNotMatch(t, /서버/);
});

test('chargerSocBubbleHtml_shows_es3_soc_and_ro4_energy_arrival_and_target', () => {
  const soc = { arrivalSoc: 21, chargeSoc: 25, targetSoc: 46 };
  const energy = { remainingWh: 15107, percent: 20 };
  const t = text(chargerSocBubbleHtml({ name: '기흥휴게소' }, soc, 'right', energy));
  assert.match(t, /ES3 충전소 도달 시 SoC 21% → 목표 46%/);
  assert.match(t, /RO4 에너지 소모량 20% → 목표 45%/);
  assert.doesNotMatch(t, /서버/);
});

test('buildArrivalBatteryPopup_lists_es3_soc_and_ro4_energy_wh', () => {
  const t = text(buildArrivalBatteryPopup(
    { label: '목적지', vxIdx: 30, consumedWh: 31554, remainingWh: 10209, percent: 13.5 },
    { remainingWh: 9461, percent: 12.5 },
  ));
  assert.match(t, /ES3 충전소 도달 시 SoC 기준: 10,209 Wh \(13\.5%\)/);
  assert.match(t, /RO4 에너지 소모량 기준: 9,461 Wh \(12\.5%\)/);
  assert.doesNotMatch(t, /서버/);
});
