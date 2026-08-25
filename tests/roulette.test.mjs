import test from 'node:test';
import assert from 'node:assert/strict';
import {
  parseNames,
  pickIndex,
  segmentAngle,
  indexAtPointer,
  rotationForIndex,
  createGame,
  nextTeamIndex,
  spin,
  isFinished,
  spinTargetRotation,
  segmentColor,
  renderTeamsHtml,
  shouldFlipLabel,
} from '../TeamRoulette/js/roulette.js';

// ---- parseNames (이름 입력 파싱) ----------------------------------------- //
//
// 줄바꿈 / 쉼표로 구분된 자유 입력을 이름 배열로 만든다.

test('parseNames_splits_by_newline', () => {
  assert.deepEqual(parseNames('홍길동\n김철수\n이영희'), ['홍길동', '김철수', '이영희']);
});

test('parseNames_splits_by_comma', () => {
  assert.deepEqual(parseNames('홍길동, 김철수 ,이영희'), ['홍길동', '김철수', '이영희']);
});

test('parseNames_drops_empty_and_trims', () => {
  assert.deepEqual(parseNames('  홍길동  \n\n\n , , 김철수\n'), ['홍길동', '김철수']);
});

test('parseNames_empty_returns_empty_array', () => {
  assert.deepEqual(parseNames(''), []);
  assert.deepEqual(parseNames(null), []);
});

test('parseNames_keeps_duplicate_names', () => {
  assert.deepEqual(parseNames('김철수\n김철수'), ['김철수', '김철수']);
});

// ---- pickIndex (균등 추첨) ------------------------------------------------ //

test('pickIndex_uses_rng_to_choose_index', () => {
  assert.equal(pickIndex(4, () => 0), 0);
  assert.equal(pickIndex(4, () => 0.5), 2);
  assert.equal(pickIndex(4, () => 0.999), 3);
});

test('pickIndex_clamps_rng_returning_one', () => {
  assert.equal(pickIndex(4, () => 1), 3);
});

test('pickIndex_empty_returns_minus_one', () => {
  assert.equal(pickIndex(0, () => 0.5), -1);
});

// ---- 룰렛 기하 (회전각 ↔ 세그먼트) --------------------------------------- //
//
// 규칙: 포인터는 12시 방향(각도 0). 회전각 0 일 때 세그먼트 i 는
// 시계방향으로 [i*seg, (i+1)*seg) 구간을 차지한다.
// 휠을 R 도만큼 시계방향으로 돌리면 포인터가 가리키는 세그먼트가 바뀐다.

test('segmentAngle_divides_360_evenly', () => {
  assert.equal(segmentAngle(4), 90);
  assert.equal(segmentAngle(30), 12);
});

test('indexAtPointer_at_zero_rotation_is_first_segment', () => {
  assert.equal(indexAtPointer(0, 4), 0);
});

test('indexAtPointer_clockwise_rotation_moves_backwards', () => {
  // 시계방향 90도 회전 → 바로 앞(반시계) 세그먼트가 포인터로 올라온다
  assert.equal(indexAtPointer(90, 4), 3);
  assert.equal(indexAtPointer(180, 4), 2);
  assert.equal(indexAtPointer(270, 4), 1);
});

test('indexAtPointer_normalizes_multi_turn_and_negative_rotation', () => {
  assert.equal(indexAtPointer(360 * 5 + 90, 4), 3);
  assert.equal(indexAtPointer(-90, 4), 1);
});

test('rotationForIndex_puts_segment_center_under_pointer', () => {
  // 4등분에서 0번 세그먼트 중심(45도)이 포인터로 오려면 315도 회전
  assert.equal(rotationForIndex(0, 4, 0), 315);
});

test('rotationForIndex_adds_full_turns', () => {
  assert.equal(rotationForIndex(0, 4, 5), 315 + 360 * 5);
});

test('rotationForIndex_roundtrips_through_indexAtPointer', () => {
  const count = 30;
  for (let i = 0; i < count; i++) {
    assert.equal(indexAtPointer(rotationForIndex(i, count, 6), count), i);
  }
});

// ---- 게임 상태 (조 배정 · 뽑힌 사람 제거) --------------------------------- //

const R = () => 0; // 항상 첫 번째를 뽑는 rng

test('createGame_starts_with_all_names_in_pool_and_empty_teams', () => {
  const g = createGame(['A', 'B', 'C'], 4);
  assert.deepEqual(g.pool, ['A', 'B', 'C']);
  assert.equal(g.teams.length, 4);
  assert.deepEqual(g.teams, [[], [], [], []]);
});

test('nextTeamIndex_returns_team_with_fewest_members', () => {
  assert.equal(nextTeamIndex([['A'], ['B'], [], ['C']]), 2);
});

test('nextTeamIndex_ties_prefer_lower_index', () => {
  assert.equal(nextTeamIndex([[], [], [], []]), 0);
  assert.equal(nextTeamIndex([['A'], [], [], []]), 1);
});

test('spin_removes_winner_from_pool', () => {
  const g = createGame(['A', 'B', 'C'], 4);
  const r = spin(g, R);
  assert.equal(r.name, 'A');
  assert.deepEqual(r.game.pool, ['B', 'C']);
});

test('spin_assigns_winner_to_next_team', () => {
  const g = createGame(['A', 'B', 'C'], 4);
  const r1 = spin(g, R);
  assert.equal(r1.teamIndex, 0);
  assert.deepEqual(r1.game.teams[0], ['A']);
  const r2 = spin(r1.game, R);
  assert.equal(r2.teamIndex, 1);
  assert.deepEqual(r2.game.teams[1], ['B']);
});

test('spin_does_not_mutate_previous_state', () => {
  const g = createGame(['A', 'B'], 2);
  spin(g, R);
  assert.deepEqual(g.pool, ['A', 'B']);
  assert.deepEqual(g.teams, [[], []]);
});

test('spin_on_empty_pool_returns_null_winner', () => {
  const g = createGame([], 4);
  const r = spin(g, R);
  assert.equal(r.name, null);
  assert.equal(r.index, -1);
});

test('spin_30_people_into_4_teams_balances_8_8_7_7', () => {
  const names = Array.from({ length: 30 }, (_, i) => `P${i + 1}`);
  let g = createGame(names, 4);
  const picked = [];
  while (!isFinished(g)) {
    const r = spin(g, Math.random);
    picked.push(r.name);
    g = r.game;
  }
  assert.deepEqual(g.teams.map((t) => t.length), [8, 8, 7, 7]);
  assert.equal(new Set(picked).size, 30); // 모두 한 번씩만 뽑힌다
  assert.deepEqual([...g.teams.flat()].sort(), [...names].sort());
});

test('isFinished_true_only_when_pool_empty', () => {
  assert.equal(isFinished(createGame(['A'], 2)), false);
  assert.equal(isFinished(createGame([], 2)), true);
});

// ---- UI 보조 순수 로직 ---------------------------------------------------- //

test('spinTargetRotation_always_moves_forward', () => {
  const cur = 1234.5;
  const t = spinTargetRotation(cur, 3, 30, 5);
  assert.ok(t > cur, `${t} > ${cur}`);
});

test('spinTargetRotation_lands_on_requested_index', () => {
  for (const cur of [0, 37.2, 359.9, 3600]) {
    for (const idx of [0, 1, 14, 29]) {
      assert.equal(indexAtPointer(spinTargetRotation(cur, idx, 30, 5), 30), idx);
    }
  }
});

test('spinTargetRotation_spins_at_least_the_requested_turns', () => {
  assert.ok(spinTargetRotation(0, 0, 30, 5) >= 360 * 5);
});

test('segmentColor_is_stable_and_distinct', () => {
  assert.equal(segmentColor(0, 4), segmentColor(0, 4));
  assert.notEqual(segmentColor(0, 4), segmentColor(1, 4));
});

test('renderTeamsHtml_lists_each_team_with_members', () => {
  const html = renderTeamsHtml([['홍길동', '김철수'], ['이영희']]);
  assert.match(html, /1조/);
  assert.match(html, /2조/);
  assert.match(html, /홍길동/);
  assert.match(html, /이영희/);
});

test('renderTeamsHtml_shows_member_count', () => {
  const html = renderTeamsHtml([['A', 'B'], []]);
  assert.match(html, /2명/);
  assert.match(html, /0명/);
});

test('renderTeamsHtml_escapes_html_in_names', () => {
  const html = renderTeamsHtml([['<img src=x onerror=alert(1)>']]);
  assert.ok(!html.includes('<img'), 'raw tag must not leak into HTML');
  assert.match(html, /&lt;img/);
});

// ---- 라벨 방향 ------------------------------------------------------------ //
//
// 왼쪽 절반(중심각 180~360도)의 이름은 뒤집어야 왼→오로 읽힌다.

test('shouldFlipLabel_right_half_is_not_flipped', () => {
  assert.equal(shouldFlipLabel(0, 4), false);   // 중심 45도
  assert.equal(shouldFlipLabel(1, 4), false);   // 중심 135도
});

test('shouldFlipLabel_left_half_is_flipped', () => {
  assert.equal(shouldFlipLabel(2, 4), true);    // 중심 225도
  assert.equal(shouldFlipLabel(3, 4), true);    // 중심 315도
});

test('shouldFlipLabel_matches_half_split_for_many_segments', () => {
  const count = 30;
  const flipped = Array.from({ length: count }, (_, i) => shouldFlipLabel(i, count));
  assert.equal(flipped.filter(Boolean).length, count / 2);
});
