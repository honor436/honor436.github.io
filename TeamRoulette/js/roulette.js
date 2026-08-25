// 조편성 룰렛 — 순수 로직 (DOM/캔버스 의존 없음)

/**
 * 자유 입력(줄바꿈 · 쉼표 혼용)을 이름 배열로 변환한다.
 * 공백만 있는 항목은 버리고, 동명이인을 위해 중복은 유지한다.
 */
export function parseNames(text) {
  if (!text) return [];
  return String(text)
    .split(/[\n,]/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/**
 * 0 ~ count-1 중 하나를 균등 확률로 고른다.
 * rng 는 [0,1) 를 반환하는 함수(테스트에서 주입).
 */
export function pickIndex(count, rng = Math.random) {
  if (count <= 0) return -1;
  const i = Math.floor(rng() * count);
  return Math.min(Math.max(i, 0), count - 1);
}

/** 세그먼트 1칸의 각도(도). */
export function segmentAngle(count) {
  return 360 / count;
}

/** 0~360 정규화. */
function norm360(deg) {
  return ((deg % 360) + 360) % 360;
}

/**
 * 휠을 rotationDeg 만큼 시계방향으로 돌렸을 때 12시 포인터가 가리키는 세그먼트 인덱스.
 */
export function indexAtPointer(rotationDeg, count) {
  if (count <= 0) return -1;
  const under = norm360(-rotationDeg);
  return Math.min(Math.floor(under / segmentAngle(count)), count - 1);
}

/**
 * index 번 세그먼트의 '중심'이 포인터에 오도록 하는 회전각(도).
 * turns 만큼 추가로 더 돌린다(연출용).
 */
export function rotationForIndex(index, count, turns = 0) {
  const seg = segmentAngle(count);
  return norm360(-(index + 0.5) * seg) + 360 * turns;
}

/** 초기 게임 상태. pool = 아직 안 뽑힌 사람, teams = 조별 배정 결과. */
export function createGame(names, teamCount) {
  return {
    pool: [...names],
    teams: Array.from({ length: teamCount }, () => []),
  };
}

/** 다음에 배정할 조 = 인원이 가장 적은 조(동률이면 앞 조). */
export function nextTeamIndex(teams) {
  let best = 0;
  for (let i = 1; i < teams.length; i++) {
    if (teams[i].length < teams[best].length) best = i;
  }
  return best;
}

/** 아직 뽑을 사람이 남았는지. */
export function isFinished(game) {
  return game.pool.length === 0;
}

/**
 * 한 번 돌린다. 원본 game 은 건드리지 않고 새 상태를 반환한다.
 * @returns {{index:number, name:string|null, teamIndex:number, game:object}}
 */
export function spin(game, rng = Math.random) {
  const index = pickIndex(game.pool.length, rng);
  if (index < 0) {
    return { index: -1, name: null, teamIndex: -1, game };
  }
  const name = game.pool[index];
  const teamIndex = nextTeamIndex(game.teams);
  const teams = game.teams.map((t, i) => (i === teamIndex ? [...t, name] : t));
  const pool = game.pool.filter((_, i) => i !== index);
  return { index, name, teamIndex, game: { pool, teams } };
}

/**
 * 현재 회전각(누적)에서 index 세그먼트로 멈추도록 하는 다음 목표 회전각.
 * 항상 현재보다 앞으로(시계방향) 나아간다.
 */
export function spinTargetRotation(currentRotation, index, count, turns = 5) {
  const base = Math.floor(currentRotation / 360) * 360;
  return base + rotationForIndex(index, count, Math.max(1, turns));
}

/** 세그먼트 i 의 색(HSL). 인접 세그먼트가 확실히 구분되도록 색상환을 건너뛴다. */
export function segmentColor(i, count) {
  const hue = Math.round((i * 360) / count + (i % 2) * 180) % 360;
  const light = i % 2 === 0 ? 58 : 46;
  return `hsl(${hue} 72% ${light}%)`;
}

/** HTML 특수문자 이스케이프. */
export function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
}

/** 조 배정 결과 → HTML 문자열. */
export function renderTeamsHtml(teams) {
  return teams
    .map((members, i) => {
      const items = members
        .map((n) => `<li class="member">${escapeHtml(n)}</li>`)
        .join('');
      return `<section class="team" style="--team-color:${segmentColor(i, teams.length)}">
  <h3 class="team-title"><span class="team-badge">${i + 1}조</span><span class="team-count">${members.length}명</span></h3>
  <ol class="team-members">${items}</ol>
</section>`;
    })
    .join('');
}

/** 세그먼트 i 의 이름표를 뒤집어 그려야 하는지(휠 왼쪽 절반). */
export function shouldFlipLabel(index, count) {
  return (index + 0.5) * segmentAngle(count) > 180;
}
