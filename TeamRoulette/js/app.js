// 조편성 룰렛 — DOM 연결
import {
  parseNames,
  createGame,
  spin,
  isFinished,
  spinTargetRotation,
  segmentColor,
  renderTeamsHtml,
} from './roulette.js';
import { drawWheel } from './wheel.js';

const SPIN_MS = 4200;
const STORE_NAMES = 'team-roulette:names';
const STORE_TEAMS = 'team-roulette:teamCount';

const SAMPLE = [
  '김민준', '이서연', '박지훈', '최수아', '정우진', '강하은', '조현우', '윤지민',
  '임서준', '한예린', '오도윤', '신다은', '서건우', '권채원', '황시우', '안유나',
  '송재민', '전소율', '홍준서', '문가율', '양태현', '배지우', '백서윤', '허준영',
  '남기훈', '유하윤', '노은준', '심아름', '구본혁', '표민서',
];

const $ = (id) => document.getElementById(id);
const el = {
  canvas: $('wheel'),
  rotor: $('wheel-rotor'),
  spinBtn: $('spin-btn'),
  spinLabel: document.querySelector('.spin-label'),
  namesInput: $('names-input'),
  teamCount: $('team-count'),
  nameCounter: $('name-counter'),
  startBtn: $('start-btn'),
  sampleBtn: $('sample-btn'),
  resetBtn: $('reset-btn'),
  warn: $('setup-warn'),
  remainChip: $('remain-chip'),
  lastChip: $('last-chip'),
  teams: $('teams'),
  overlay: $('overlay'),
  confetti: $('confetti'),
  winnerName: $('winner-name'),
  winnerTeam: $('winner-team'),
  nextBtn: $('next-btn'),
  closeBtn: $('close-btn'),
};

const state = {
  game: createGame([], 4),
  rotation: 0,
  spinning: false,
};

// ---------- 렌더 ---------------------------------------------------------- //

function renderWheel() {
  drawWheel(el.canvas, state.game.pool);
}

function renderStatus() {
  const remain = state.game.pool.length;
  const done = state.game.teams.reduce((n, t) => n + t.length, 0);
  el.remainChip.textContent = remain > 0
    ? `남은 인원 ${remain}명 · 배정 완료 ${done}명`
    : (done > 0 ? `조편성 완료 · 총 ${done}명` : '명단을 입력하세요');
  el.spinBtn.disabled = state.spinning || remain === 0;
  el.spinLabel.textContent = remain === 0 && done > 0 ? '완료' : '돌리기';
}

function renderTeams() {
  el.teams.innerHTML = state.game.teams.some((t) => t.length)
    ? renderTeamsHtml(state.game.teams)
    : '';
}

function renderAll() {
  renderWheel();
  renderStatus();
  renderTeams();
}

// ---------- 게임 시작/초기화 ---------------------------------------------- //

function currentNames() {
  return parseNames(el.namesInput.value);
}

function updateCounter() {
  el.nameCounter.textContent = `${currentNames().length}명`;
}

function startGame() {
  const names = currentNames();
  const teamCount = Math.max(2, Math.min(12, Number(el.teamCount.value) || 4));
  el.teamCount.value = teamCount;

  if (names.length < 2) {
    el.warn.textContent = '참가자를 2명 이상 입력해 주세요.';
    el.warn.hidden = false;
    return;
  }
  el.warn.hidden = true;

  localStorage.setItem(STORE_NAMES, el.namesInput.value);
  localStorage.setItem(STORE_TEAMS, String(teamCount));

  state.game = createGame(names, teamCount);
  state.spinning = false;
  el.lastChip.textContent = '아직 뽑지 않았습니다';
  renderAll();
}

// ---------- 스핀 ---------------------------------------------------------- //

function doSpin() {
  if (state.spinning || isFinished(state.game)) return;

  const result = spin(state.game, Math.random);
  const count = state.game.pool.length;
  const turns = 5 + Math.floor(Math.random() * 3);
  const target = spinTargetRotation(state.rotation, result.index, count, turns);

  state.spinning = true;
  state.rotation = target;
  renderStatus();

  el.rotor.style.transition = `transform ${SPIN_MS}ms cubic-bezier(.14,.62,.06,1)`;
  el.rotor.style.transform = `rotate(${target}deg)`;

  let settled = false;
  const settle = () => {
    if (settled) return;
    settled = true;
    finishSpin(result);
  };
  el.rotor.addEventListener('transitionend', settle, { once: true });
  setTimeout(settle, SPIN_MS + 250);
}

function finishSpin(result) {
  const teamCount = state.game.teams.length;
  state.game = result.game;

  el.lastChip.textContent = `최근 당첨: ${result.name} → ${result.teamIndex + 1}조`;
  el.winnerName.textContent = result.name;
  el.winnerTeam.textContent = `${result.teamIndex + 1}조 배정`;
  el.winnerTeam.style.setProperty('--winner-team-color', segmentColor(result.teamIndex, teamCount));
  el.nextBtn.hidden = isFinished(state.game);
  el.nextBtn.textContent = '다음 뽑기';

  burstConfetti();
  el.overlay.hidden = false;
  el.nextBtn.hidden ? el.closeBtn.focus() : el.nextBtn.focus();

  renderTeams();
}

function closeOverlay() {
  el.overlay.hidden = true;
  el.confetti.innerHTML = '';
  state.spinning = false;
  renderWheel(); // 뽑힌 사람이 사라진 룰렛
  renderStatus();
}

function burstConfetti() {
  const pieces = [];
  for (let i = 0; i < 46; i++) {
    const left = Math.random() * 100;
    const delay = Math.random() * 0.5;
    const dur = 1.6 + Math.random() * 1.6;
    const color = segmentColor(i, 46);
    pieces.push(
      `<i style="left:${left}%;background:${color};animation-duration:${dur}s;animation-delay:${delay}s"></i>`
    );
  }
  el.confetti.innerHTML = pieces.join('');
}

// ---------- 이벤트 -------------------------------------------------------- //

el.spinBtn.addEventListener('click', doSpin);
el.startBtn.addEventListener('click', startGame);
el.resetBtn.addEventListener('click', startGame);
el.sampleBtn.addEventListener('click', () => {
  el.namesInput.value = SAMPLE.join('\n');
  updateCounter();
  startGame();
});
el.namesInput.addEventListener('input', updateCounter);
el.closeBtn.addEventListener('click', closeOverlay);
el.nextBtn.addEventListener('click', () => {
  closeOverlay();
  doSpin();
});
el.overlay.addEventListener('click', (e) => {
  if (e.target === el.overlay) closeOverlay();
});

document.addEventListener('keydown', (e) => {
  const typing = e.target === el.namesInput || e.target === el.teamCount;
  if (typing) return;
  if (e.code === 'Space' || e.code === 'Enter') {
    e.preventDefault();
    if (!el.overlay.hidden) {
      if (el.nextBtn.hidden) closeOverlay();
      else { closeOverlay(); doSpin(); }
    } else {
      doSpin();
    }
  }
  if (e.code === 'Escape' && !el.overlay.hidden) closeOverlay();
});

window.addEventListener('resize', renderWheel);

// ---------- 초기화 -------------------------------------------------------- //

el.namesInput.value = localStorage.getItem(STORE_NAMES) || '';
el.teamCount.value = localStorage.getItem(STORE_TEAMS) || '4';
updateCounter();
if (currentNames().length >= 2) startGame();
else renderAll();
