/* honor436 사이트 비밀번호 게이트 (클라이언트 측 차단막).
 *
 * 주의: GitHub Pages 는 정적 호스팅이라 서버 인증이 불가능합니다.
 * 이 게이트는 일반 사용자에 대한 가벼운 차단일 뿐 진짜 보안이 아니며,
 * 소스 보기/개발자도구로 우회 가능합니다. 비밀번호는 평문 대신 SHA-256
 * 해시로 비교합니다.
 *
 * 사용법: 각 페이지 <head> 에 (defer 없이) 다음 한 줄을 넣으면 됩니다.
 *   <script src="/auth-gate.js"></script>
 * head 단계에서 즉시 실행되어 본문을 가리고 비밀번호 입력을 요구합니다.
 */
(function () {
  var KEY = 'h436_gate_v1';
  // SHA-256("mns1234") — 비밀번호 평문은 소스에 두지 않음
  var HASH = 'f4ae2d8c0ea877f403e06bf5a4e9b98e798cacb2f5d0d2fdf07d5dabf21572fe';

  // 이미 인증된 세션이면 아무것도 하지 않음
  try { if (sessionStorage.getItem(KEY) === '1') return; } catch (e) {}

  var root = document.documentElement;

  // 인증 전: 본문 숨김 + 스크롤 잠금 (head 단계에서 즉시 적용 → 내용 노출 방지)
  var style = document.createElement('style');
  style.id = 'h436-gate-style';
  style.textContent =
    'body{visibility:hidden!important}html{overflow:hidden!important}' +
    '#h436-gate{visibility:visible!important}';
  root.appendChild(style);

  function unlock() {
    try { sessionStorage.setItem(KEY, '1'); } catch (e) {}
    var s = document.getElementById('h436-gate-style');
    if (s && s.parentNode) s.parentNode.removeChild(s);
    var g = document.getElementById('h436-gate');
    if (g && g.parentNode) g.parentNode.removeChild(g);
  }

  function sha256hex(str) {
    return crypto.subtle.digest('SHA-256', new TextEncoder().encode(str)).then(function (buf) {
      return Array.prototype.map.call(new Uint8Array(buf), function (b) {
        return b.toString(16).padStart(2, '0');
      }).join('');
    });
  }

  function build() {
    if (document.getElementById('h436-gate')) return;
    var gate = document.createElement('div');
    gate.id = 'h436-gate';
    gate.setAttribute('style',
      'position:fixed;inset:0;z-index:2147483647;' +
      'background:radial-gradient(ellipse at top,#111a2e 0%,#0b1220 65%);' +
      'display:flex;align-items:center;justify-content:center;visibility:visible;' +
      'font-family:"Segoe UI","Noto Sans KR",-apple-system,sans-serif');
    gate.innerHTML =
      '<form id="h436-gate-form" style="background:rgba(15,23,42,0.95);border:1px solid rgba(148,163,184,0.18);border-radius:14px;padding:30px 26px;width:min(92vw,350px);box-shadow:0 24px 70px rgba(0,0,0,.55);text-align:center">' +
        '<div style="font-size:32px;margin-bottom:6px">🔒</div>' +
        '<div style="color:#e2e8f0;font-size:17px;font-weight:800;margin-bottom:3px">honor436 개발 도구</div>' +
        '<div style="color:#94a3b8;font-size:12.5px;margin-bottom:18px">비밀번호를 입력하세요</div>' +
        '<input id="h436-gate-input" type="password" autocomplete="current-password" placeholder="비밀번호" style="width:100%;padding:11px 13px;border-radius:9px;border:1px solid rgba(148,163,184,0.3);background:rgba(2,6,23,0.6);color:#e2e8f0;font-size:15px;outline:none;margin-bottom:10px">' +
        '<div id="h436-gate-error" style="color:#f87171;font-size:12px;min-height:16px;margin-bottom:10px"></div>' +
        '<button type="submit" style="width:100%;padding:11px;border:none;border-radius:9px;background:#38bdf8;color:#0b1220;font-size:15px;font-weight:800;cursor:pointer">입장</button>' +
      '</form>';
    (document.body || root).appendChild(gate);

    var form = document.getElementById('h436-gate-form');
    var input = document.getElementById('h436-gate-input');
    var err = document.getElementById('h436-gate-error');
    form.addEventListener('submit', function (e) {
      e.preventDefault();
      err.textContent = '';
      sha256hex(input.value).then(function (h) {
        if (h === HASH) { unlock(); }
        else { err.textContent = '비밀번호가 올바르지 않습니다.'; input.value = ''; input.focus(); }
      }).catch(function () {
        err.textContent = '이 환경에서는 인증을 사용할 수 없습니다 (HTTPS 필요).';
      });
    });
    input.focus();
  }

  // body 가 있으면 즉시, 없으면 documentElement 에 바로 붙여 즉시 노출
  build();
})();
