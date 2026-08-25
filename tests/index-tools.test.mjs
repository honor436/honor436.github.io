import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

// 메인 페이지(개발용 웹 도구 모음)의 도구 카드 링크가 실제 경로를 가리키는지 검증.

const root = new URL('../', import.meta.url);
const html = readFileSync(new URL('index.html', root), 'utf-8');

function cardLinks(source) {
  return [...source.matchAll(/class="card-link"\s+href="\.\/([^"]+)"/g)].map((m) => m[1]);
}

test('index_tool_cards_link_to_existing_paths', () => {
  const links = cardLinks(html);
  assert.ok(links.length >= 3, `도구 카드가 너무 적다: ${links.length}`);
  for (const link of links) {
    assert.ok(existsSync(fileURLToPath(new URL(link, root))), `없는 경로: ${link}`);
  }
});

test('index_lists_team_roulette_tool', () => {
  assert.ok(cardLinks(html).includes('TeamRoulette/'), '조편성 룰렛 카드가 없다');
});

test('index_team_roulette_card_has_title', () => {
  assert.match(html, /조편성 룰렛/);
});

// 공개 도구 페이지는 모두 사이트 비밀번호 게이트를 포함해야 한다.

test('every_tool_page_includes_auth_gate', () => {
  const pages = ['index.html', ...cardLinks(html).map((l) => `${l}index.html`)];
  for (const page of pages) {
    const source = readFileSync(fileURLToPath(new URL(page, root)), 'utf-8');
    assert.match(source, /auth-gate\.js/, `게이트 누락: ${page}`);
  }
});
