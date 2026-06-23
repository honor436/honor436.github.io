/**
 * 빌드(배포) 날짜 표시 유틸.
 * 정적 사이트라 별도 빌드 스텝이 없으므로 document.lastModified
 * (서버가 내려주는 Last-Modified = 파일 갱신/배포 시각)를 사용한다.
 */

'use strict';

/**
 * document.lastModified 문자열 또는 Date 를 'YYYY-MM-DD' 로 포맷.
 * 파싱 불가하면 오늘 날짜로 폴백.
 */
export function formatBuildDate(input) {
  let d = input instanceof Date ? input : new Date(input);
  if (isNaN(d.getTime())) d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}
