// 崩溃恢复契约测试：
// 1. 正常重启：干净加载并延续原链
// 2. 断电残尾（末尾单条不完整记录）：留存审计副本、截除残尾、从最后验证记录续链
// 3. 尾部完整但哈希错误：只读隔离（不属于残尾）
// 4. 中间记录篡改（内容 / prevHash / 序号）：只读隔离并报告首个损坏位置
// 5. 隔离期间读写边界：读开放、写（含 RESET）一律拒绝且日志字节不变
// 6. 残尾恢复后再次重启与连续追加：链连续、再次加载为 clean
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, appendFile, readFile, writeFile, readdir } from 'node:fs/promises';
import { readFileSync } from 'node:fs';
import { spawn } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { EventStore } from '../src/store.js';
import { Kernel } from '../src/kernel.js';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');

async function makeLog(t) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'dr-rec-'));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, 'events.log');
  const store = new EventStore(file);
  const kernel = new Kernel(store); // 空库 -> 写入种子（与服务器启动路径一致）
  return { dir, file, store, kernel };
}

async function readLines(file) {
  const raw = await readFile(file, 'utf8');
  return raw.split('\n').filter(Boolean);
}

test('正常重启：干净加载，状态一致并延续原链', async (t) => {
  const { file, store, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  kernel.dispatch({ type: 'SET_METER_OFFLINE', meterId: 'MP-1', offline: true });
  const seq = store.seq;
  const tip = store.tipHash;

  const st2 = new EventStore(file);
  const rec = await st2.load();
  assert.equal(rec.status, 'clean');
  assert.equal(rec.records, seq);
  assert.equal(st2.seq, seq);
  assert.equal(st2.tipHash, tip);
  assert.equal(st2.quarantine, null);
  assert.equal(st2.verify().ok, true);

  const k2 = new Kernel(st2); // 非空库不重复种子
  assert.equal(k2.s.offline.has('MP-1'), true, '重放后投影一致');
  k2.dispatch({ type: 'TICK', minutes: 15 });
  const firstNew = st2.all()[seq]; // 重启后追加的第一条记录（TICK 可能级联多条）
  assert.equal(firstNew.seq, seq + 1);
  assert.equal(firstNew.prevHash, tip, '新记录衔接重启前链尖');
  assert.equal(st2.verify().ok, true);
});

test('断电残尾：末尾半截 JSON 留存审计副本后截除，从最后验证记录续链', async (t) => {
  const { dir, file, store, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const goodSeq = store.seq;
  const goodTip = store.tipHash;
  const goodBytes = readFileSync(file, 'utf8');

  // 模拟断电：只来得及写出半截 JSON（无换行提交标记）
  const fragment = `{"seq":${goodSeq + 1},"ts":1759`;
  await appendFile(file, fragment);

  const st2 = new EventStore(file);
  const rec = await st2.load();
  assert.equal(rec.status, 'tail-recovered');
  assert.equal(rec.resumedFromSeq, goodSeq);
  assert.equal(rec.resumedTipHash, goodTip);
  assert.equal(rec.droppedTail.bytes, Buffer.byteLength(fragment));
  assert.equal(rec.droppedTail.parseable, false);

  // 残尾的可审计副本：逐字节一致
  const baks = (await readdir(dir)).filter((f) => f.includes('.tail-') && f.endsWith('.bak'));
  assert.equal(baks.length, 1);
  const copy = await readFile(path.join(dir, baks[0]), 'utf8');
  assert.equal(copy, fragment);
  assert.ok(rec.droppedTail.auditCopy.endsWith(baks[0]));

  // 主日志恢复为已验证前缀的原始字节，服务不隔离、可续链
  assert.equal(readFileSync(file, 'utf8'), goodBytes);
  assert.equal(st2.quarantine, null);
  assert.equal(st2.verify().ok, true);
  const k2 = new Kernel(st2);
  k2.dispatch({ type: 'TICK', minutes: 15 });
  const firstNew = st2.all()[goodSeq]; // 恢复后续写的第一条记录
  assert.equal(firstNew.seq, goodSeq + 1);
  assert.equal(firstNew.prevHash, goodTip, '恢复后续写延续原链');
  assert.equal(st2.verify().ok, true);
});

test('残尾恰好是合法 JSON（缺换行提交标记）同样按未提交截除', async (t) => {
  const { dir, file, store, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const goodSeq = store.seq;
  const goodBytes = readFileSync(file, 'utf8');

  // 构造一条“内容完整、哈希自洽但缺换行提交标记”的残片
  const draft = { seq: goodSeq + 1, ts: null, type: 'SIM_TICK', payload: { torn: true }, prevHash: store.tipHash };
  draft.hash = EventStore.hashOf(draft);
  await appendFile(file, JSON.stringify(draft)); // 故意不写换行

  const st2 = new EventStore(file);
  const rec = await st2.load();
  assert.equal(rec.status, 'tail-recovered');
  assert.equal(rec.droppedTail.parseable, true, '残片可解析但仍属未提交');
  assert.equal(st2.seq, goodSeq, '未提交记录不进入链');
  assert.equal(readFileSync(file, 'utf8'), goodBytes);
  const baks = (await readdir(dir)).filter((f) => f.endsWith('.bak'));
  assert.equal(baks.length, 1);
});

test('尾部完整但哈希错误：进入只读隔离（不属于断电残尾）', async (t) => {
  const { dir, file, store, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const lines = await readLines(file);
  const lastRec = JSON.parse(lines.at(-1));
  lastRec.hash = '0'.repeat(64); // 记录完整、换行齐全，仅内容哈希被改写
  lines[lines.length - 1] = JSON.stringify(lastRec);
  const tampered = lines.join('\n') + '\n';
  await writeFile(file, tampered);

  const st2 = new EventStore(file);
  const rec = await st2.load();
  assert.equal(rec.status, 'quarantine');
  assert.equal(st2.quarantine.brokenAt, lastRec.seq);
  assert.match(st2.quarantine.reason, /哈希不符/);
  assert.equal(st2.records.length, lastRec.seq - 1, '只保留已验证前缀');
  assert.equal(st2.verify().ok, false);
  assert.equal(st2.verify().quarantined, true);
  // 隔离不触碰日志文件，也不产生残尾副本
  assert.equal(readFileSync(file, 'utf8'), tampered);
  assert.equal((await readdir(dir)).filter((f) => f.endsWith('.bak')).length, 0);
});

test('中间记录篡改：内容 / prevHash / 序号异常均进入隔离并定位首个损坏', async (t) => {
  // 变体 A：改写中间记录内容（哈希不再自洽）
  {
    const { file, store } = await makeLog(t);
    const lines = await readLines(file);
    const mid = JSON.parse(lines[2]);
    mid.payload = { ...mid.payload, tampered: true };
    lines[2] = JSON.stringify(mid);
    await writeFile(file, lines.join('\n') + '\n');

    const st2 = new EventStore(file);
    await st2.load();
    assert.equal(st2.quarantine.brokenAt, mid.seq);
    assert.match(st2.quarantine.reason, /哈希不符/);
    assert.equal(st2.records.length, mid.seq - 1);
    void store;
  }
  // 变体 B：中间记录 prevHash 不衔接（链断裂）
  {
    const { file } = await makeLog(t);
    const lines = await readLines(file);
    const mid = JSON.parse(lines[3]);
    mid.prevHash = '1'.repeat(64);
    lines[3] = JSON.stringify(mid);
    await writeFile(file, lines.join('\n') + '\n');

    const st2 = new EventStore(file);
    await st2.load();
    assert.equal(st2.quarantine.brokenAt, mid.seq);
    assert.match(st2.quarantine.reason, /prevHash 不衔接/);
  }
  // 变体 C：序号不连续
  {
    const { file } = await makeLog(t);
    const lines = await readLines(file);
    const mid = JSON.parse(lines[1]);
    const expectSeq = mid.seq;
    mid.seq = expectSeq + 100;
    lines[1] = JSON.stringify(mid);
    await writeFile(file, lines.join('\n') + '\n');

    const st2 = new EventStore(file);
    await st2.load();
    assert.equal(st2.quarantine.brokenAt, expectSeq, '按日志位置报告首个损坏');
    assert.equal(st2.quarantine.foundSeq, expectSeq + 100);
    assert.match(st2.quarantine.reason, /序号不连续/);
  }
});

test('隔离期间读写边界：状态可读且暴露损坏位置，一切写入被拒绝且日志不变', async (t) => {
  const { file, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const lines = await readLines(file);
  const lastRec = JSON.parse(lines.at(-1));
  lastRec.hash = 'f'.repeat(64);
  lines[lines.length - 1] = JSON.stringify(lastRec);
  const tampered = lines.join('\n') + '\n';
  await writeFile(file, tampered);

  const st2 = new EventStore(file);
  await st2.load();
  const k2 = new Kernel(st2); // 隔离：不写种子，只重放已验证前缀

  // —— 读边界：状态接口暴露首个损坏位置，前缀投影可审计 ——
  const view = k2.stateView();
  assert.equal(view.quarantine.brokenAt, lastRec.seq);
  assert.match(view.quarantine.reason, /哈希不符/);
  assert.equal(view.chain.ok, false);
  assert.equal(view.chain.quarantined, true);
  assert.ok(view.meters.length > 0, '已验证前缀（含种子）仍可查阅');
  assert.ok(view.events !== undefined);

  // —— 写边界：所有命令（含 RESET）拒绝，错误码 STORE_QUARANTINED ——
  const writes = [
    { type: 'PUBLISH_EVENT', meterId: 'MP-1', targetKw: 10 },
    { type: 'CANCEL_EVENT', eventId: 'EV_x' },
    { type: 'CLAIM_DEVICE', eventId: 'EV_x', deviceId: 'DEV-1', aggregatorId: 'AGG-A' },
    { type: 'DEVICE_RECEIPT', eventId: 'EV_x', deviceId: 'DEV-1', status: 'ACCEPTED' },
    { type: 'INGEST_READINGS', items: [] },
    { type: 'LOAD_MISSING_DAY', meterId: 'MP-1', dayOffset: 4 },
    { type: 'BACKFILL_ACTUALS', eventId: 'EV_x' },
    { type: 'SET_METER_OFFLINE', meterId: 'MP-1', offline: true },
    { type: 'TICK', minutes: 15 },
    { type: 'RESET' },
  ];
  for (const cmd of writes) {
    assert.throws(() => k2.dispatch(cmd), (e) => e.code === 'STORE_QUARANTINED', cmd.type);
  }
  // store 层兜底：直接追加 / 截断同样拒绝（RESET 无法销毁证据）
  assert.throws(() => st2.append('SIM_TICK', {}), (e) => e.code === 'STORE_QUARANTINED' && e.status === 503);
  assert.throws(() => st2.truncate(), (e) => e.code === 'STORE_QUARANTINED');

  // 日志文件字节级不变（没有追加、没有截断）
  assert.equal(readFileSync(file, 'utf8'), tampered);
});

test('HTTP 边界：隔离服务只读启动，GET 开放、POST 一律 503 且不写日志', async (t) => {
  const { file, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const lines = await readLines(file);
  const lastRec = JSON.parse(lines.at(-1));
  lastRec.hash = 'a'.repeat(64);
  lines[lines.length - 1] = JSON.stringify(lastRec);
  const tampered = lines.join('\n') + '\n';
  await writeFile(file, tampered);

  const port = 18600 + Math.floor(Math.random() * 300);
  const base = `http://127.0.0.1:${port}`;
  const child = spawn(process.execPath, ['src/server.js'], {
    cwd: ROOT,
    env: { ...process.env, PORT: String(port), DR_LOG: file },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr.on('data', (c) => (stderr += c));
  t.after(() => child.kill('SIGKILL'));

  // 等待服务以只读模式启动
  let state = null;
  for (let i = 0; i < 60 && !state; i++) {
    try {
      const res = await fetch(`${base}/api/state`);
      if (res.ok) state = await res.json();
    } catch { /* 尚未就绪 */ }
    if (!state) await new Promise((r) => setTimeout(r, 100));
  }
  assert.ok(state, `隔离模式下服务应能只读启动${stderr ? '：' + stderr : ''}`);

  // 状态接口显示首个损坏位置
  assert.equal(state.quarantine.brokenAt, lastRec.seq);
  assert.equal(state.chain.quarantined, true);

  // 所有业务写入 503 / STORE_QUARANTINED
  const posts = [
    ['/api/events', { meterId: 'MP-1', targetKw: 10, startInMinutes: 30 }],
    ['/api/events/cancel', { eventId: 'EV_x' }],
    ['/api/claims', { eventId: 'EV_x', deviceId: 'DEV-1', aggregatorId: 'AGG-A' }],
    ['/api/receipts', { eventId: 'EV_x', deviceId: 'DEV-1', status: 'ACCEPTED' }],
    ['/api/readings', { items: [] }],
    ['/api/late-history', { meterId: 'MP-1', dayOffset: 4 }],
    ['/api/backfill', { eventId: 'EV_x' }],
    ['/api/meters/offline', { meterId: 'MP-1', offline: true }],
    ['/api/sim/tick', { minutes: 15 }],
    ['/api/sim/reset', {}],
  ];
  for (const [p, body] of posts) {
    const res = await fetch(base + p, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    assert.equal(res.status, 503, `POST ${p} 应返回 503`);
    const data = await res.json();
    assert.equal(data.code, 'STORE_QUARANTINED', p);
    assert.equal(data.quarantine.brokenAt, lastRec.seq, p);
  }

  // 只读接口保持开放
  const chain = await (await fetch(`${base}/api/chain/verify`)).json();
  assert.equal(chain.ok, false);
  assert.equal(chain.brokenAt, lastRec.seq);
  assert.equal((await fetch(`${base}/api/log`)).status, 200);
  assert.equal((await fetch(`${base}/`)).status, 200);

  // 全程未向日志追加任何字节
  assert.equal(readFileSync(file, 'utf8'), tampered);
});

test('残尾恢复后再次重启与连续追加：链连续、再次加载为 clean', async (t) => {
  const { dir, file, store, kernel } = await makeLog(t);
  kernel.dispatch({ type: 'TICK', minutes: 15 });
  const seq1 = store.seq;
  const tip1 = store.tipHash;
  await appendFile(file, '{"seq":999,"broken":'); // 断电残尾

  // 第一次重启：残尾恢复
  const st2 = new EventStore(file);
  const rec2 = await st2.load();
  assert.equal(rec2.status, 'tail-recovered');
  const k2 = new Kernel(st2);

  // 恢复后连续追加多条记录
  k2.dispatch({ type: 'TICK', minutes: 15 });
  k2.dispatch({ type: 'TICK', minutes: 15 });
  k2.dispatch({ type: 'SET_METER_OFFLINE', meterId: 'MP-1', offline: true });
  const seq2 = st2.seq;
  const tip2 = st2.tipHash;
  assert.ok(seq2 >= seq1 + 3, '恢复后持续追加');

  // 恢复点之后的第一条新记录衔接断电前链尖（延续原链）
  assert.equal(st2.all()[seq1].seq, seq1 + 1);
  assert.equal(st2.all()[seq1].prevHash, tip1);

  // 第二次重启：干净加载，链完整、序号连续
  const st3 = new EventStore(file);
  const rec3 = await st3.load();
  assert.equal(rec3.status, 'clean');
  assert.equal(st3.seq, seq2);
  assert.equal(st3.tipHash, tip2);
  assert.equal(st3.verify().ok, true);
  st3.all().forEach((r, i) => assert.equal(r.seq, i + 1, '序号 1..N 连续'));

  // 第三次启动的服务仍可继续追加且链保持完整
  const k3 = new Kernel(st3);
  k3.dispatch({ type: 'TICK', minutes: 15 });
  assert.equal(st3.all()[seq2].prevHash, tip2, '再次重启后续写仍衔接原链');
  assert.equal(st3.verify().ok, true);

  // 审计副本仍在，未被后续运行清除
  assert.equal((await readdir(dir)).filter((f) => f.endsWith('.bak')).length, 1);
});
