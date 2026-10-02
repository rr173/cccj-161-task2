// 只增事件日志 + SHA-256 哈希链。
// 每条记录形如：
// { seq, ts, type, payload, prevHash, hash }
// hash = sha256(canonical({seq,ts,type,payload,prevHash}))
// 任何对历史记录的篡改都会破坏链；append 之后永不更新、永不删除。
//
// 崩溃恢复契约（重启加载时）：
// 1. 任何“完整记录”（以换行符为提交标记）的序号、前序哈希或内容哈希异常
//    -> 进入只读隔离：只重放已验证通过的前缀，append/truncate 一律拒绝，
//    隔离信息（首个损坏位置）通过 stateView/verify 暴露给状态接口与页面。
// 2. 唯一的可自动恢复异常：文件末尾的单条“不完整记录”（断电残尾，
//    即最后一个换行符之后的残片——换行符是记录的提交标记，无换行即未提交）。
//    残尾先原样留存可审计副本（<log>.tail-<时间戳>.bak），再从日志中截除，
//    服务从最后一条验证通过的记录恢复并延续原链。
// 3. 尾部记录完整但哈希错误不属于残尾，按第 1 条进入隔离（不改动任何字节）。
import { existsSync, mkdirSync, appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { canonicalJSON, sha256Hex } from './util.js';

const ZERO_HASH = '0'.repeat(64);

// 记录结构完整性（解析之后、链校验之前）
function structuralProblem(rec) {
  if (!rec || typeof rec !== 'object' || Array.isArray(rec)) return '记录不是 JSON 对象';
  if (!Number.isInteger(rec.seq)) return '缺少合法序号 seq';
  if (typeof rec.type !== 'string' || !rec.type) return '缺少事件类型 type';
  if (typeof rec.prevHash !== 'string' || !/^[0-9a-f]{64}$/.test(rec.prevHash)) return 'prevHash 结构非法';
  if (typeof rec.hash !== 'string' || !/^[0-9a-f]{64}$/.test(rec.hash)) return 'hash 结构非法';
  return null;
}

export class EventStore {
  constructor(file = null) {
    this.file = file;
    this.records = [];
    this.seq = 0;
    this.tipHash = ZERO_HASH;
    // 隔离状态：null 或 { brokenAt, lineNo, reason, foundSeq?, goodRecords, totalLines }
    this.quarantine = null;
    // 本次加载的恢复报告：{ status: 'empty'|'clean'|'tail-recovered'|'quarantine', ... }
    this.recovery = { status: 'empty', records: 0 };
    if (file && dirname(file) && dirname(file) !== '.') {
      mkdirSync(dirname(file), { recursive: true });
    }
  }

  static hashOf(rec) {
    const { hash, ...rest } = rec;
    void hash;
    return sha256Hex(canonicalJSON(rest));
  }

  // 对一串已解析记录做链校验，返回首个问题或 null（load 与 verify 共用同一套判定）
  static chainProblem(records) {
    let prev = ZERO_HASH;
    for (let i = 0; i < records.length; i++) {
      const r = records[i];
      const sp = structuralProblem(r);
      if (sp) return { brokenAt: i + 1, reason: `记录结构不完整（${sp}）` };
      if (r.seq !== i + 1) return { brokenAt: i + 1, foundSeq: r.seq, reason: '序号不连续' };
      if (r.prevHash !== prev) return { brokenAt: r.seq, reason: 'prevHash 不衔接（链断裂）' };
      if (EventStore.hashOf(r) !== r.hash) {
        return { brokenAt: r.seq, reason: '记录内容与哈希不符（记录被改写）' };
      }
      prev = r.hash;
    }
    return null;
  }

  #quarantinedError(action) {
    const q = this.quarantine;
    const err = new Error(
      `事件日志处于只读隔离：首个损坏记录 seq ${q.brokenAt}（${q.reason}），拒绝${action}；请审计日志文件后人工处理`
    );
    err.code = 'STORE_QUARANTINED';
    err.status = 503;
    err.details = { quarantine: q };
    return err;
  }

  append(type, payload, ts) {
    if (this.quarantine) throw this.#quarantinedError('追加记录');
    const rec = {
      seq: this.seq + 1,
      ts: ts ?? null,
      type,
      payload: payload ?? {},
      prevHash: this.tipHash,
    };
    rec.hash = EventStore.hashOf(rec);
    this.records.push(rec);
    this.seq = rec.seq;
    this.tipHash = rec.hash;
    if (this.file) {
      appendFileSync(this.file, JSON.stringify(rec) + '\n');
    }
    return rec;
  }

  all() {
    return this.records;
  }

  // 截断式重置：清空内存与磁盘（演示复位用）。隔离期间禁止——日志文件即证据。
  truncate() {
    if (this.quarantine) throw this.#quarantinedError('截断日志');
    this.records = [];
    this.seq = 0;
    this.tipHash = ZERO_HASH;
    if (this.file) writeFileSync(this.file, '');
  }

  // 校验整条哈希链；返回 { ok, length, tipHash, brokenAt, reason, ... }
  // 隔离状态下直接报告隔离信息（首个损坏位置），内存中只留有已验证前缀。
  verify() {
    if (this.quarantine) {
      return {
        ok: false,
        quarantined: true,
        length: this.records.length,
        tipHash: this.tipHash,
        ...this.quarantine,
      };
    }
    const problem = EventStore.chainProblem(this.records);
    if (problem) {
      return { ok: false, length: this.records.length, tipHash: this.tipHash, ...problem };
    }
    return { ok: true, length: this.records.length, tipHash: this.tipHash };
  }

  // 启动加载：按恢复契约重放日志，返回恢复报告（同时写入 this.recovery / this.quarantine）
  async load() {
    this.quarantine = null;
    if (!this.file || !existsSync(this.file)) {
      this.recovery = { status: 'empty', records: 0 };
      return this.recovery;
    }
    const raw = readFileSync(this.file, 'utf8');
    if (!raw.trim()) {
      this.recovery = { status: 'empty', records: 0 };
      return this.recovery;
    }

    // 换行符是记录的提交标记：文件不以换行结尾 => 末尾存在未提交的断电残尾
    const hasTail = !raw.endsWith('\n');
    const lines = raw.split('\n');
    const fragment = hasTail ? lines.pop() : (lines.pop(), null); // 以换行结尾时末段恒为空串

    // 逐条解析并校验完整记录；首个异常即进入隔离（只保留已验证前缀）
    const good = [];
    let problem = null;
    for (let i = 0; i < lines.length; i++) {
      const expected = good.length + 1;
      let rec;
      try {
        rec = JSON.parse(lines[i]);
      } catch {
        problem = { brokenAt: expected, lineNo: i + 1, reason: '记录不是合法 JSON（完整记录损坏）' };
        break;
      }
      const sp = structuralProblem(rec);
      if (sp) {
        problem = { brokenAt: expected, lineNo: i + 1, foundSeq: rec?.seq ?? null, reason: `记录结构不完整（${sp}）` };
        break;
      }
      if (rec.seq !== expected) {
        problem = { brokenAt: expected, lineNo: i + 1, foundSeq: rec.seq, reason: '序号不连续' };
        break;
      }
      const prev = good.length ? good[good.length - 1].hash : ZERO_HASH;
      if (rec.prevHash !== prev) {
        problem = { brokenAt: expected, lineNo: i + 1, reason: 'prevHash 不衔接（链断裂）' };
        break;
      }
      if (EventStore.hashOf(rec) !== rec.hash) {
        problem = { brokenAt: expected, lineNo: i + 1, reason: '记录内容与哈希不符（记录被改写）' };
        break;
      }
      good.push(rec);
    }

    this.records = good;
    this.seq = good.length ? good[good.length - 1].seq : 0;
    this.tipHash = good.length ? good[good.length - 1].hash : ZERO_HASH;

    // 情形 1：完整记录异常 -> 只读隔离。日志文件一字节不动（文件即证据）。
    if (problem) {
      this.quarantine = {
        ...problem,
        goodRecords: good.length,
        totalLines: lines.length + (hasTail ? 1 : 0),
      };
      this.recovery = { status: 'quarantine', records: good.length, quarantine: this.quarantine };
      return this.recovery;
    }

    // 情形 2：仅末尾单条不完整记录（断电残尾）-> 留存审计副本、截除残尾、从最后验证记录续链
    if (hasTail) {
      const stamp = new Date().toISOString().replace(/[:.]/g, '-');
      const auditCopy = `${this.file}.tail-${stamp}.bak`;
      writeFileSync(auditCopy, fragment); // 残尾原始字节，逐字节留存
      writeFileSync(this.file, lines.map((l) => l + '\n').join('')); // 截除残尾，保留已验证行的原始字节
      let parseable = false;
      try {
        JSON.parse(fragment);
        parseable = true; // 残片恰好是合法 JSON（例如只缺换行提交标记），仍按未提交处理
      } catch { /* 典型的半截 JSON 残尾 */ }
      this.recovery = {
        status: 'tail-recovered',
        records: good.length,
        resumedFromSeq: this.seq,
        resumedTipHash: this.tipHash,
        droppedTail: {
          bytes: Buffer.byteLength(fragment, 'utf8'),
          preview: fragment.slice(0, 120),
          parseable,
          auditCopy,
        },
      };
      return this.recovery;
    }

    this.recovery = { status: 'clean', records: good.length };
    return this.recovery;
  }
}
