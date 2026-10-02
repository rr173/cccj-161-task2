// 只增事件日志 + SHA-256 哈希链。
// 每条记录形如：
// { seq, ts, type, payload, prevHash, hash }
// hash = sha256(canonical({seq,ts,type,payload,prevHash}))
// 任何对历史记录的篡改都会破坏链；append 之后永不更新、永不删除。
import { createReadStream, existsSync, mkdirSync, appendFileSync, writeFileSync } from 'node:fs';
import { createInterface } from 'node:readline';
import { dirname } from 'node:path';
import { canonicalJSON, sha256Hex } from './util.js';

export class EventStore {
  constructor(file = null) {
    this.file = file;
    this.records = [];
    this.seq = 0;
    this.tipHash = '0'.repeat(64);
    if (file && dirname(file) && dirname(file) !== '.') {
      mkdirSync(dirname(file), { recursive: true });
    }
  }

  static hashOf(rec) {
    const { hash, ...rest } = rec;
    void hash;
    return sha256Hex(canonicalJSON(rest));
  }

  append(type, payload, ts) {
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

  // 截断式重置：清空内存与磁盘（演示复位用）
  truncate() {
    this.records = [];
    this.seq = 0;
    this.tipHash = '0'.repeat(64);
    if (this.file) writeFileSync(this.file, '');
  }

  // 校验整条哈希链；返回 { ok, length, tipHash, brokenAt, reason }
  verify() {
    let prev = '0'.repeat(64);
    for (let i = 0; i < this.records.length; i++) {
      const r = this.records[i];
      if (r.seq !== i + 1) {
        return { ok: false, length: this.records.length, tipHash: this.tipHash, brokenAt: r.seq, reason: '序号不连续' };
      }
      if (r.prevHash !== prev) {
        return { ok: false, length: this.records.length, tipHash: this.tipHash, brokenAt: r.seq, reason: 'prevHash 不衔接（链断裂）' };
      }
      const expect = EventStore.hashOf(r);
      if (expect !== r.hash) {
        return { ok: false, length: this.records.length, tipHash: this.tipHash, brokenAt: r.seq, reason: '记录内容与哈希不符（记录被改写）' };
      }
      prev = r.hash;
    }
    return { ok: true, length: this.records.length, tipHash: this.tipHash };
  }

  async load() {
    if (!this.file || !existsSync(this.file)) return;
    const rl = createInterface({ input: createReadStream(this.file), crlfDelay: Infinity });
    for await (const line of rl) {
      const t = line.trim();
      if (!t) continue;
      const rec = JSON.parse(t);
      this.records.push(rec);
      this.seq = Math.max(this.seq, rec.seq);
      this.tipHash = rec.hash;
    }
  }
}
