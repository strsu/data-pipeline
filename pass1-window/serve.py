"""Pass-1 윈도우 실험 실시간 뷰어 — 서버가 이미지까지 서빙하므로 별도 http.server 불필요.

`run_window.py`가 백그라운드로 JSONL을 append하는 동안, 이 페이지가 5초마다 폴링해 진행바·집계·
컷 카드를 갱신한다. 컷마다 [이미지 | glm 티처 | qwen 싱글 | qwen 윈도우] 4열 대조.

실행:
  cd webtoon-pipeline && PYTHONPATH=. .venv/bin/python ../pass1-window/serve.py --port 8791
  → http://127.0.0.1:8791
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

_PIPELINE = Path(__file__).resolve().parents[1] / "webtoon-pipeline"
if str(_PIPELINE) not in sys.path:
    sys.path.insert(0, str(_PIPELINE))

CFG: dict = {}


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _blocks_view(san: dict) -> list[dict]:
    out = []
    for b in (san or {}).get("blocks", []):
        sp = b.get("speaker") or {}
        out.append({
            "index": b.get("index"), "type": b.get("type"),
            "text": b.get("corrected_text", ""),
            "speaker": sp.get("face_label") or sp.get("name"),
            "basis": sp.get("basis"),
        })
    return out


def _arm_view(entry: dict) -> dict:
    if not entry:
        return {"missing": True}
    st = entry.get("structural") or {}
    san = entry.get("sanitized") or {}
    return {
        "error": entry.get("error"),
        "summary": san.get("cut_summary"),
        "chars": [c.get("face_label") for c in san.get("characters", []) if c.get("face_label")],
        "blocks": _blocks_view(san),
        "merge": len(st.get("merges") or []),
        "missing_idx": st.get("missing_indices") or [],
        "extra_idx": st.get("extra_indices") or [],
        "latency": entry.get("latency_s"),
        "prompt_tokens": (entry.get("usage") or {}).get("prompt_tokens"),
    }


def build_data() -> dict:
    src_dir: Path = CFG["src_dir"]
    out_dir: Path = CFG["out_dir"]
    wid, eps = CFG["webtoon"], CFG["episodes"]

    src: list[dict] = []
    for no in eps:
        src.extend([r for r in _read_jsonl(src_dir / f"w{wid}_e{no}.jsonl") if not r.get("skipped")])
    src.sort(key=lambda r: (r["episode_no"], r["cut_number"]))

    win = {}
    for no in eps:
        for r in _read_jsonl(out_dir / f"w{wid}_e{no}.jsonl"):
            win[(r["episode_no"], r["cut_number"])] = r

    cuts = []
    agg = {"single": {"n": 0, "merge": 0, "missing": 0, "spk_t": 0, "spk_a": 0},
           "window": {"n": 0, "merge": 0, "missing": 0, "spk_t": 0, "spk_a": 0}}
    for r in src:
        key = (r["episode_no"], r["cut_number"])
        w = win.get(key)
        single = _arm_view(r.get("student"))
        teacher = _arm_view(r.get("teacher") if not (r.get("teacher") or {}).get("error") else None)
        window = _arm_view(w["window_call"]) if w else {"pending": True}

        # 윈도우가 처리한 컷만 집계에 넣는다(진행 중 공정 대조).
        if w and not window.get("error"):
            for name, arm, ag in (("single", single, r.get("agreement")),
                                  ("window", window, w.get("agreement_vs_teacher"))):
                if arm.get("error") or arm.get("missing"):
                    continue
                a = agg[name]
                a["n"] += 1
                a["merge"] += 1 if arm["merge"] else 0
                a["missing"] += 1 if arm["missing_idx"] else 0
                if ag:
                    a["spk_t"] += ag["speaker_assigned_total"]
                    a["spk_a"] += ag["speaker_assigned_agree"]
        cuts.append({
            "episode_no": r["episode_no"], "cut_number": r["cut_number"],
            "image": r["image_file"], "n_regions": r["input"]["n_regions"],
            "window_cuts": w["window"]["cuts"] if w else None,
            "teacher": teacher, "single": single, "window": window,
        })

    def _pct(n, d):
        return round(100 * n / d, 1) if d else None

    for name in agg:
        a = agg[name]
        a["merge_rate"] = _pct(a["merge"], a["n"])
        a["missing_rate"] = _pct(a["missing"], a["n"])
        a["speaker_agree"] = _pct(a["spk_a"], a["spk_t"])
    return {
        "webtoon": wid, "episodes": eps,
        "total": len(src), "done": len(win),
        "agg": agg, "cuts": cuts,
        "out_dir": str(out_dir),
    }


HTML = r"""<!doctype html>
<html lang="ko"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pass-1 윈도우 실험 LIVE</title>
<style>
:root{--bg:#0e1116;--panel:#161b22;--panel2:#1c232d;--border:#2a323d;--fg:#e6edf3;--muted:#8b949e;
 --single:#58a6ff;--window:#3fb950;--teacher:#a371f7;--warn:#f0883e;--bad:#f85149}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
 font:13.5px/1.6 -apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo",sans-serif}
header{position:sticky;top:0;z-index:10;background:var(--panel);border-bottom:1px solid var(--border);padding:10px 16px}
.row1{display:flex;gap:14px;align-items:center;flex-wrap:wrap}
h1{font-size:15px;margin:0;font-weight:600}
.dot{width:8px;height:8px;border-radius:50%;background:var(--window);display:inline-block;margin-right:6px;
 animation:pulse 1.4s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:.3}50%{opacity:1}}
.bar{flex:1;min-width:180px;height:8px;background:var(--panel2);border-radius:4px;overflow:hidden}
.bar>i{display:block;height:100%;background:var(--window);transition:width .4s}
.status{color:var(--muted);font-size:12.5px}
.scores{display:flex;gap:8px;flex-wrap:wrap;margin-top:9px}
.card{background:var(--panel2);border:1px solid var(--border);border-radius:8px;padding:7px 11px;min-width:150px}
.card .k{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:13px;margin-top:3px;display:flex;gap:8px;align-items:baseline}
.s{color:var(--single);font-weight:600}.w{color:var(--window);font-weight:600}
.delta{font-size:11.5px;padding:1px 5px;border-radius:4px}
.up{background:rgba(63,185,80,.16);color:var(--window)}.down{background:rgba(248,81,73,.16);color:var(--bad)}
.flat{background:rgba(139,148,158,.16);color:var(--muted)}
main{padding:14px;max-width:1700px;margin:0 auto}
.cut{background:var(--panel);border:1px solid var(--border);border-radius:10px;margin-bottom:14px;overflow:hidden}
.cut>h2{margin:0;padding:8px 12px;font-size:12.5px;background:var(--panel2);border-bottom:1px solid var(--border);
 display:flex;gap:10px;align-items:center;font-weight:600}
.wc{color:var(--muted);font-weight:400;font-size:11.5px}
.grid{display:grid;grid-template-columns:230px 1fr 1fr 1fr;gap:0}
.grid>div{padding:10px 12px;border-right:1px solid var(--border)}
.grid>div:last-child{border-right:0}
.grid img{width:100%;border-radius:6px;display:block}
.col>h3{margin:0 0 7px;font-size:11px;text-transform:uppercase;letter-spacing:.05em}
.col.t h3{color:var(--teacher)}.col.s h3{color:var(--single)}.col.w h3{color:var(--window)}
.sum{color:var(--muted);font-size:12px;margin-bottom:7px}
.blk{border-top:1px dashed var(--border);padding:4px 0;font-size:12.5px}
.blk .m{color:var(--muted);font-size:11px}
.tag{display:inline-block;padding:0 5px;border-radius:3px;background:var(--panel2);
 border:1px solid var(--border);font-size:10.5px;margin-right:4px}
.spk{color:var(--window)}
.badge{font-size:10.5px;padding:1px 5px;border-radius:4px;margin-right:4px}
.b-ok{background:rgba(63,185,80,.15);color:var(--window)}
.b-bad{background:rgba(248,81,73,.15);color:var(--bad)}
.pending{color:var(--warn);font-size:12px}
.empty{color:var(--muted);text-align:center;padding:60px}
.filters{margin-top:9px;display:flex;gap:8px;align-items:center;font-size:12px;color:var(--muted)}
.filters button{background:var(--panel2);border:1px solid var(--border);color:var(--muted);
 border-radius:6px;padding:3px 9px;cursor:pointer;font-size:12px}
.filters button.on{border-color:var(--window);color:var(--window)}
</style></head><body>
<header>
 <div class="row1">
  <h1><span class="dot"></span>Pass-1 윈도우 실험 — center-predict</h1>
  <div class="bar"><i id="bar" style="width:0"></i></div>
  <div class="status" id="st">로딩…</div>
 </div>
 <div class="scores" id="scores"></div>
 <div class="filters">
  <span>보기:</span>
  <button data-f="all" class="on">전체</button>
  <button data-f="diff">화자 다른 컷만</button>
  <button data-f="done">완료만</button>
 </div>
</header>
<main><div id="list" class="empty">불러오는 중…</div></main>
<script>
let FILTER='all';
const esc=s=>(s||'').replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
function delta(s,w,lowerBetter){
  if(s==null||w==null) return '';
  const d=+(w-s).toFixed(1); if(Math.abs(d)<0.05) return '<span class="delta flat">±0</span>';
  const good=lowerBetter? d<0 : d>0;
  return `<span class="delta ${good?'up':'down'}">${d>0?'+':''}${d}</span>`;
}
function scoreCard(k,s,w,unit,lowerBetter){
  return `<div class="card"><div class="k">${k}</div><div class="v">
    <span class="s">${s==null?'—':s+unit}</span><span style="color:#555">→</span>
    <span class="w">${w==null?'—':w+unit}</span>${delta(s,w,lowerBetter)}</div></div>`;
}
function armHtml(a,cls,title){
  if(!a) return '';
  if(a.pending) return `<div class="col ${cls}"><h3>${title}</h3><div class="pending">대기 중…</div></div>`;
  if(a.missing) return `<div class="col ${cls}"><h3>${title}</h3><div class="pending">없음</div></div>`;
  if(a.error) return `<div class="col ${cls}"><h3>${title}</h3><div class="pending">에러: ${esc(a.error).slice(0,120)}</div></div>`;
  const bad=[];
  if(a.merge) bad.push(`<span class="badge b-bad">병합 ${a.merge}</span>`);
  if(a.missing_idx.length) bad.push(`<span class="badge b-bad">누락 ${a.missing_idx.join(',')}</span>`);
  if(a.extra_idx.length) bad.push(`<span class="badge b-bad">여분 ${a.extra_idx.join(',')}</span>`);
  if(!bad.length) bad.push('<span class="badge b-ok">1:1 정상</span>');
  const meta=[a.latency!=null?a.latency+'s':'',a.prompt_tokens?a.prompt_tokens+'tok':''].filter(Boolean).join(' · ');
  const blocks=a.blocks.map(b=>`<div class="blk">
     <span class="tag">#${b.index}</span><span class="tag">${b.type||'—'}</span>
     ${b.speaker?`<span class="spk">${esc(b.speaker)}</span> <span class="m">(${b.basis||'-'})</span>`:'<span class="m">화자없음</span>'}
     <div>${esc(b.text)}</div></div>`).join('');
  return `<div class="col ${cls}"><h3>${title} <span class="m" style="font-weight:400">${meta}</span></h3>
    <div>${bad.join('')}</div>
    <div class="sum">${esc(a.summary)||''}</div>
    <div class="m">인물: ${a.chars.join(', ')||'—'}</div>${blocks}</div>`;
}
function spkDiff(c){
  if(!c.window||c.window.pending||c.window.error) return false;
  const m=a=>JSON.stringify((a.blocks||[]).map(b=>[b.index,b.speaker]));
  return m(c.single)!==m(c.window);
}
async function tick(){
  const d=await (await fetch('/api/data')).json();
  const pct=d.total? 100*d.done/d.total : 0;
  document.getElementById('bar').style.width=pct+'%';
  document.getElementById('st').textContent=
    `w${d.webtoon} ep${d.episodes.join(',')} — ${d.done}/${d.total} 컷 (${pct.toFixed(0)}%) · 집계 ${d.agg.window.n}컷`;
  const s=d.agg.single,w=d.agg.window;
  document.getElementById('scores').innerHTML=
    scoreCard('병합 위반율 (낮을수록↑)',s.merge_rate,w.merge_rate,'%',true)+
    scoreCard('index 누락율 (낮을수록↑)',s.missing_rate,w.missing_rate,'%',true)+
    scoreCard('화자 일치율 vs glm (높을수록↑)',s.speaker_agree,w.speaker_agree,'%',false)+
    `<div class="card"><div class="k">범례</div><div class="v" style="font-size:11.5px">
      <span class="s">싱글</span> → <span class="w">윈도우</span></div></div>`;
  let cuts=d.cuts;
  if(FILTER==='done') cuts=cuts.filter(c=>c.window&&!c.window.pending);
  if(FILTER==='diff') cuts=cuts.filter(spkDiff);
  document.getElementById('list').className='';
  document.getElementById('list').innerHTML= cuts.length? cuts.map(c=>`
    <div class="cut"><h2>ep${c.episode_no} · 컷 ${c.cut_number} <span class="wc">블록 ${c.n_regions}
      ${c.window_cuts?`· 윈도우 [${c.window_cuts.join(', ')}]`:''}</span></h2>
      <div class="grid">
        <div><img loading="lazy" src="/img/${c.image}"></div>
        ${armHtml(c.teacher,'t','glm-4.6v (티처)')}
        ${armHtml(c.single,'s','qwen 싱글')}
        ${armHtml(c.window,'w','qwen 윈도우')}
      </div></div>`).join('') : '<div class="empty">해당 컷 없음</div>';
}
document.querySelectorAll('.filters button').forEach(b=>b.onclick=()=>{
  document.querySelectorAll('.filters button').forEach(x=>x.classList.remove('on'));
  b.classList.add('on'); FILTER=b.dataset.f; tick();
});
tick(); setInterval(tick,5000);
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # 폴링 로그 소음 억제
        pass

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):  # noqa: N802
        path = unquote(urlparse(self.path).path)
        if path == "/":
            return self._send(200, HTML.encode("utf-8"), "text/html; charset=utf-8")
        if path == "/api/data":
            try:
                body = json.dumps(build_data(), ensure_ascii=False).encode("utf-8")
            except Exception as e:  # noqa: BLE001
                body = json.dumps({"error": str(e)}).encode("utf-8")
            return self._send(200, body, "application/json; charset=utf-8")
        if path.startswith("/img/"):
            rel = path[len("/img/"):]
            f = (CFG["src_dir"] / rel).resolve()
            if not str(f).startswith(str(CFG["src_dir"].resolve())) or not f.exists():
                return self._send(404, b"not found", "text/plain")
            return self._send(200, f.read_bytes(), "image/jpeg")
        self._send(404, b"not found", "text/plain")


def main() -> None:
    ap = argparse.ArgumentParser(description="Pass-1 윈도우 실험 실시간 뷰어")
    ap.add_argument("--webtoon", type=int, default=23)
    ap.add_argument("--episodes", default="10")
    ap.add_argument("--source-hash", default="78c51d18a1d9")
    ap.add_argument("--out-dir", default=None, help="미지정 시 datasets/pass1_window의 단일 디렉터리 자동")
    ap.add_argument("--datasets", default=str(Path(__file__).resolve().parents[1] / "datasets"))
    ap.add_argument("--port", type=int, default=8791)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    datasets = Path(args.datasets)
    if args.out_dir:
        out_dir = Path(args.out_dir)
    else:
        subs = [Path(d) for d in glob.glob(str(datasets / "pass1_window" / "*")) if os.path.isdir(d)]
        if len(subs) != 1:
            raise SystemExit(f"--out-dir 지정 필요(후보 {[s.name for s in subs]})")
        out_dir = subs[0]

    eps = ([int(x) for x in args.episodes.split(",")] if "-" not in args.episodes
           else list(range(int(args.episodes.split("-")[0]), int(args.episodes.split("-")[1]) + 1)))
    CFG.update({"src_dir": datasets / "pass1" / args.source_hash, "out_dir": out_dir,
                "webtoon": args.webtoon, "episodes": eps})

    print(f"뷰어: http://{args.host}:{args.port}  (out={out_dir})")
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
