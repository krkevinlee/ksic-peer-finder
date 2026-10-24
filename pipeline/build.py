#!/usr/bin/env python3
"""업종 피어 파인더 데이터 빌드.

KRX KIND 상장법인목록 + DART(기업개황 업종코드, 정기보고서 재무) + KRX OpenAPI(일별 시가총액)를
모아 site/data/*.json 을 만든다. 호출 결과는 CACHE_DIR 에 쌓아 두고, 다음 실행에서는 바뀐 것만 다시 부른다.

환경변수: DART_API_KEY, KRX_API_KEY, CACHE_DIR(기본 ./cache)
사용: python pipeline/build.py [--budget 12000]
"""
import argparse, datetime as dt, io, json, os, re, sys, threading, time, urllib.parse, urllib.request, zipfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE = os.path.abspath(os.environ.get("CACHE_DIR") or os.path.join(ROOT, "cache"))
SITE_DATA = os.path.join(ROOT, "site", "data")
KST = dt.timezone(dt.timedelta(hours=9))
TODAY = dt.datetime.now(KST).date()
RC = {"Q1": "11013", "H1": "11012", "Q3": "11014", "FY": "11011"}
EOK = 1e8


def log(*a):
    print(f"[{dt.datetime.now(KST):%H:%M:%S}]", *a, flush=True)


# ---------------------------------------------------------------- cache / http
def load(name, default):
    p = os.path.join(CACHE, name)
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    return default


def save(name, obj):
    os.makedirs(CACHE, exist_ok=True)
    tmp = os.path.join(CACHE, name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    os.replace(tmp, os.path.join(CACHE, name))


def http(url, headers=None, tries=4):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", **(headers or {})})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001 - network flakiness; retried
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


class LimitReached(Exception):
    pass


class Dart:
    """OpenDART 호출. 실행당 호출 수(budget)와 초당 호출 수를 제한한다."""

    def __init__(self, key, budget, rate=8.0):
        self.key, self.left, self.gap = key, budget, 1.0 / rate
        self.lock, self.next_at, self.stopped, self.calls = threading.Lock(), 0.0, False, 0

    def __call__(self, api, **params):
        with self.lock:
            if self.stopped or self.left <= 0:
                raise LimitReached()
            self.left -= 1
            self.calls += 1
            wait = self.next_at - time.time()
            self.next_at = max(time.time(), self.next_at) + self.gap
        if wait > 0:
            time.sleep(wait)
        params["crtfc_key"] = self.key
        raw = http("https://opendart.fss.or.kr/api/" + api + "?" + urllib.parse.urlencode(params))
        if api.endswith(".xml"):
            return raw
        j = json.loads(raw)
        st = j.get("status")
        if st == "000":
            return j
        if st == "013":
            return None
        if st in ("020", "021", "800", "901"):  # 한도 초과, 점검 등
            self.stopped = True
            raise LimitReached(f"DART {st} {j.get('message')}")
        raise RuntimeError(f"DART {api} {st} {j.get('message')}")


# ---------------------------------------------------------------- sources
def kind_list():
    """KIND 상장법인목록 (유가/코스닥). 실패하면 캐시."""
    try:
        t = http("https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13").decode("euc-kr", "replace")
        rows = []
        for r in re.findall(r"<tr>(.*?)</tr>", t, re.S):
            c = [re.sub(r"<[^>]+>", "", x).strip() for x in re.findall(r"<td[^>]*>(.*?)</td>", r, re.S)]
            if len(c) >= 5 and c[1] in ("유가", "코스닥"):
                rows.append({"name": c[0], "mkt": "KOSPI" if c[1] == "유가" else "KOSDAQ", "ticker": c[2].zfill(6), "ind": c[3], "prod": c[4][:80]})
        if len(rows) > 2000:
            save("kind.json", {"date": TODAY.isoformat(), "rows": rows})
            return rows, TODAY.isoformat()
        log("KIND: unexpected row count", len(rows))
    except Exception as e:  # noqa: BLE001
        log("KIND fetch failed, using cache:", e)
    k = load("kind.json", {"date": "", "rows": []})
    return k["rows"], k["date"]


def corp_codes(dart):
    """DART 고유번호 전체 목록. 7일마다 갱신."""
    c = load("corps.json", None)
    if c and (TODAY - dt.date.fromisoformat(c["date"])).days < 7:
        return c["rows"]
    z = zipfile.ZipFile(io.BytesIO(dart("corpCode.xml")))
    root = ET.fromstring(z.read(z.namelist()[0]))
    rows = [[e.findtext("corp_code"), (e.findtext("corp_name") or "").strip(), (e.findtext("stock_code") or "").strip(), e.findtext("modify_date") or ""] for e in root.iter("list")]
    save("corps.json", {"date": TODAY.isoformat(), "rows": rows})
    log("corpCode rows", len(rows))
    return rows


def periodic_filings(dart):
    """최근 16개월 상장사 정기보고서 접수 목록. {corp_code: [[rcept_no, report_nm, rcept_dt], ...]}"""
    f = load("filings.json", {"done_to": "", "by": {}})
    start = TODAY - dt.timedelta(days=490)
    if f["done_to"]:
        start = max(start, dt.date.fromisoformat(f["done_to"]) - dt.timedelta(days=7))
    by = f["by"]
    cur = start
    while cur <= TODAY:
        end = min(cur + dt.timedelta(days=88), TODAY)
        for cls in ("Y", "K"):
            page = 1
            while True:
                j = dart("list.json", bgn_de=cur.strftime("%Y%m%d"), end_de=end.strftime("%Y%m%d"), corp_cls=cls, pblntf_ty="A", page_no=page, page_count=100, last_reprt_at="Y")
                if not j:
                    break
                for it in j["list"]:
                    lst = by.setdefault(it["corp_code"], [])
                    if not any(x[0] == it["rcept_no"] for x in lst):
                        lst.append([it["rcept_no"], it["report_nm"], it["rcept_dt"]])
                if page >= j["total_page"]:
                    break
                page += 1
        cur = end + dt.timedelta(days=1)
    cut = (TODAY - dt.timedelta(days=520)).strftime("%Y%m%d")
    by = {k: [x for x in v if x[2] >= cut] for k, v in by.items()}
    f = {"done_to": TODAY.isoformat(), "by": by}
    save("filings.json", f)
    return by


def unlisted_priority(dart):
    """비상장사 중 증권신고서(발행공시)·감사보고서 제출사 — 업종코드를 먼저 모을 대상."""
    f = load("unl_prio.json", {"done_to": "", "C": {}, "F": {}})
    for ty, days in (("C", 730), ("F", 400)):
        start = TODAY - dt.timedelta(days=days)
        if f["done_to"]:
            start = max(start, dt.date.fromisoformat(f["done_to"]) - dt.timedelta(days=3))
        cur = start
        while cur <= TODAY:
            end = min(cur + dt.timedelta(days=88), TODAY)
            page = 1
            while True:
                j = dart("list.json", bgn_de=cur.strftime("%Y%m%d"), end_de=end.strftime("%Y%m%d"), corp_cls="E", pblntf_ty=ty, page_no=page, page_count=100)
                if not j:
                    break
                for it in j["list"]:
                    f[ty][it["corp_code"]] = max(f[ty].get(it["corp_code"], ""), it["rcept_dt"])
                if page >= j["total_page"]:
                    break
                page += 1
            cur = end + dt.timedelta(days=1)
    f["done_to"] = TODAY.isoformat()
    save("unl_prio.json", f)
    return f


# ---------------------------------------------------------------- financials
def parse_report(nm, acc_mt):
    """'반기보고서 (2026.06)' -> ('H1', fiscal_year_label, period_end 'YYYY.MM')"""
    m = re.search(r"(사업|반기|분기)보고서\s*\((\d{4})\.(\d{2})\)", nm)
    if not m:
        return None
    kind, y, mo = m.group(1), int(m.group(2)), int(m.group(3))
    after = (mo - acc_mt) % 12  # 결산월 이후 경과 개월
    t = {"사업": "FY", "반기": "H1"}.get(kind) or ("Q1" if after == 3 else "Q3" if after == 9 else None)
    if not t:
        return None
    # 회계연도 = 결산월이 속한 해 (12월 결산이면 기간 말 연도)
    fy = y + (1 if (acc_mt != 12 and mo > acc_mt and t != "FY") else 0)
    return t, fy, f"{y}.{mo:02d}"


NEEDS = {  # 최신 보고서 종류 -> 필요한 (연도 오프셋, 보고서)
    "Q1": [(-1, "H1"), (-1, "Q3"), (-1, "FY"), (0, "Q1")],
    "H1": [(-1, "Q3"), (-1, "FY"), (0, "Q1"), (0, "H1")],
    "Q3": [(-1, "Q3"), (-1, "FY"), (0, "Q1"), (0, "H1"), (0, "Q3")],
    "FY": [(0, "Q1"), (0, "H1"), (0, "Q3"), (0, "FY")],
}
ORDER = {"Q1": 1, "H1": 2, "Q3": 3, "FY": 4}


def pick(rows, ids, names):
    for r in rows:
        if r["account_id"] in ids:
            return r
    for r in rows:
        if re.sub(r"[\s()]", "", r["account_nm"]) in names:
            return r
    return None


def num(s):
    try:
        return float(str(s).replace(",", "")) if s not in (None, "", "-") else None
    except ValueError:
        return None


def extract(lst):
    """fnlttSinglAcntAll list -> {'rev': [3m, cum], 'op': [...], 'ni': [...]}"""
    rows = [r for r in lst if r.get("sj_div") in ("IS", "CIS")]
    out = {}
    specs = {
        "rev": (("ifrs-full_Revenue", "ifrs_Revenue"), {"매출액", "수익매출액", "영업수익", "매출", "순매출액", "매출액영업수익", "영업수익매출액", "수익"}),
        "op": (("dart_OperatingIncomeLoss",), {"영업이익", "영업이익손실", "영업손익", "영업손실"}),
        "nip": (("ifrs-full_ProfitLossAttributableToOwnersOfParent",), {"지배기업소유주지분", "지배기업의소유주에게귀속되는당기순이익", "지배기업소유주지분순이익"}),
        "ni": (("ifrs-full_ProfitLoss",), {"당기순이익", "당기순이익손실", "당기순손익", "분기순이익", "반기순이익", "분기순이익손실", "반기순이익손실"}),
    }
    for k, (ids, names) in specs.items():
        r = pick(rows, ids, names)
        if r:
            a, c = num(r.get("thstrm_amount")), num(r.get("thstrm_add_amount"))
            out[k] = [a, c if c is not None else a]
    if "nip" in out:
        out["ni"] = out.pop("nip")
    return out


def fetch_report(dart, corp, years, t, fs_pref):
    """(fs, data) — data None 이면 해당 보고서 재무 없음. 12월 결산이 아니면 사업연도를 두 개 시도."""
    order = [fs_pref, "OFS" if fs_pref == "CFS" else "CFS"] if fs_pref else ["CFS", "OFS"]
    for fs in order:
        for y in years:
            j = dart("fnlttSinglAcntAll.json", corp_code=corp, bsns_year=str(y), reprt_code=RC[t], fs_div=fs)
            if j and j.get("list"):
                return fs, {"y": y, **extract(j["list"])}
    return fs_pref, None


def quarters(reps, latest_t, fy):
    """보고서들로 최근 4개 분기 [rev, op, ni] 계산."""
    def g(off, t):
        return reps.get(f"{fy + off}_{t}")

    def q3m(off, t, k):  # 해당 분기 3개월 값
        r = g(off, t)
        if not r or k not in r:
            return None
        if t == "Q1":
            return r[k][0] if r[k][0] is not None else r[k][1]
        if t in ("H1", "Q3"):
            return r[k][0]
        # FY: 연간 - 3분기 누적
        q3 = g(off, "Q3")
        if q3 and k in q3 and r[k][1] is not None and q3[k][1] is not None:
            return r[k][1] - q3[k][1]
        return None

    seq = {"Q1": [(-1, "H1"), (-1, "Q3"), (-1, "FY"), (0, "Q1")],
           "H1": [(-1, "Q3"), (-1, "FY"), (0, "Q1"), (0, "H1")],
           "Q3": [(-1, "FY"), (0, "Q1"), (0, "H1"), (0, "Q3")],
           "FY": [(0, "Q1"), (0, "H1"), (0, "Q3"), (0, "FY")]}[latest_t]
    lab = {"Q1": "1Q", "H1": "2Q", "Q3": "3Q", "FY": "4Q"}
    qs, labels = [], []
    for off, t in seq:
        qs.append([q3m(off, t, k) for k in ("rev", "op", "ni")])
        labels.append(f"{fy + off}.{lab[t]}")
    return qs, labels


def build_fin(dart, listed, ksic, filings):
    fin = load("fin.json", {})
    jobs = []
    for c in listed:
        cc = c["corp"]
        if not cc:
            continue
        acc = int((ksic.get(cc) or ["", "12"])[1] or 12)
        reps = []
        for rno, nm, rdt in filings.get(cc, []):
            p = parse_report(nm, acc)
            if p:
                reps.append((p[1], ORDER[p[0]], p[0], rno))
        if not reps:
            continue
        reps.sort()
        fy, _, lt, _ = reps[-1]
        filed = {f"{y}_{t}": rno for y, _, t, rno in reps}
        need = [(fy + off, t) for off, t in NEEDS[lt]]
        jobs.append((cc, fy, lt, need, filed, acc))

    lock = threading.Lock()

    def work(job):
        cc, fy, lt, need, filed, acc = job
        e = fin.get(cc) or {"fs": "", "r": {}}
        for y, t in need:
            key = f"{y}_{t}"
            want = filed.get(key)
            if not want:
                continue  # 접수된 적 없는 보고서 (상장 전 등)
            have = e["r"].get(key)
            if have is not None and have.get("frc") == want:
                continue  # 이미 같은 접수번호로 받아 둠
            years = [y] if acc == 12 else [y, y - 1]
            fs, data = fetch_report(dart, cc, years, t, e["fs"])
            if data:
                e["fs"] = fs
            e["r"][key] = {**(data or {"empty": 1}), "frc": want}
        e["lt"], e["fy"] = lt, fy
        with lock:
            fin[cc] = e

    done = 0
    try:
        with ThreadPoolExecutor(6) as ex:
            for _ in ex.map(work, jobs):
                done += 1
                if done % 200 == 0:
                    log(f"fin {done}/{len(jobs)} (DART calls {dart.calls})")
                    with lock:
                        save("fin.json", fin)
    except LimitReached as e:
        log("fin stopped:", e)
    save("fin.json", fin)
    return fin


# ---------------------------------------------------------------- KSIC (기업개황)
def build_ksic(dart, targets, reserve):
    ksic = load("ksic.json", {})  # corp_code -> [induty_code, acc_mt]
    todo = [cc for cc in targets if cc not in ksic]
    lock = threading.Lock()

    def work(cc):
        if dart.left <= reserve:
            raise LimitReached()
        j = dart("company.json", corp_code=cc)
        with lock:
            ksic[cc] = [str((j or {}).get("induty_code") or ""), str((j or {}).get("acc_mt") or "12")]

    n = 0
    try:
        with ThreadPoolExecutor(6) as ex:
            for _ in ex.map(work, todo):
                n += 1
                if n % 500 == 0:
                    log(f"ksic {n}/{len(todo)} (DART calls {dart.calls})")
                    with lock:
                        save("ksic.json", ksic)
    except LimitReached:
        log(f"ksic stopped at {n}/{len(todo)}")
    save("ksic.json", ksic)
    return ksic


# ---------------------------------------------------------------- KRX 시가총액
def build_mcap(key):
    m = load("mcap.json", {})  # 'YYYYMMDD' -> {ticker: 억원}
    start = TODAY - dt.timedelta(days=100)
    d = start
    while d < TODAY:
        ds = d.strftime("%Y%m%d")
        recent = (TODAY - d).days <= 4
        if d.weekday() < 5 and (ds not in m or (recent and not m[ds])):
            day = {}
            ok = True
            for path in ("stk_bydd_trd", "ksq_bydd_trd"):
                try:
                    j = json.loads(http(f"http://data-dbg.krx.co.kr/svc/apis/sto/{path}?basDd={ds}", {"AUTH_KEY": key, "Content-Type": "application/json"}))
                    for r in j.get("OutBlock_1") or []:
                        v = num(r.get("MKTCAP"))
                        if v:
                            day[r["ISU_CD"]] = round(v / EOK, 1)
                except Exception as e:  # noqa: BLE001
                    log("KRX", ds, path, e)
                    ok = False
            if ok:
                m[ds] = day
        d += dt.timedelta(days=1)
    for k in [k for k in m if k < start.strftime("%Y%m%d")]:
        del m[k]
    save("mcap.json", m)
    return m


def mcap_stats(m, ticker):
    days = sorted(k for k, v in m.items() if v)
    if not days:
        return None, None, None, ""
    last = days[-1]
    ld = dt.datetime.strptime(last, "%Y%m%d").date()

    def avg(months):
        y, mo = ld.year, ld.month - months
        while mo <= 0:
            y, mo = y - 1, mo + 12
        frm = ld.replace(year=y, month=mo, day=min(ld.day, 28)).strftime("%Y%m%d")
        vals = [m[d][ticker] for d in days if d > frm and ticker in m[d]]
        return round(sum(vals) / len(vals), 1) if vals else None

    return m[last].get(ticker), avg(1), avg(3), last


# ---------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=12000, help="이번 실행의 DART 호출 상한")
    ap.add_argument("--only", default="", help="테스트용: 쉼표로 구분한 종목코드만 처리 (비상장 수집 생략)")
    args = ap.parse_args()
    dkey, kkey = os.environ.get("DART_API_KEY"), os.environ.get("KRX_API_KEY")
    if not dkey or not kkey:
        sys.exit("DART_API_KEY / KRX_API_KEY 가 필요합니다")
    dart = Dart(dkey, args.budget)
    K = json.load(open(os.path.join(ROOT, "pipeline", "ksic11.json"), encoding="utf-8"))

    kind, kind_date = kind_list()
    log("KIND rows", len(kind), kind_date)
    corps = corp_codes(dart)
    by_stock = {r[2]: r for r in corps if r[2]}
    listed = []
    for k in kind:
        r = by_stock.get(k["ticker"])
        listed.append({**k, "corp": r[0] if r else ""})
    if args.only:
        listed = [c for c in listed if c["ticker"] in args.only.split(",")]

    # 1) 상장사 업종코드 (먼저), 2) 정기보고서 목록, 3) 재무, 4) 남은 예산으로 비상장 업종코드
    ksic = build_ksic(dart, [c["corp"] for c in listed if c["corp"]], reserve=0)
    try:
        filings = periodic_filings(dart)
    except LimitReached:
        filings = load("filings.json", {"by": {}})["by"]
    fin = build_fin(dart, listed, ksic, filings)
    try:
        prio = unlisted_priority(dart)
    except LimitReached:
        prio = load("unl_prio.json", {"C": {}, "F": {}})
    rank = lambda r: (r[0] not in prio["C"], r[0] not in prio["F"], "".join(chr(255 - ord(ch)) for ch in r[3]))
    unl = sorted((r for r in corps if not r[2]), key=rank)
    if not args.only:
        ksic = build_ksic(dart, [r[0] for r in unl], reserve=50)
    log("DART calls this run", dart.calls)

    m = build_mcap(kkey)

    # ---- outputs
    os.makedirs(SITE_DATA, exist_ok=True)
    rows, mdate = [], ""
    for c in listed:
        kc = (ksic.get(c["corp"]) or [""])[0]
        e = fin.get(c["corp"])
        q = labels = ltm = None
        fs = ""
        if e and e.get("lt"):
            reps = {k: v for k, v in e["r"].items() if v and not v.get("empty")}
            q, labels = quarters(reps, e["lt"], e["fy"])
            ltm = [round(sum(x[i] for x in q) / EOK, 1) if all(x[i] is not None for x in q) else None for i in range(3)]
            q = [[None if v is None else round(v / EOK, 1) for v in x] for x in q]
            fs = {"CFS": "연결", "OFS": "별도"}.get(e.get("fs"), "")
        m0, m1, m3, mdate_c = mcap_stats(m, c["ticker"])
        mdate = mdate or mdate_c
        mins = [x for x in (m0, m1, m3) if x]
        per = round(min(mins) / ltm[2], 1) if (mins and ltm and ltm[2] and ltm[2] > 0) else None
        rows.append([c["name"], c["mkt"], c["ticker"], kc, c["ind"], c["prod"], c["corp"], fs, labels[-1] if labels else "", q, ltm, [m0, m1, m3], per])
    unlisted = [[r[1], r[0], (ksic.get(r[0]) or [""])[0]] for r in corps if not r[2]]
    meta = {"built": dt.datetime.now(KST).strftime("%Y-%m-%d %H:%M"), "kind": kind_date, "mcap": mdate,
            "ksic_unlisted_done": sum(1 for r in unlisted if r[1] in ksic), "unlisted_total": len(unlisted)}
    with open(os.path.join(SITE_DATA, "listed.json"), "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "k": K, "c": rows}, f, ensure_ascii=False, separators=(",", ":"))
    with open(os.path.join(SITE_DATA, "unlisted.json"), "w", encoding="utf-8") as f:
        json.dump(unlisted, f, ensure_ascii=False, separators=(",", ":"))
    log("done", meta)


if __name__ == "__main__":
    main()
