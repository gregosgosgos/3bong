# crawler.py
import os, re, json, asyncio
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, urljoin
from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PwTimeoutError
import gspread
from google.oauth2.service_account import Credentials

# ======================
# 환경설정 / 검증
# ======================
load_dotenv()
SITE_BASE = os.getenv("SITE_BASE", "https://3bong.kr").rstrip("/")
LOGIN_URL = os.getenv("LOGIN_URL", f"{SITE_BASE}/member/login.php")
USER_ID   = os.getenv("USER_ID")
USER_PW   = os.getenv("USER_PW")
SHEET_ID  = os.getenv("SHEET_ID")
SHEET_TAB = os.getenv("SHEET_TAB", "크롤링결과")

# 성능/재고 관련 옵션(.env)
ENABLE_STOCK = os.getenv("ENABLE_STOCK", "1") == "1"       # 0이면 재고 수집 스킵
STOCK_MODE = os.getenv("STOCK_MODE", "http")               # "http" (빠름) 또는 "dialog" (정확도↑)
STOCK_CONCURRENCY = int(os.getenv("STOCK_CONCURRENCY", "20"))  # 동시 요청 수(HTTP 모드)
STOCK_TIMEOUT_MS  = int(os.getenv("STOCK_TIMEOUT_MS", "2000"))  # 각 HTTP 요청 타임아웃(ms)
TREAT_SILENT_AS_ZERO = os.getenv("TREAT_SILENT_AS_ZERO", "1") == "1"  # dialog 모드에서 무반응+10000 유지시 0 기록
MAX_PAGES = int(os.getenv("MAX_PAGES", "0") or "0")        # 테스트용 페이지 제한(0=무제한)

required_keys = ["USER_ID", "USER_PW", "SHEET_ID"]
missing = [k for k in required_keys if not os.getenv(k)]
if missing:
    raise RuntimeError(f"환경변수 누락: {', '.join(missing)}. "
                       "로컬은 .env, GitHub Actions는 Secrets로 설정하세요.")

# ======================
# 카테고리
# ======================
CATE_CODES = {
    "과자/쿠키/스낵": "021013",
    "초콜릿류": "021005",
    "캔디(사탕)/카라멜": "021002",
    "젤리/껌/가루쿡": "021015",
    "건견과/어포/육포": "021004",
    "중국간식": "021012",
    "음료/푸딩": "021003",
}

# ======================
# 셀렉터
# ======================
CARD_SEL       = "div.item_cont"
NAME_SEL       = ".item_name"
PRICE_FALLBACK = ".item_price span"
DETAIL_LINKSEL = "a[href*='goods_view']"
CODE_ATTR_SEL  = "[data-goods-no]"
PHOTO_BOX_SEL  = ".item_photo_box"

# ======================
# 유틸
# ======================
def clean_price_text(txt: str | None) -> int | None:
    if not txt: return None
    m = re.sub(r"[^\d]", "", txt)
    return int(m) if m else None

def strip_brand_prefix(name: str) -> str:
    """
    맨 앞 '브랜드명)' 패턴만 제거 (중간 괄호는 유지)
    예) '오성) 레몬맛 샌드 227g' -> '레몬맛 샌드 227g'
        '삼진)바삭프레첼버터갈릭맛 45g' -> '바삭프레첼버터갈릭맛 45g'
    """
    if not name: return name
    m = re.match(r'^\s*([^\(\)\[\]]{1,30})\)\s*(.+)$', name)
    return m.group(2).strip() if m else name

def parse_name_pack_expiry(raw_name: str | None):
    """
    반환: (상품명, 묶음형태("타/박/개"), 묶음당수량(int), 유통기한("YY.MM.DD"))
    """
    if not raw_name:
        return None, None, None, None
    lines = [re.sub(r"\s+", " ", l.strip()) for l in raw_name.splitlines() if l.strip()]
    base_name = lines[0] if lines else raw_name.strip()
    base_name = strip_brand_prefix(base_name)
    tail_text = " ".join(lines[1:]) if len(lines) > 1 else ""

    unit = None; qty = None; expiry = None
    for m in re.finditer(r"\(([^)]*)\)", tail_text):
        inside = m.group(1)
        mu = re.search(r"(타|박|개)", inside)
        if mu: unit = mu.group(1)
        mq = re.search(r"(\d+)\s*개입", inside)
        if mq:
            try: qty = int(mq.group(1))
            except: qty = None
        if unit or (qty is not None):
            break
    md = re.search(r"(\d{2}\.\d{2}\.\d{2})", tail_text)
    if md: expiry = md.group(1)
    return base_name, unit, qty, expiry

def build_list_url(cate_code: str, page: int) -> str:
    base = f"{SITE_BASE}/goods/goods_list.php"
    parsed = urlparse(base)
    qs = {"cateCd": cate_code, "page": str(page)}
    return urlunparse(parsed._replace(query=urlencode(qs)))

def absolutize_img(url: str | None) -> str | None:
    # 이미지 쪽은 기존 로직 유지(이미 정상 동작)
    if not url: return None
    if url.startswith("http"): return url
    if url.startswith("//"):   return "https:" + url
    if url.startswith("/"):    return f"{SITE_BASE}{url}"
    return f"{SITE_BASE}/goods/{url.lstrip('./')}"

async def text_of(el):
    if not el: return None
    try:
        t = await el.inner_text()
        if t: return t
    except: pass
    try:
        return await el.text_content()
    except:
        return None

# ======================
# 로그인
# ======================
async def login(page):
    ID_BOX = ["input[name='m_id']", "#loginId", "input[name='loginId']"]
    PW_BOX = ["input[name='m_pwd']", "#loginPwd", "input[type='password']"]
    SUBMIT = ["button[type='submit']", "input[type='submit']", "#btnLogin"]

    await page.goto(LOGIN_URL, wait_until="domcontentloaded")
    id_el = pw_el = None
    for s in ID_BOX:
        try: id_el = await page.wait_for_selector(s, timeout=2000); break
        except: pass
    for s in PW_BOX:
        try: pw_el = await page.wait_for_selector(s, timeout=2000); break
        except: pass
    if not (id_el and pw_el):
        raise RuntimeError("로그인 입력칸을 못 찾았습니다.")
    await id_el.fill(USER_ID); await pw_el.fill(USER_PW)
    clicked = False
    for s in SUBMIT:
        btn = await page.query_selector(s)
        if btn: await btn.click(); clicked = True; break
    if not clicked: await page.keyboard.press("Enter")
    await page.wait_for_load_state("networkidle")

# ======================
# 페이지 수 추정
# ======================
async def get_max_page_on(page) -> int:
    last = await page.query_selector('a[aria-label="Last"], a.last, a[href*="page="]:has-text("끝")')
    if last:
        href = await last.get_attribute("href") or ""
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            try: return int(m.group(1))
            except: pass
    links = await page.query_selector_all('a[href*="page="]')
    mx = 1
    for a in links[:25]:
        h = await a.get_attribute("href") or ""
        m = re.search(r"[?&]page=(\d+)", h)
        if m:
            try:
                v = int(m.group(1))
                if v > mx: mx = v
            except: pass
    return mx

# ======================
# 품절 제외
# ======================
async def is_soldout(card) -> bool:
    try:
        li_has = await card.evaluate("(el) => el.closest('li')?.classList?.contains('item_soldout') ?? false")
        if li_has: return True
    except: pass
    if await card.query_selector("strong.item_soldout_bg"):
        return True
    return False

# ======================
# 썸네일
# ======================
async def extract_thumbnail(card) -> str | None:
    box = await card.query_selector(PHOTO_BOX_SEL)
    if not box:
        img = await card.query_selector("img[data-original], img[src]")
        if img:
            for attr in ("data-original", "src"):
                v = await img.get_attribute(attr)
                if v: return absolutize_img(v)
        return None
    for attr in ("data-image-list", "data-image-main", "data-image-detail"):
        v = await box.get_attribute(attr)
        if v: return absolutize_img(v)
    img = await box.query_selector("img[data-original], img[src]")
    if img:
        for attr in ("data-original", "src"):
            v = await img.get_attribute(attr)
            if v: return absolutize_img(v)
    return None

# ======================
# 판매가/판매수량 선택 (마진 10~20%)
# ======================
SALE_CANDIDATES = [1900, 2900, 3900]
FEE_RATE = 0.22
BAND_MIN = 0.10
BAND_MAX = 0.20

def choose_sale_combo(unit_cost: float | None):
    if unit_cost is None or unit_cost <= 0:
        return None, None, None
    feasible = []
    for sp in SALE_CANDIDATES:
        net = sp * (1 - FEE_RATE)
        if net <= 0: continue
        q_max = int((1 - BAND_MIN) * net // unit_cost)  # floor(0.9*net/unit_cost)
        if q_max >= 1:
            margin = 1 - (q_max * unit_cost) / net
            feasible.append((sp, q_max, margin))
    if not feasible:
        return None, None, None
    in_band = [r for r in feasible if BAND_MIN <= r[2] <= BAND_MAX]
    if in_band:
        in_band.sort(key=lambda x: (-x[2], x[0]))    # 마진 내림차순, 동률이면 낮은 판매가
        return in_band[0]
    above = [r for r in feasible if r[2] > BAND_MAX]
    if above:
        above.sort(key=lambda x: (x[2] - BAND_MAX, x[0]))  # 초과폭 최소, 동률이면 낮은 판매가
        return above[0]
    return None, None, None

# ======================
# 카드 → 기초 행 (재고 제외)
# ======================
async def parse_card_to_row_base(page, card, cate_name: str, current_url: str):
    if await is_soldout(card):
        return None

    raw_name = await text_of(await card.query_selector(NAME_SEL))
    prod_name, pack_unit, pack_qty, expiry = parse_name_pack_expiry(raw_name)

    bundle_price = None
    holder = await card.query_selector("[data-goods-price]")
    if holder:
        raw = await holder.get_attribute("data-goods-price")
        if raw:
            try: bundle_price = int(round(float(raw)))
            except: bundle_price = None
    if bundle_price is None:
        txt = await text_of(await card.query_selector(PRICE_FALLBACK))
        bundle_price = clean_price_text(txt)

    # 상세 링크(상세만 urljoin으로 정확히)
    full = None; code = None
    link = await card.query_selector(DETAIL_LINKSEL)
    if link:
        href = await link.get_attribute("href")
        if href:
            full = urljoin(current_url, href)   # ✅ 핵심: goods/goods/goods_view.php 방지
            qs = parse_qs(urlparse(full).query)
            code = qs.get("goodsNo", [None])[0]
    if not code:
        holder2 = await card.query_selector(CODE_ATTR_SEL)
        if holder2:
            code = await holder2.get_attribute("data-goods-no")

    thumb = await extract_thumbnail(card)

    unit_cost = None
    if bundle_price is not None and pack_qty:
        unit_cost = round(bundle_price / pack_qty)

    sell_price, sell_qty, margin = choose_sale_combo(unit_cost)

    if not prod_name:
        return None

    return {
        "카테고리": cate_name,
        "상품명": prod_name,
        "유통기한": expiry,
        "묶음당수량": pack_qty,
        "묶음단가": bundle_price,
        "개당단가": unit_cost,
        "재고수량": None,          # <- 이후 보강
        "판매수량": sell_qty,
        "판매가": sell_price,
        "마진율": round(margin, 4) if margin is not None else None,
        "URL": full,
        "이미지URL": thumb,
    }

# ======================
# 카테고리 크롤
# ======================
async def crawl_category(page, cate_name: str, cate_code: str):
    rows = []
    first = build_list_url(cate_code, 1)
    await page.goto(first, wait_until="domcontentloaded")
    max_page = await get_max_page_on(page)
    if max_page < 1: max_page = 1
    if MAX_PAGES and max_page > MAX_PAGES:
        max_page = MAX_PAGES
    print(f"[{cate_name}] 최대 {max_page}페이지 추정")

    for p in range(1, max_page + 1):
        url = build_list_url(cate_code, p)
        await page.goto(url, wait_until="domcontentloaded")
        cards = await page.query_selector_all(CARD_SEL)
        print(f"[{cate_name}] {p}페이지 카드수: {len(cards)}")
        if not cards:
            break
        for c in cards:
            row = await parse_card_to_row_base(page, c, cate_name, url)
            if row: rows.append(row)
    return rows

# ======================
# 재고수량 (HTTP 모드: 빠름)
# ======================
async def fetch_stock_http(request_ctx, url: str) -> int | None:
    # 짧은 타임아웃 + 1회 재시도
    async def _once():
        try:
            resp = await request_ctx.get(url, timeout=STOCK_TIMEOUT_MS)
            if not resp.ok:
                return None
            html = await resp.text()
            stocks = [int(s.replace(",", "")) for s in re.findall(r'data-stock\s*=\s*["\']?([\d,]+)', html)]
            return max(stocks) if stocks else None
        except:
            return None
    v = await _once()
    if v is not None:
        return v
    return await _once()

# ======================
# 재고수량 (dialog 모드: 상세 열어 10000 입력→경고 파싱, 정확도↑)
# ======================
async def fetch_stock_from_detail(page, goods_url: str) -> int | None:
    """
    우선순위:
    1) DOM의 input[data-stock] (최우선)
    2) 수량칸에 10000 입력 → dialog/페이지 텍스트: "최대수량은 ####이하"
    3) 이벤트 후 다시 data-stock 재확인
    4) (옵션) 무반응+10000 유지면 0
    """
    tmp = await page.context.new_page()
    try:
        tmp.set_default_timeout(9000)
        await tmp.goto(goods_url, wait_until="domcontentloaded", timeout=9000)

        async def read_dom_stocks():
            try:
                vals = await tmp.evaluate("""
                    () => Array.from(document.querySelectorAll('input[data-stock]'))
                               .map(el => el.getAttribute('data-stock'))
                """)
                found = []
                for v in vals or []:
                    if v:
                        n = re.sub(r"[^\d]", "", v)
                        if n.isdigit():
                            found.append(int(n))
                return max(found) if found else None
            except:
                return None

        # 1) 먼저 data-stock 바로 읽기
        v0 = await read_dom_stocks()
        if v0 is not None:
            return v0

        # 수량 input 후보
        selectors = [
            "input[name='goodsCnt[]']","input[name='goodsCnt']","input[name^='goodsCnt']",
            "[id*='option_display'] input[type='text']",".goods_qty input[type='text']",
            "input.text.goodsCnt_0","input.text","input[type='text']"
        ]
        qty = None
        for sel in selectors:
            try:
                await tmp.wait_for_selector(sel, timeout=2500)
                els = await tmp.query_selector_all(sel)
                if not els: continue
                ranked = []
                for el in els:
                    name = (await el.get_attribute("name")) or ""
                    ds   = await el.get_attribute("data-stock")
                    rank = (1 if "goodsCnt" in name else 0) + (1 if ds else 0)
                    ranked.append((rank, el))
                ranked.sort(key=lambda x: -x[0])
                qty = ranked[0][1]
                break
            except PwTimeoutError:
                continue
        if not qty:
            return None

        # dialog 캡처
        dialog_msg = {"text": None}
        def _on_dialog(d):
            dialog_msg["text"] = d.message
            asyncio.create_task(d.dismiss())
        tmp.on("dialog", _on_dialog)

        # 10000 입력 + 이벤트 트리거
        await qty.click()
        await qty.fill("")
        await qty.type("10000", delay=5)
        await tmp.evaluate("""
            (el) => {
                el.dispatchEvent(new Event('input', {bubbles:true}));
                el.dispatchEvent(new Event('change',{bubbles:true}));
            }
        """, qty)
        await tmp.evaluate("el => el.blur()", qty)
        try: await qty.press("Enter")
        except: pass
        await tmp.wait_for_timeout(1200)

        # 2) dialog 우선
        if dialog_msg["text"]:
            m = re.search(r"최대수량은\s*([\d,]+)\s*이하", dialog_msg["text"])
            if m: return int(m.group(1).replace(",", ""))

        # 2-보조) 페이지 내 텍스트
        for sel in ["text=최대수량은","div:has-text('최대수량은')",
                    "[class*='alert']:has-text('최대수량은')",
                    "#option_display_item_0 :has-text('최대수량은')"]:
            try:
                el = await tmp.wait_for_selector(sel, timeout=1200)
                if el:
                    t = await el.text_content() or ""
                    m = re.search(r"최대수량은\s*([\d,]+)\s*이하", t)
                    if m: return int(m.group(1).replace(",", ""))
            except PwTimeoutError:
                continue

        # 3) 이벤트 후 다시 data-stock 재확인
        v1 = await read_dom_stocks()
        if v1 is not None:
            return v1

        # 4) 완전 무반응 & 값이 10000이면 (옵션) 0
        try:
            val = await qty.input_value()
            if val and re.sub(r"[^\d]", "", val) == "10000":
                return 0 if TREAT_SILENT_AS_ZERO else None
        except:
            pass
        return None
    finally:
        await tmp.close()

# ======================
# 재고 병렬 보강 (모드 스위치 지원)
# ======================
async def enrich_stocks_concurrently(context, rows: list[dict]):
    if not ENABLE_STOCK:
        return rows

    targets = [(i, r["URL"]) for i, r in enumerate(rows) if r.get("URL")]
    if not targets:
        return rows

    # dialog 모드는 상세 탭을 열어야 해서 과도한 병렬은 비추천(4~6 정도 권장)
    concurrency = STOCK_CONCURRENCY if STOCK_MODE == "http" else min(6, max(1, STOCK_CONCURRENCY))
    sem = asyncio.Semaphore(max(1, concurrency))

    async def worker_http(idx: int, url: str):
        async with sem:
            v = await fetch_stock_http(context.request, url)
            return idx, v

    async def worker_dialog(idx: int, url: str):
        async with sem:
            # context.pages[0]는 로그인된 메인 페이지. 상세 탭은 함수 내부에서 열고 닫음.
            page0 = context.pages[0]
            v = await fetch_stock_from_detail(page0, url)
            return idx, v

    tasks = [
        asyncio.create_task(
            worker_http(i, u) if STOCK_MODE == "http" else worker_dialog(i, u)
        ) for i, u in targets
    ]

    for fut in asyncio.as_completed(tasks):
        idx, stock = await fut
        rows[idx]["재고수량"] = stock
    return rows

# ======================
# Google Sheets
# ======================
def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    json_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    if json_str:
        info = json.loads(json_str)
        creds = Credentials.from_service_account_info(info, scopes=scopes)
    elif file_path:
        creds = Credentials.from_service_account_file(file_path, scopes=scopes)
    else:
        raise RuntimeError("서비스 계정 키가 없습니다. GOOGLE_SERVICE_ACCOUNT_JSON 또는 GOOGLE_SERVICE_ACCOUNT_FILE을 설정하세요.")
    return gspread.authorize(creds)

def upload_df_to_sheet(df: pd.DataFrame, sheet_id: str, tab_name: str):
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="100", cols="20")

    cols = ["카테고리","상품명","유통기한","묶음당수량","묶음단가","개당단가",
            "재고수량","판매수량","판매가","마진율","URL","이미지URL"]
    df = df.reindex(columns=cols)

    ws.clear()
    values = [cols] + df.astype(object).where(pd.notna(df), "").values.tolist()
    ws.update(range_name="A1", values=values, value_input_option="USER_ENTERED")

# ======================
# 메인
# ======================
async def main():
    print("🚀 crawler start")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

        # 리스트 페이지 빨리: 이미지/미디어/폰트 차단
        async def route_intercept(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return await route.abort()
            return await route.continue_()
        await ctx.route("**/*", route_intercept)
        ctx.set_default_timeout(6000)

        page = await ctx.new_page()
        print("🔑 try login...")
        await login(page)
        print("✅ login ok")

        # 1) 목록 수집 (재고 제외)
        all_rows = []
        for name, code in CATE_CODES.items():
            items = await crawl_category(page, name, code)
            print(f"[완료] {name}({code}) -> {len(items)}개")
            all_rows += items

        # 2) 재고 보강
        if ENABLE_STOCK:
            mode = "HTTP" if STOCK_MODE == "http" else "DIALOG"
            print(f"🔎 재고 수집 시작 (모드 {mode}, 동시 {STOCK_CONCURRENCY})...")
            all_rows = await enrich_stocks_concurrently(ctx, all_rows)

        df = pd.DataFrame(all_rows).drop_duplicates().reset_index(drop=True)
        print(f"총 행수: {len(df)}")

        # 로컬 백업
        df.to_csv("3bong_products.csv", index=False, encoding="utf-8-sig")

        # 시트 업로드
        upload_df_to_sheet(df, SHEET_ID, SHEET_TAB)
        print(f"✅ 구글 시트 업로드 완료: sheet={SHEET_ID}, tab={SHEET_TAB}")

        await ctx.close(); await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
