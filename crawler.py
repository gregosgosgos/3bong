# crawler.py
import os, re, json, asyncio
import pandas as pd
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv
from playwright.async_api import async_playwright

# === Google Sheets ===
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

if not USER_ID or not USER_PW:
    raise RuntimeError(".env에 USER_ID, USER_PW를 채워주세요.")
if not SHEET_ID:
    raise RuntimeError(".env에 SHEET_ID를 넣어주세요. (해당 시트를 서비스계정 이메일로 편집자 공유 필수)")

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
    문자열 맨 앞 '브랜드명)' 접두어가 있으면 제거.
    예) '오성) 레몬맛 샌드 227g' -> '레몬맛 샌드 227g'
        '삼진)바삭프레첼버터갈릭맛 45g' -> '바삭프레첼버터갈릭맛 45g'
    (중간 괄호는 건드리지 않음)
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
    base_name = strip_brand_prefix(base_name)  # 제조사 접두어 제거
    tail_text = " ".join(lines[1:]) if len(lines) > 1 else ""

    unit = None; qty = None; expiry = None
    # ( ... ) 블록에서 타/박/개 + n개입
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
    # YY.MM.DD
    md = re.search(r"(\d{2}\.\d{2}\.\d{2})", tail_text)
    if md: expiry = md.group(1)
    return base_name, unit, qty, expiry

def build_list_url(cate_code: str, page: int) -> str:
    base = f"{SITE_BASE}/goods/goods_list.php"
    parsed = urlparse(base)
    qs = {"cateCd": cate_code, "page": str(page)}
    return urlunparse(parsed._replace(query=urlencode(qs)))

def absolutize(url: str | None) -> str | None:
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
# 페이지 수 추정(가볍게)
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
# 품절 판정(가볍게)
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
# 썸네일 추출
# ======================
async def extract_thumbnail(card) -> str | None:
    box = await card.query_selector(PHOTO_BOX_SEL)
    if not box:
        img = await card.query_selector("img[data-original], img[src]")
        if img:
            for attr in ("data-original", "src"):
                v = await img.get_attribute(attr)
                if v: return absolutize(v)
        return None
    for attr in ("data-image-list", "data-image-main", "data-image-detail"):
        v = await box.get_attribute(attr)
        if v: return absolutize(v)
    img = await box.query_selector("img[data-original], img[src]")
    if img:
        for attr in ("data-original", "src"):
            v = await img.get_attribute(attr)
            if v: return absolutize(v)
    return None

# ======================
# 판매가/판매수량 선택
# - 10~20% 범위 내 '마진율 최대' 우선
# - 없으면 20% 초과 중 '20%에 가장 가까운(초과 최소)' 선택
# - 그래도 없으면 공란
# ======================
SALE_CANDIDATES = [1900, 2900, 3900]
FEE_RATE = 0.22
BAND_MIN = 0.10   # 하한 10%
BAND_MAX = 0.20

def choose_sale_combo(unit_cost: float | None):
    """
    unit_cost: 개당단가
    반환: (판매가, 판매수량, 마진율) 혹은 (None, None, None)
    """
    if unit_cost is None or unit_cost <= 0:
        return None, None, None

    feasible = []
    for sp in SALE_CANDIDATES:
        net = sp * (1 - FEE_RATE)
        if net <= 0:
            continue
        # margin >= 0.10 만족하는 최대 수량
        q_max = int((1 - BAND_MIN) * net // unit_cost)  # floor(0.9*net/unit_cost)
        if q_max >= 1:
            margin = 1 - (q_max * unit_cost) / net
            feasible.append((sp, q_max, margin))

    if not feasible:
        return None, None, None

    in_band = [r for r in feasible if BAND_MIN <= r[2] <= BAND_MAX]
    if in_band:
        in_band.sort(key=lambda x: (-x[2], x[0]))  # 마진 내림차순, 동률이면 낮은 판매가
        sp, q, m = in_band[0]
        return sp, q, m

    above = [r for r in feasible if r[2] > BAND_MAX]
    if above:
        above.sort(key=lambda x: (x[2] - BAND_MAX, x[0]))  # 초과폭 최소, 동률이면 낮은 판매가
        sp, q, m = above[0]
        return sp, q, m

    return None, None, None  # 모두 10% 미만이면 공란

# ======================
# 상세페이지에서 재고수량(data-stock) 추출
# ======================
async def fetch_stock_from_detail(page, goods_url: str) -> int | None:
    """
    상품 상세 페이지 HTML 내 input[name='goodsCnt[]']의 data-stock에서 재고수량 추출
    옵션 인풋이 여러 개면 최댓값을 사용 (정책 변경 가능)
    """
    resp = await page.request.get(goods_url)
    html = await resp.text()
    stocks = [int(s.replace(",", "")) for s in re.findall(r'data-stock="([\d,]+)"', html)]
    if stocks:
        return max(stocks)   # 합계로 바꾸려면 sum(stocks)
    return None

# ======================
# 카드 → 행
# ======================
async def parse_card_to_row(page, card, cate_name: str, current_url: str):
    if await is_soldout(card):
        return None

    # 이름/묶음/유통기한
    raw_name = await text_of(await card.query_selector(NAME_SEL))
    prod_name, pack_unit, pack_qty, expiry = parse_name_pack_expiry(raw_name)

    # 묶음단가(총액)
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

    # 상세 링크/코드
    full = None; code = None
    link = await card.query_selector(DETAIL_LINKSEL)
    if link:
        href = await link.get_attribute("href")
        if href:
            full = absolutize(href)
            qs = parse_qs(urlparse(full).query)
            code = qs.get("goodsNo", [None])[0]
    if not code:
        holder2 = await card.query_selector(CODE_ATTR_SEL)
        if holder2:
            code = await holder2.get_attribute("data-goods-no")

    # 이미지
    thumb = await extract_thumbnail(card)

    # 개당단가
    unit_cost = None
    if bundle_price is not None and pack_qty:
        unit_cost = round(bundle_price / pack_qty)

    # 판매가/판매수량/마진율
    sell_price, sell_qty, margin = choose_sale_combo(unit_cost)

    # 재고수량 (상세페이지 data-stock)
    stock_qty = None
    if full:
        stock_qty = await fetch_stock_from_detail(page, full)

    if not prod_name:
        return None

    # === 최종 행 (컬럼 순서 반영: 개당단가 → 재고수량 → 판매수량) ===
    return {
        "카테고리": cate_name,
        "상품명": prod_name,
        "유통기한": expiry,
        "묶음당수량": pack_qty,
        "묶음단가": bundle_price,
        "개당단가": unit_cost,
        "재고수량": stock_qty,              # ← 여기
        "판매수량": sell_qty,
        "판매가": sell_price,
        "마진율": round(margin, 4) if margin is not None else None,
        "URL": full,
        "이미지URL": thumb,
    }

# ======================
# 크롤링 루틴
# ======================
async def crawl_category(page, cate_name: str, cate_code: str):
    rows = []
    first = build_list_url(cate_code, 1)
    await page.goto(first, wait_until="domcontentloaded")
    max_page = await get_max_page_on(page)
    if max_page < 1: max_page = 1
    print(f"[{cate_name}] 최대 {max_page}페이지 추정")

    for p in range(1, max_page + 1):
        url = build_list_url(cate_code, p)
        await page.goto(url, wait_until="domcontentloaded")
        cards = await page.query_selector_all(CARD_SEL)
        print(f"[{cate_name}] {p}페이지 카드수: {len(cards)}")

        if not cards:
            break
        for c in cards:
            row = await parse_card_to_row(page, c, cate_name, url)
            if row: rows.append(row)
        await page.wait_for_timeout(20)
    return rows

# ======================
# Sheets 업로드
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

    # 저장 컬럼 순서 강제 (재고수량을 개당단가와 판매수량 사이)
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
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context()

        # 이미지/폰트/미디어만 차단 (stylesheet 허용: DOM 안정성/속도 균형)
        async def route_intercept(route):
            rt = route.request.resource_type
            if rt in {"image", "media", "font"}:
                return await route.abort()
            return await route.continue_()
        await ctx.route("**/*", route_intercept)
        ctx.set_default_timeout(5000)

        page = await ctx.new_page()
        await login(page)

        all_rows = []
        for name, code in CATE_CODES.items():
            items = await crawl_category(page, name, code)
            print(f"[완료] {name}({code}) -> {len(items)}개")
            all_rows += items

        df = pd.DataFrame(all_rows).drop_duplicates().reset_index(drop=True)
        print(f"총 행수: {len(df)}")

        # 로컬 CSV 백업(선택)
        df.to_csv("3bong_products.csv", index=False, encoding="utf-8-sig")

        # 시트 업로드
        upload_df_to_sheet(df, SHEET_ID, SHEET_TAB)
        print(f"✅ 구글 시트 업로드 완료: sheet={SHEET_ID}, tab={SHEET_TAB}")

        await ctx.close(); await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
