# app.py — Ký quỹ & Giá phải chạm (60/50/40%) — KHÔNG quyền chọn
# Upload: POS.xlsx + MGM.xlsx → đọc đúng mã SP, thống kê, tính giá phải chạm; GIỮ đầy đủ nhóm KIM LOẠI
# Thêm: LỊCH ĐÁO HẠN NHÚNG + so khớp Mã HĐ + cảnh báo FND + lưu ICS/CSV nhắc lịch
# Sửa: bỏ infer_datetime_format (deprecated), tạo exp_df trước khi dùng trong TAB "📅 Đáo hạn HĐ mở"

import os, re, unicodedata, uuid
from io import StringIO
import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# -------------------- CẤU HÌNH TRANG --------------------
st.set_page_config(page_title="Ký quỹ & Giá phải chạm (60/50/40%) — no options", layout="wide")
st.title("Ký quỹ & Giá TT cần chạm (60/50/40%) — Không quyền chọn, ĐỦ nhóm Kim loại + Nhắc FND")

# -------------------- HÀM TIỆN ÍCH CHUNG --------------------
DEFAULT_PREFIX = "068C"
MARGIN_TARGETS = [60, 50, 40]
MONTH_LETTERS = set("FGHJKMNQUVXZ")  # chữ tháng future

def _fix_cyrillic_like(s: str) -> str:
    """Chuẩn hóa sàn: thay ký tự Cyrillic trông giống Latin."""
    if not isinstance(s, str):
        return s
    s2 = s.strip()
    s2 = (s2
          .replace("СВОТ", "CBOT")
          .replace("Свот", "CBOT")
          .replace("Свот".upper(), "CBOT"))
    return s2

def _parse_ddmmyyyy(s):
    # pandas mới không cần infer_datetime_format
    return pd.to_datetime(s, errors="coerce", dayfirst=True)

def _strip_accents(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s).replace("đ","d").replace("Đ","D")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^0-9a-zA-Z]+", " ", s)
    return re.sub(r"\s+", " ", s).strip().lower()

def _norm_cols(df: pd.DataFrame):
    return {c: _strip_accents(c) for c in df.columns}

def _find_col(df: pd.DataFrame, candidates, required=False, default=None):
    norm_map = _norm_cols(df)
    inv = {v: k for k, v in norm_map.items()}
    for cand in candidates:
        key = _strip_accents(cand)
        if key in inv:
            return inv[key]
    if required:
        raise ValueError(f"Thiếu cột bắt buộc: {candidates} — cột có: {list(norm_map.values())}")
    return default

def _to_num(x):
    if isinstance(x, (int, float)) or pd.isna(x): return x
    s = str(x).replace(",", "").replace(" ", "").replace("\u00a0","")
    try: return float(s)
    except: return np.nan

def _num_col(df, col, fill=None):
    s = pd.to_numeric(df[col].map(_to_num), errors="coerce") if (col and col in df.columns) else pd.Series(np.nan, index=df.index)
    if fill is not None: s = s.fillna(fill)
    return s

def _safe_num(s):
    return pd.to_numeric(s, errors="coerce").fillna(0.0)

def _looks_like_account(q):
    Q = str(q).strip().upper()
    return bool(re.search(r"[0-9A-Z]", Q)) or Q.endswith("-A")

def _full_acc_from_suffix(suffix):
    s = str(suffix).strip().upper()
    if s.startswith(DEFAULT_PREFIX): return s
    m = re.fullmatch(r"(\d{4,})(-A)?", s)
    return f"{DEFAULT_PREFIX}{m.group(1)}{m.group(2) or ''}" if m else s

def _resolve_account(q, acc_df, col_key):
    Q = str(q).strip().upper()
    hit = acc_df[acc_df[col_key].astype(str).str.upper() == Q]
    if not hit.empty: return hit
    Q2 = _full_acc_from_suffix(Q)
    hit = acc_df[acc_df[col_key].astype(str).str.upper() == Q2]
    if not hit.empty: return hit
    suf = acc_df[acc_df[col_key].astype(str).str.upper().str.endswith(Q)]
    if suf.empty and Q2 != Q:
        suf = acc_df[acc_df[col_key].astype(str).str.upper().str.endswith(Q2)]
    return suf

def extract_raw_token(ma_hd: str) -> str:
    """
    Tách token thô từ 'Mã HĐ':
    - Nếu chuỗi kết thúc bằng 2 số: cắt 2 số năm, lấy phần trước (PL1NYF26 -> PL1NYF; XWH26 -> XWH)
    - Nếu không: lấy block chữ+số đầu tiên.
    - Trả về chữ hoa, không khoảng trắng.
    """
    s = (ma_hd or "").strip().upper()
    s = re.sub(r"\s+", "", s)
    m = re.match(r"^([A-Z0-9\.]+?)(\d{2})$", s)
    if m:
        return m.group(1)
    m2 = re.match(r"^([A-Z0-9\.]+)", s)
    return (m2.group(1) if m2 else "").strip()

def to_base_code(raw_token: str) -> str:
    """
    Base code = bỏ 1 chữ tháng cuối (F,G,H,J,K,M,N,Q,U,V,X,Z) nếu có.
    Ví dụ: PL1NYF -> PL1NY; XWH -> XW; SI5COZ -> SI5CO; ZCEZ -> ZCE; SILZ -> SIL
    """
    t = (raw_token or "").strip().upper()
    return t[:-1] if (len(t)>=3 and t[-1] in MONTH_LETTERS) else t

def looks_like_option_str(s: str) -> bool:
    s = (s or "").strip().upper()
    return s.startswith(("C.","P.")) or re.match(r"^(C|P)[\.\-_/ ]", s) is not None

def _read_best_sheet(file, required_keys_like):
    xl = pd.ExcelFile(file)
    best = (None, None, None, -1)
    for s in xl.sheet_names:
        for h in range(0, 6):
            try:
                df = xl.parse(s, header=h)
            except Exception:
                continue
            norm = set(_norm_cols(df).values())
            score = sum(1 for keys in required_keys_like for k in keys if _strip_accents(k) in norm)
            if score > best[3]:
                best = (df, s, h, score)
    if best[0] is None:
        raise ValueError("Không đọc được sheet nào hợp lệ.")
    return best[0], best[1], best[2]

try:
    from pandas.io.formats.style import Styler
except Exception:
    Styler = object  # type: ignore

def style_positions(df: pd.DataFrame) -> Styler:
    cols = df.columns
    def _color_row(r: pd.Series):
        has_pos = ("NetQty" in r.index) and pd.notna(r["NetQty"]) and abs(float(r["NetQty"])) > 0
        css = "background-color: #fff7ed;" if has_pos else ""
        return [css] * len(cols)
    sty = df.style.apply(_color_row, axis=1)
    for t in (60, 50, 40):
        c = f"Delta_to_{t}_%"
        if c in df.columns:
            sty = sty.background_gradient(subset=[c], cmap="RdYlGn_r")
    return sty

# -------------------- LỊCH ĐÁO HẠN NHÚNG --------------------
SCHED_RAW = r"""STT	Tên hợp đồng	Mã hợp đồng	Nhóm hàng hóa	Sở giao dịch	Ngày thông báo đầu tiên	Ngày giao dịch cuối cùng
1	Dầu đậu tương 7/25	ZLEN25	Nông sản	СВОТ	30/06/2025	14/07/2025
2	Dầu đậu tương 8/25	ZLEQ25	Nông sản	СВОТ	31/07/2025	14/08/2025
3	Dầu đậu tương 9/25	ZLEU25	Nông sản	CBOT	29/08/2025	12/09/2025
4	Dầu đậu tương 10/25	ZLEV25	Nông sản	СВОТ	30/09/2025	14/10/2025
5	Dầu đậu tương 12/25	ZLEZ25	Nông sản	СВОТ	28/11/2025	12/12/2025
6	Dầu đậu tương 1/26	ZLEF26	Nông sản	СВОТ	31/12/2025	14/01/2026
7	Dầu đậu tương micro 8/25	MZLQ25	Nông sản	СВОТ	25/07/2025	25/07/2025
8	Dầu đậu tương micro 9/25	MZLU25	Nông sản	СВОТ	22/08/2025	22/08/2025
9	Dầu đậu tương micro 10/25	MZLV25	Nông sản	СВОТ	26/09/2025	26/09/2025
10	Dầu đậu tương micro 12/25	MZLZ25	Nông sản	СВОТ	21/11/2025	21/11/2025
11	Dầu đậu tương micro 1/26	MZLF26	Nông sản	СВОТ	26/12/2025	26/12/2025
12	Đậu tương 7/25	ZSEN25	Nông sản	СВОТ	30/06/2025	14/07/2025
13	Đậu tương 8/25	ZSEQ25	Nông sản	СВОТ	31/07/2025	14/08/2025
14	Đậu tương 9/25	ZSEU25	Nông sản	СВОТ	29/08/2025	12/09/2025
15	Đậu tương 11/25	ZSEX25	Nông sản	СВОТ	31/10/2025	14/11/2025
16	Đậu tương 1/26	ZSEF26	Nông sản	СВОТ	31/12/2025	14/01/2026
17	Đậu tương mini 7/25	XBN25	Nông sản	СВОТ	30/06/2025	14/07/2025
18	Đậu tương mini 8/25	XBQ25	Nông sản	СВОТ	31/07/2025	14/08/2025
19	Đậu tương mini 9/25	XBU25	Nông sản	СВОТ	29/08/2025	12/09/2025
20	Đậu tương mini 11/25	XBX25	Nông sản	СВОТ	31/10/2025	14/11/2025
21	Đậu tương mini 1/26	XBF26	Nông sản	СВОТ	31/12/2025	14/01/2026
22	Đậu tương micro 8/25	MZSQ25	Nông sản	СВОТ	25/07/2025	25/07/2025
23	Đậu tương micro 9/25	MZSU25	Nông sản	СВОТ	22/08/2025	22/08/2025
24	Đậu tương micro 11/25	MZSX25	Nông sản	Свот	24/10/2025	24/10/2025
25	Đậu tương micro 1/26	MZSF26	Nông sản	Свот	26/12/2025	26/12/2025
26	Khô đậu tương 7/25	ZMEN25	Nông sản	СВОТ	30/06/2025	14/07/2025
27	Khô đậu tương 8/25	ZMEQ25	Nông sản	СВОТ	31/07/2025	14/08/2025
28	Khô đậu tương 9/25	ZMEU25	Nông sản	CBOT	29/08/2025	12/09/2025
29	Khô đậu tương 10/25	ZMEV25	Nông sản	СВОТ	30/09/2025	14/10/2025
30	Khô đậu tương 12/25	ZMEZ25	Nông sản	Свот	28/11/2025	12/12/2025
31	Khô đậu tương 1/26	ZMEF26	Nông sản	СВОТ	31/12/2025	14/01/2026
32	Khô đậu tương micro 8/25	MZMQ25	Nông sản	Свот	25/07/2025	25/07/2025
33	Khô đậu tương micro 9/25	MZMU25	Nông sản	СВОТ	22/08/2025	22/08/2025
34	Khô đậu tương micro 10/25	MZMV25	Nông sản	Свот	26/09/2025	26/09/2025
35	Khô đậu tương micro 12/25	MZMZ25	Nông sản	СВОТ	21/11/2025	21/11/2025
36	Khô đậu tương micro 1/26	MZMF26	Nông sản	CBOT	26/12/2025	26/12/2025
37	Lúa mỳ 7/25	ZWAN25	Nông sản	СВОТ	30/06/2025	14/07/2025
38	Lúa mỳ 9/25	ZWAU25	Nông sản	CBOT	29/08/2025	12/09/2025
39	Lúa mỳ 12/25	ZWAZ25	Nông sản	СВОТ	28/11/2025	12/12/2025
40	Lúa mỳ mini 7/25	XWN25	Nông sản	СВОТ	30/06/2025	14/07/2025
41	Lúa mỳ mini 9/25	XWU25	Nông sản	СВОТ	29/08/2025	12/09/2025
42	Lúa mỳ mini 12/25	XWZ25	Nông sản	CBOT	28/11/2025	12/12/2025
43	Lúa mỳ micro 9/25	MZWU25	Nông sản	СВОТ	22/08/2025	22/08/2025
44	Lúa mỳ micro 12/25	MZWZ25	Nông sản	СВОТ	21/11/2025	21/11/2025
45	Lúa mỳ Kansas 7/25	KWEN25	Nông sản	СВОТ	30/06/2025	14/07/2025
46	Lúa mỳ Kansas 9/25	KWEU25	Nông sản	Свот	29/08/2025	12/09/2025
47	Lúa mỳ Kansas 12/25	KWEZ25	Nông sản	СВОТ	28/11/2025	12/12/2025
48	Ngô 7/25	ZCEN25	Nông sản	СВОТ	30/06/2025	14/07/2025
49	Ngô 9/25	ZCEU25	Nông sản	CBOT	29/08/2025	12/09/2025
50	Ngô 12/25	ZCEZ25	Nông sản	СВОТ	28/11/2025	12/12/2025
51	Ngô mini 7/25	XCN25	Nông sản	СВОТ	30/06/2025	14/07/2025
52	Ngô mini 9/25	XCU25	Nông sản	СВОТ	29/08/2025	12/09/2025
53	Ngô mini 12/25	XCZ25	Nông sản	Свот	28/11/2025	12/12/2025
54	Ngô micro 9/25	MZCU25	Nông sản	СВОТ	22/08/2025	22/08/2025
55	Ngô micro 12/25	MZCZ25	Nông sản	СВОТ	21/11/2025	21/11/2025
56	Dầu cọ thô 7/25	MPON25	Nguyên liệu	BMDX	30/06/2025	15/07/2025
57	Dầu cọ thô 8/25	MPOQ25	Nguyên liệu	BMDX	31/07/2025	15/08/2025
58	Dầu cọ thô 9/25	MPOU25	Nguyên liệu	BMDX	29/08/2025	15/09/2025
59	Dầu cọ thô 10/25	MPOV25	Nguyên liệu	BMDX	30/09/2025	15/10/2025
60	Dầu cọ thô 11/25	MPOX25	Nguyên liệu	BMDX	31/10/2025	14/11/2025
61	Dầu cọ thô 12/25	MPOZ25	Nguyên liệu	BMDX	28/11/2025	15/12/2025
62	Dầu cọ thô 1/26	MPOF26	Nguyên liệu	BMDX	31/12/2025	15/01/2026
63	Cà phê Robusta 7/25	LRCN25	Nguyên liệu	ICE EU	25/06/2025	25/07/2025
64	Cà phê Robusta 9/25	LRCU25	Nguyên liệu	ICE EU	26/08/2025	24/09/2025
65	Cà phê Robusta 11/25	LRCX25	Nguyên liệu	ICE EU	28/10/2025	24/11/2025
66	Cà phê Robusta 1/26	LRCF26	Nguyên liệu	ICE EU	24/12/2025	26/01/2026
67	Đường trắng 8/25	QWQ25	Nguyên liệu	ICE EU	16/07/2025	16/07/2025
68	Đường trắng 10/25	QWV25	Nguyên liệu	ICE EU	15/09/2025	15/09/2025
69	Đường trắng 12/25	QWZ25	Nguyên liệu	ICE EU	14/11/2025	14/11/2025
70	Bông sợi 7/25	CTEN25	Nguyên liệu	ICE US	24/06/2025	09/07/2025
71	Bông sợi 10/25	CTEV25	Nguyên liệu	ICE US	24/09/2025	09/10/2025
72	Bông sợi 12/25	CTEZ25	Nguyên liệu	ICE US	21/11/2025	08/12/2025
73	Ca cao 7/25	CCEN25	Nguyên liệu	ICE US	24/06/2025	16/07/2025
74	Ca cao 9/25	CCEU25	Nguyên liệu	ICE US	25/08/2025	15/09/2025
75	Ca cao 12/25	CCEZ25	Nguyên liệu	ICE US	21/11/2025	15/12/2025
76	Cà phê Arabica 7/25	KCEN25	Nguyên liệu	ICE US	20/06/2025	21/07/2025
77	Cà phê Arabica 9/25	KCEU25	Nguyên liệu	ICE US	21/08/2025	18/09/2025
78	Cà phê Arabica 12/25	KCEZ25	Nguyên liệu	ICE US	19/11/2025	18/12/2025
79	Đường 10/25	SBEV25	Nguyên liệu	ICE US	30/09/2025	30/09/2025
80	Cao su RSS3 7/25	TRUN25	Nguyên liệu	OSE	25/07/2025	25/07/2025
81	Cao su RSS3 8/25	TRUQ25	Nguyên liệu	OSE	25/08/2025	25/08/2025
82	Cao su RSS3 9/25	TRUU25	Nguyên liệu	OSE	24/09/2025	24/09/2025
83	Cao su RSS3 10/25	TRUV25	Nguyên liệu	OSE	27/10/2025	27/10/2025
84	Cao su RSS3 11/25	TRUX25	Nguyên liệu	OSE	21/11/2025	21/11/2025
85	Cao su RSS3 12/25	TRUZ25	Nguyên liệu	OSE	22/12/2025	22/12/2025
86	Cao su TSR20 8/25	ZFTQ25	Nguyên liệu	SGX	31/07/2025	31/07/2025
87	Cao su TSR20 9/25	ZFTU25	Nguyên liệu	SGX	29/08/2025	29/08/2025
88	Cao su TSR20 10/25	ZFTV25	Nguyên liệu	SGX	30/09/2025	30/09/2025
89	Cao su TSR20 11/25	ZFTX25	Nguyên liệu	SGX	31/10/2025	31/10/2025
90	Cao su TSR20 12/25	ZFTZ25	Nguyên liệu	SGX	28/11/2025	28/11/2025
91	Cao su TSR20 1/26	ZFTF26	Nguyên liệu	SGX	30/12/2025	30/12/2025
92	Bạc Nano ACM 9/2025	SI5COU25	Kim loại	ACM	27/08/2025	27/08/2025
93	Bạc Nano ACM 12/2025	SI5COZ25	Kim loại	ACM	25/11/2025	25/11/2025
94	Bạc Nano ACM 1/2026	SI5COF26	Kim loại	ACM	29/12/2025	29/12/2025
95	Bạch kim Nano ACM 10/2025	PL1NYV25	Kim loại	ACM	26/09/2025	26/09/2025
96	Bạch kim Nano ACM 1/2026	PL1NYF26	Kim loại	ACM	29/12/2025	29/12/2025
97	Đồng Nano ACM 8/2025	CP2COQ25	Kim loại	ACM	29/07/2025	29/07/2025
98	Đồng Nano ACM 9/2025	CP2COU25	Kim loại	ACM	27/08/2025	27/08/2025
99	Đồng Nano ACM 10/2025	CP2COV25	Kim loại	ACM	26/09/2025	26/09/2025
100	Đồng Nano ACM 11/2025	CP2COX25	Kim loại	ACM	29/10/2025	29/10/2025
101	Đồng Nano ACM 12/2025	CP2COZ25	Kim loại	ACM	25/11/2025	25/11/2025
102	Đồng Nano ACM 1/2026	CP2COF26	Kim loại	ACM	29/12/2025	29/12/2025
103	Bạc 7/25	SIEN25	Kim loại	COMEX	30/06/2025	29/07/2025
104	Bạc 8/25	SIEQ25	Kim loại	COMEX	31/07/2025	27/08/2025
105	Bạc 9/25	SIEU25	Kim loại	COMEX	29/08/2025	26/09/2025
106	Bạc 10/25	SIEV25	Kim loại	COMEX	30/09/2025	29/10/2025
107	Bạc 11/25	SIEX25	Kim loại	COMEX	31/10/2025	25/11/2025
108	Bạc 12/25	SIEZ25	Kim loại	COMEX	28/11/2025	29/12/2025
109	Bạc 1/26	SIEF26	Kim loại	COMEX	31/12/2025	28/01/2026
110	Bạc mini 9/25	MQIU25	Kim loại	COMEX	27/08/2025	27/08/2025
111	Bạc mini 12/25	MQIZ25	Kim loại	COMEX	25/11/2025	25/11/2025
112	Bạc mini 1/26	MQIF26	Kim loại	COMEX	29/12/2025	29/12/2025
113	Bạc micro 7/25	SILN25	Kim loại	COMEX	30/06/2025	29/07/2025
114	Bạc micro 8/25	SILQ25	Kim loại	COMEX	31/07/2025	27/08/2025
115	Bạc micro 9/25	SILU25	Kim loại	COMEX	29/08/2025	26/09/2025
116	Bạc micro 10/25	SILV25	Kim loại	COMEX	30/09/2025	29/10/2025
117	Bạc micro 11/25	SILX25	Kim loại	COMEX	31/10/2025	25/11/2025
118	Bạc micro 12/25	SILZ25	Kim loại	COMEX	28/11/2025	29/12/2025
119	Bạc micro 1/26	SILF26	Kim loại	COMEX	31/12/2025	28/01/2026
120	Đồng 7/25	CPEN25	Kim loại	COMEX	30/06/2025	29/07/2025
121	Đồng 8/25	CPEQ25	Kim loại	COMEX	31/07/2025	27/08/2025
122	Đồng 9/25	CPEU25	Kim loại	COMEX	29/08/2025	26/09/2025
123	Đồng 10/25	CPEV25	Kim loại	COMEX	30/09/2025	29/10/2025
124	Đồng 11/25	CPEX25	Kim loại	COMEX	31/10/2025	25/11/2025
125	Đồng 12/25	CPEZ25	Kim loại	COMEX	28/11/2025	29/12/2025
126	Đồng 1/26	CPEF26	Kim loại	COMEX	31/12/2025	28/01/2026
127	Đồng mini 8/25	MQCQ25	Kim loại	COMEX	29/07/2025	29/07/2025
128	Đồng mini 9/25	MQCU25	Kim loại	COMEX	27/08/2025	27/08/2025
129	Đồng mini 10/25	MQCV25	Kim loại	COMEX	26/09/2025	26/09/2025
130	Đồng mini 11/25	MQCX25	Kim loại	COMEX	29/10/2025	29/10/2025
131	Đồng mini 12/25	MQCZ25	Kim loại	COMEX	25/11/2025	25/11/2025
132	Đồng mini 1/26	MQCF26	Kim loại	COMEX	29/12/2025	29/12/2025
133	Đồng micro 8/25	MHGQ25	Kim loại	COMEX	29/07/2025	29/07/2025
134	Đồng micro 9/25	MHGU25	Kim loại	COMEX	27/08/2025	27/08/2025
135	Đồng micro 10/25	MHGV25	Kim loại	COMEX	26/09/2025	26/09/2025
136	Đồng micro 11/25	MHGX25	Kim loại	COMEX	29/10/2025	29/10/2025
137	Đồng micro 12/25	MHGZ25	Kim loại	COMEX	25/11/2025	25/11/2025
138	Đồng micro 1/26	MHGF26	Kim loại	COMEX	29/12/2025	29/12/2025
139	Nhôm COMEX 7/25	ALIN25	Kim loại	COMEX	30/06/2025	29/07/2025
140	Nhôm COMEX 8/25	ALIQ25	Kim loại	COMEX	31/07/2025	27/08/2025
141	Nhôm COMEX 9/25	ALIU25	Kim loại	COMEX	29/08/2025	26/09/2025
142	Nhôm COMEX 10/25	ALIV25	Kim loại	COMEX	30/09/2025	29/10/2025
143	Nhôm COMEX 11/25	ALIX25	Kim loại	COMEX	31/10/2025	25/11/2025
144	Nhôm COMEX 12/25	ALIZ25	Kim loại	COMEX	28/11/2025	29/12/2025
145	Nhôm COMEX 1/26	ALIF26	Kim loại	COMEX	31/12/2025	28/01/2026
146	Bạch kim 7/25	PLEN25	Kim loại	NYMEX	30/06/2025	29/07/2025
147	Bạch kim 8/25	PLEQ25	Kim loại	NYMEX	31/07/2025	27/08/2025
148	Bạch kim 9/25	PLEU25	Kim loại	NYMEX	29/08/2025	26/09/2025
149	Bạch kim 10/25	PLEV25	Kim loại	NYMEX	30/09/2025	29/10/2025
150	Bạch kim 11/25	PLEX25	Kim loại	NYMEX	31/10/2025	25/11/2025
151	Bạch kim 12/25	PLEZ25	Kim loại	NYMEX	28/11/2025	29/12/2025
152	Bạch kim 1/26	PLEF26	Kim loại	NYMEX	31/12/2025	28/01/2026
153	Quặng sắt 7/25	FEFN25	Kim loại	SGX	31/07/2025	31/07/2025
154	Quặng sắt 8/25	FEFQ25	Kim loại	SGX	29/08/2025	29/08/2025
155	Quặng sắt 9/25	FEFU25	Kim loại	SGX	30/09/2025	30/09/2025
156	Quặng sắt 10/25	FEFV25	Kim loại	SGX	31/10/2025	31/10/2025
157	Quặng sắt 11/25	FEFX25	Kim loại	SGX	28/11/2025	28/11/2025
158	Quặng sắt 12/25	FEFZ25	Kim loại	SGX	31/12/2025	31/12/2025
159	Đồng LME	LDKZ/CAD	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
160	Nhôm LME	LALZ/AHD	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
161	Chì LME	LEDZ/PBD	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
162	Thiếc LME	LTIZ/SND	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
163	Kẽm LME	LZHZ/ZDS	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
164	Niken LME	LNIZ/NID	Kim loại	LME	02 ngày làm việc trước ngày đáo hạn của hợp đồng	
165	Thép thanh vằn FOB Thổ Nhĩ Kỳ 7/25	SSRN25	Kim loại	LME	31/07/2025	31/07/2025
166	Thép thanh vằn FOB Thổ Nhĩ Kỳ 8/25	SSRQ25	Kim loại	LME	29/08/2025	29/08/2025
167	Thép thanh vằn FOB Thổ Nhĩ Kỳ 9/25	SSRU25	Kim loại	LME	30/09/2025	30/09/2025
168	Thép thanh vằn FOB Thổ Nhĩ Kỳ 10/25	SSRV25	Kim loại	LME	31/10/2025	31/10/2025
169	Thép thanh vằn FOB Thổ Nhĩ Kỳ 11/25	SSRX25	Kim loại	LME	28/11/2025	28/11/2025
170	Thép thanh vằn FOB Thổ Nhĩ Kỳ 12/25	SSRZ25	Kim loại	LME	31/12/2025	31/12/2025
171	Thép phế liệu CFR Thổ Nhĩ Kỳ 7/25	SSCN25	Kim loại	LME	31/07/2025	31/07/2025
172	Thép phế liệu CFR Thổ Nhĩ Kỳ 8/25	SSCQ25	Kim loại	LME	29/08/2025	29/08/2025
173	Thép phế liệu CFR Thổ Nhĩ Kỳ 9/25	SSCU25	Kim loại	LME	30/09/2025	30/09/2025
174	Thép phế liệu CFR Thổ Nhĩ Kỳ 10/25	SSCV25	Kim loại	LME	31/10/2025	31/10/2025
175	Thép phế liệu CFR Thổ Nhĩ Kỳ 11/25	SSCX25	Kim loại	LME	28/11/2025	28/11/2025
176	Thép phế liệu CFR Thổ Nhĩ Kỳ 12/25	SSCZ25	Kim loại	LME	31/12/2025	31/12/2025
177	Thép cuộn cán nóng FOB Trung Quốc 7/25	LHCN25	Kim loại	LME	31/07/2025	31/07/2025
178	Thép cuộn cán nóng FOB Trung Quốc 8/25	LHCQ25	Kim loại	LME	29/08/2025	29/08/2025
179	Thép cuộn cán nóng FOB Trung Quốc 9/25	LHCU25	Kim loại	LME	30/09/2025	30/09/2025
180	Thép cuộn cán nóng FOB Trung Quốc 10/25	LHCV25	Kim loại	LME	31/10/2025	31/10/2025
181	Thép cuộn cán nóng FOB Trung Quốc 11/25	LHCX25	Kim loại	LME	28/11/2025	28/11/2025
182	Thép cuộn cán nóng FOB Trung Quốc 12/25	LHCZ25	Kim loại	LME	31/12/2025	31/12/2025
"""

def build_embedded_schedule() -> pd.DataFrame:
    df = pd.read_csv(StringIO(SCHED_RAW), sep="\t", dtype=str, keep_default_na=False)
    df = df.rename(columns={
        "STT":"STT",
        "Tên hợp đồng":"Tên HĐ (lich)",
        "Mã hợp đồng":"Mã HĐ",
        "Nhóm hàng hóa":"Nhóm (lich)",
        "Sở giao dịch":"Sở GD",
        "Ngày thông báo đầu tiên":"FND",
        "Ngày giao dịch cuối cùng":"LTD",
    })
    df["Mã HĐ"] = df["Mã HĐ"].str.strip().str.upper()
    df["Sở GD"] = df["Sở GD"].map(_fix_cyrillic_like)
    df["FND"] = _parse_ddmmyyyy(df["FND"])
    df["LTD"] = _parse_ddmmyyyy(df["LTD"])
    df = df.sort_values(["Mã HĐ"]).reset_index(drop=True)

    out_dir = os.path.join(os.path.dirname(__file__) if '__file__' in globals() else ".", "data")
    os.makedirs(out_dir, exist_ok=True)
    xlsx_path = os.path.join(out_dir, "expiry_schedule_mxv_2025-09-23.xlsx")
    csv_path  = os.path.join(out_dir, "expiry_schedule_mxv_2025-09-23.csv")
    try:
        df.to_excel(xlsx_path, index=False)
    except Exception:
        pass
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return df

SCHEDULE_EMBEDDED = build_embedded_schedule()

# -------------------- SIDEBAR --------------------
st.sidebar.header("Tải dữ liệu")
pos_file = st.sidebar.file_uploader("1) Trạng thái mở (Excel)", type=["xlsx","xls"])
mgm_file = st.sidebar.file_uploader("2) Danh sách quản lý ký quỹ (Excel)", type=["xlsx","xls"])
fx_vnd_usd = st.sidebar.number_input("Tỷ giá quy đổi VND → USD", min_value=10000, max_value=40000, value=25000, step=50)
alert_days = st.sidebar.number_input("Cảnh báo FND trước (ngày)", min_value=1, max_value=60, value=14, step=1)
show_debug = st.sidebar.checkbox("Hiển thị debug cột", value=False)

if not pos_file or not mgm_file:
    st.info("Hãy tải **cả hai** file Excel: POS + MGM.")
    st.stop()

# -------------------- ĐỌC FILE POS / MGM --------------------
pos_require = [
    ["mã hđ","ma hd","mã hợp đồng","ma hop dong"],
    ["mã tkgd","ma tkgd","tkgd"],
    ["giá tt","gia tt","gia thi truong","gia thuc te"],
]
mgm_require = [
    ["mã tkgd","ma tkgd","tkgd"],
    ["gia tri rong ky quy usd","giá trị ròng ký quỹ usd","giá trị ròng ký quỹ (usd)","equity usd","equity_now"],
    ["ty le ky quy hien tai","tỷ lệ ký quỹ hiện tại","margin_now","margin ratio","ty le ky quy hien tai (%)","tỷ lệ ký quỹ hiện tại (%)"],
]

pos_raw, pos_sheet, pos_header = _read_best_sheet(pos_file, pos_require)
mgm_raw, mgm_sheet, mgm_header = _read_best_sheet(mgm_file, mgm_require)

if show_debug:
    st.caption(f"POS: sheet **{pos_sheet}** header @{pos_header} | MGM: sheet **{mgm_sheet}** header @{mgm_header}")
    st.write("POS cols:", list(_norm_cols(pos_raw).values()))
    st.write("MGM cols:", list(_norm_cols(mgm_raw).values()))

# -------------------- CHUẨN HÓA POS --------------------
c_mahd  = _find_col(pos_raw, ["mã hđ","ma hd","mã hợp đồng","ma hop dong"], required=True)
c_tkgd  = _find_col(pos_raw, ["mã tkgd","ma tkgd","tkgd"], required=True)
c_ten   = _find_col(pos_raw, ["tên tkgd","ten tkgd","khach hang"], required=False)
c_tenhd = _find_col(pos_raw, ["tên hđ","ten hd","tên hợp đồng"], required=False)
c_sp    = _find_col(pos_raw, ["sp_magd","mã giao dịch","ma gd","ma hang","ma sp"], required=False)
c_buy   = _find_col(pos_raw, ["kl mua","so luong mua"], required=False)
c_sell  = _find_col(pos_raw, ["kl bán","kl ban","so luong ban"], required=False)
c_net   = _find_col(pos_raw, ["netqty","net qty","kl rong"], required=False)
c_giatt = _find_col(pos_raw, ["giá tt","gia tt","gia thi truong","gia thuc te"], required=True)
c_giatb = _find_col(pos_raw, ["giá tb","gia tb","gia vao lenh"], required=False)
c_tick  = _find_col(pos_raw, ["ticksize","tick size","buoc gia"], required=False)
c_imrow = _find_col(pos_raw, ["im_row","im_row_expect","im position","im per row","im/vithe"], required=False)
c_mult  = _find_col(pos_raw, ["contract_multiplier","multiplier","he so lot","lotsize"], required=False)

pos = pos_raw.copy()
pos["Mã HĐ"]    = pos[c_mahd].astype(str).str.strip().str.upper()
pos["TKGD_KEY"] = pos[c_tkgd].astype(str)
pos["Tên TKGD"] = pos[c_ten].astype(str) if c_ten else ""
pos["Tên HĐ"]   = pos[c_tenhd].astype(str) if c_tenhd else ""
pos["SP_MaGD"]  = pos[c_sp].astype(str) if c_sp else ""

# loại quyền chọn theo SP_MaGD nếu có
is_opt = pos["SP_MaGD"].map(looks_like_option_str)
if is_opt.any():
    st.warning(f"Đã loại **{int(is_opt.sum())}** dòng quyền chọn theo SP_MaGD (prefix C./P.).")
pos = pos.loc[~is_opt].copy()

pos["Giá TT"] = _num_col(pos, c_giatt)
pos["Giá TB"] = _num_col(pos, c_giatb)
pos["KL Mua"] = _num_col(pos, c_buy, 0.0) if c_buy else pd.Series(0.0, index=pos.index)
pos["KL Bán"] = _num_col(pos, c_sell, 0.0) if c_sell else pd.Series(0.0, index=pos.index)
if c_net:
    pos["NetQty"] = _num_col(pos, c_net).fillna(pos["KL Mua"] - pos["KL Bán"])
else:
    pos["NetQty"] = (pos["KL Mua"] - pos["KL Bán"]).fillna(0.0)

pos["TickSize_POS"] = _num_col(pos, c_tick)
pos["IM_row_file"]  = _num_col(pos, c_imrow)
pos["Mult_POS"]     = _num_col(pos, c_mult)

# -------------------- BÓC MÃ / ALIAS --------------------
pos["SP_RawToken"] = pos["Mã HĐ"].map(extract_raw_token)
pos["SP_Base"]     = pos["SP_RawToken"].map(to_base_code)

ALIAS = {
    "ZCEZ":"ZCE", "ZCEH":"ZCE", "ZCEK":"ZCE", "ZCEN":"ZCE", "ZCEU":"ZCE", "ZCEV":"ZCE", "ZCEX":"ZCE",
    "ZWAZ":"ZWA", "ZWAH":"ZWA", "ZWAK":"ZWA", "ZWAN":"ZWA", "ZWAU":"ZWA", "ZWAV":"ZWA", "ZWAX":"ZWA",
    "XWH":"XW", "XWZ":"XW", "XWK":"XW", "XWU":"XW",
    "XCZ":"XC", "XCH":"XC", "XCK":"XC", "XCU":"XC",
    "PLEF":"PLE", "PLEJ":"PLE", "PLEK":"PLE", "PLEM":"PLE", "PLEN":"PLE",
    "SILZ":"SIL", "SILH":"SIL", "SILK":"SIL",
    "PL1NYF":"PL1NY",
    "SI5COZ":"SI5CO",
    "CP2COZ":"CP2CO",
    "SBEH":"SBE", "SBEK":"SBE", "SBEV":"SBE", "SBEZ":"SBE",
}
pos["SP_Base_norm"] = pos["SP_Base"].replace(ALIAS)

# -------------------- CATALOG TÍCH HỢP --------------------
cat = pd.DataFrame([
    # Nông nghiệp
    ("ZSE","Đậu tương CBOT","nông nghiệp",5000,"cent/giạ",0.25,0.01,58_256_000),
    ("XB","Đậu tương mini CBOT","nông nghiệp",1000,"cent/giạ",0.125,0.01,11_651_200),
    ("MZS","Đậu tương micro CBOT","nông nghiệp",500,"cent/giạ",0.5,0.01,6_911_280),
    ("ZLE","Dầu đậu tương CBOT","nông nghiệp",60000,"cent/pound",0.01,0.01,61_168_800),
    ("ZME","Khô đậu tương CBOT","nông nghiệp",100,"USD/tấn thiếu",0.1,1.0,45_148_400),
    ("MZM","Khô đậu tương micro","nông nghiệp",10,"USD/tấn thiếu",0.2,1.0,4_528_080),
    ("ZCE","Ngô CBOT","nông nghiệp",5000,"cent/giạ",0.25,0.01,28_413_040),
    ("XC","Ngô mini CBOT","nông nghiệp",1000,"cent/giạ",0.125,0.01,5_693_200),
    ("ZWA","Lúa mì CBOT","nông nghiệp",5000,"cent/giạ",0.25,0.01,48_061_200),
    ("XW","Lúa mì mini CBOT","nông nghiệp",1000,"cent/giạ",0.125,0.01,9_612_240),
    ("MZW","Lúa mì micro CBOT","nông nghiệp",500,"cent/giạ",0.5,0.01,4_819_360),
    ("KWE","Lúa mì Kansas CBOT","nông nghiệp",5000,"cent/giạ",0.25,0.01,46_604_800),
    # Kim loại
    ("PLE","Bạch kim NYMEX","kim loại",50,"USD/troy oz",0.1,1.0,145_640_000),
    ("PL1NY","Bạch kim Nano ACM","kim loại",5,"USD/troy oz",0.1,1.0,8_976_720),
    ("SIE","Bạc COMEX","kim loại",5000,"USD/troy oz",0.005,1.0,436_920_000),
    ("MQI","Bạc mini COMEX","kim loại",2500,"USD/troy oz",0.0125,1.0,218_460_000),
    ("SIL","Bạc micro COMEX","kim loại",1000,"USD/troy oz",0.005,1.0,87_384_000),
    ("SI5CO","Bạc Nano ACM","kim loại",100,"USD/troy oz",0.005,1.0,5_057_680),
    ("CPE","Đồng COMEX","kim loại",25000,"USD/pound",0.0005,1.0,262_152_000),
    ("MQC","Đồng mini COMEX","kim loại",12500,"USD/pound",0.002,1.0,131_076_000),
    ("MHG","Đồng micro COMEX","kim loại",2500,"USD/pound",0.0005,1.0,26_215_200),
    ("CP2CO","Đồng Nano ACM","kim loại",1000,"USD/pound",0.0005,1.0,5_296_000),
    ("ALI","Nhôm COMEX","kim loại",25,"USD/ton",0.25,1.0,101_948_000),
    # Nguyên liệu CN
    ("SBE","Đường 11 ICE US","nguyên liệu công nghiệp",112000,"cent/pound",0.01,0.01,28_386_560),
    ("QW","Đường trắng ICE EU","nguyên liệu công nghiệp",50,"USD/tấn",0.1,1.0,46_287_040),
    ("KCE","Cà phê Arabica ICE US","nguyên liệu công nghiệp",37500,"cent/pound",0.05,0.01,337_858_320),
    ("LRC","Cà phê Robusta ICE EU","nguyên liệu công nghiệp",10,"USD/tấn",1.0,1.0,164_864_480),
    ("CTE","Bông ICE US","nguyên liệu công nghiệp",50000,"cent/pound",0.01,0.01,43_294_800),
    ("CCE","Cacao ICE US","nguyên liệu công nghiệp",10,"USD/tấn",1.0,1.0,255_161_280),
    ("TRU","Cao su RSS3","nguyên liệu công nghiệp",5000,"JPY/khối",0.1,np.nan,525_141_000),
    ("ZFT","Cao su TSR20","nguyên liệu công nghiệp",10,"USD/khối",1.0,1.0,17_476_800),
    ("MPO","Dầu cọ thô","nguyên liệu công nghiệp",25,"USD/tấn",1.0,1.0,1_288_000),
], columns=["SP_Base","SP_Ten","SP_Nhom","LotSize","QuoteUnit","TickSize_cat","USD_per_quote_unit","IM_per_contract_VND"])
cat["SP_Base"] = cat["SP_Base"].str.upper()

dup = cat["SP_Base"].value_counts()
dup = dup[dup>1]
if not dup.empty:
    st.warning("⚠️ Catalog có SP_Base trùng: " + ", ".join(list(dup.index)))

cat["Contract_Multiplier_cat"] = pd.to_numeric(cat["LotSize"], errors="coerce") * pd.to_numeric(cat["USD_per_quote_unit"], errors="coerce")
cat["IM_per_lot_USD"] = pd.to_numeric(cat["IM_per_contract_VND"], errors="coerce") / float(fx_vnd_usd)

pos = pos.merge(cat, left_on="SP_Base_norm", right_on="SP_Base", how="left", suffixes=("","_cat"))
pos["Contract_Multiplier"] = pos["Mult_POS"].where(pos["Mult_POS"].notna(), pos["Contract_Multiplier_cat"])
pos["TickSize"]            = pos["TickSize_POS"].where(pos["TickSize_POS"].notna(), pos["TickSize_cat"])

missing_mask = pos["Contract_Multiplier"].isna() | pos["TickSize"].isna() | pos["SP_Ten"].isna()
missing = (pos.loc[missing_mask, ["SP_Base","SP_RawToken","Mã HĐ"]]
             .drop_duplicates()
             .rename(columns={"SP_Base":"Gợi ý mã base"}))
if not missing.empty:
    st.warning("📝 Các mã cần bổ sung vào catalog:\n\n" + missing.to_markdown(index=False))

# -------------------- GHÉP LỊCH FND/LTD + CẢNH BÁO HỆ THỐNG --------------------
sched = SCHEDULE_EMBEDDED.copy()
today = pd.Timestamp.today().normalize()

pos = pos.merge(
    sched[["Mã HĐ","FND","LTD","Tên HĐ (lich)","Sở GD"]],
    on="Mã HĐ", how="left"
)
pos["Days_to_FND"] = (pos["FND"] - today).dt.days

soon_mask_all = pos["FND"].notna() & (pos["Days_to_FND"]>=0) & (pos["Days_to_FND"] <= int(alert_days))
soon_df_all = (pos.loc[soon_mask_all, ["TKGD_KEY","Tên TKGD","Mã HĐ","SP_Ten","FND","LTD","Days_to_FND"]]
                 .drop_duplicates()
                 .sort_values(["Days_to_FND","TKGD_KEY","Mã HĐ"]))

with st.expander(f"🔔 Cảnh báo FND toàn hệ thống (≤ {alert_days} ngày)", expanded=not soon_df_all.empty):
    if soon_df_all.empty:
        st.info("Chưa có vị thế nào sắp tới ngày **thông báo đầu tiên (FND)** trong ngưỡng.")
    else:
        st.success(f"Có **{len(soon_df_all)}** dòng vị thế sắp FND trong {alert_days} ngày.")
        st.dataframe(soon_df_all, use_container_width=True)
        for _, rr in soon_df_all.head(10).iterrows():
            st.toast(f"⏰ {rr['Mã HĐ']} | {rr['SP_Ten']}: còn {int(rr['Days_to_FND'])} ngày tới FND", icon="🔔")

def _ensure_data_dir():
    out_dir = os.path.join(os.path.dirname(__file__) if '__file__' in globals() else ".", "data")
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

def save_reminders_csv_ics(df: pd.DataFrame, fname_base: str = "expiry_reminders"):
    out_dir = _ensure_data_dir()
    keep = df.copy()
    # nếu không có cột TKGD_KEY/Tên TKGD thì thêm rỗng để tránh lỗi
    for col in ["TKGD_KEY","Tên TKGD"]:
        if col not in keep.columns: keep[col] = ""
    for col in ["FND","LTD"]:
        if col in keep.columns: keep[col] = pd.to_datetime(keep[col], errors="coerce")

    cols = [c for c in ["TKGD_KEY","Tên TKGD","Mã HĐ","SP_Ten","FND","LTD","Days_to_FND","Days_to_Expiry"] if c in keep.columns]
    csv_path = os.path.join(out_dir, f"{fname_base}.csv")
    keep[cols].to_csv(csv_path, index=False, encoding="utf-8-sig")

    # Tạo ICS all-day events tại FND (nếu có), fallback LTD nếu cần
    ics_lines = ["BEGIN:VCALENDAR","VERSION:2.0","PRODID:-//mxv-app//expiry//VN"]
    for _, r in keep.iterrows():
        when = r["FND"] if ("FND" in keep.columns and pd.notna(r["FND"])) else (r["LTD"] if ("LTD" in keep.columns and pd.notna(r["LTD"])) else None)
        if when is None: 
            continue
        dt = pd.to_datetime(when).date()
        dt_str = dt.strftime("%Y%m%d")
        uid = f"{r.get('Mã HĐ','UNKNOWN')}-{uuid.uuid4().hex[:8]}@mxv-app"
        summary = f"FND {r.get('Mã HĐ','')}" if ("FND" in keep.columns and pd.notna(r.get("FND", pd.NaT))) else f"EXP {r.get('Mã HĐ','')}"
        desc = f"Tên KH: {r.get('Tên TKGD','')} | TKGD: {r.get('TKGD_KEY','')} | LTD: {r.get('LTD','')}"
        ics_lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            f"DTSTART;VALUE=DATE:{dt_str}",
            f"SUMMARY:{summary}",
            f"DESCRIPTION:{desc}",
            "END:VEVENT"
        ]
    ics_lines.append("END:VCALENDAR")
    ics_path = os.path.join(out_dir, f"{fname_base}.ics")
    with open(ics_path, "w", encoding="utf-8") as f:
        f.write("\n".join(ics_lines))
    return csv_path, ics_path

if st.button("💾 Lưu nhắc lịch FND (CSV + ICS) — toàn hệ thống"):
    if soon_df_all.empty:
        st.warning("Không có dòng sắp FND theo ngưỡng để lưu.")
    else:
        csv_path, ics_path = save_reminders_csv_ics(soon_df_all, "expiry_reminders")
        st.success(f"Đã lưu: `{csv_path}` và `{ics_path}`")

# -------------------- IM VỊ THẾ & MGM --------------------
pos["IM_row_file_USD"] = pos["IM_row_file"]
pos["IM_row_calc_USD"] = (pos["IM_per_lot_USD"] * pos["NetQty"].abs()).where(pos["IM_per_lot_USD"].notna())
pos["IM_row_USD"]      = pos["IM_row_file_USD"].where(pos["IM_row_file_USD"].notna(), pos["IM_row_calc_USD"])

m_tkgd  = _find_col(mgm_raw, ["mã tkgd","ma tkgd","tkgd"], required=True)
m_ten   = _find_col(mgm_raw, ["tên tkgd","ten tkgd"], required=False)
m_equ   = _find_col(mgm_raw, ["gia tri rong ky quy usd","giá trị ròng ký quỹ usd","giá trị ròng ký quỹ (usd)","equity usd","equity_now"], required=True)
m_ratio = _find_col(mgm_raw, ["ty le ky quy hien tai","tỷ lệ ký quỹ hiện tại","margin_now","margin ratio","ty le ky quy hien tai (%)","tỷ lệ ký quỹ hiện tại (%)"], required=True)
m_imtot = _find_col(mgm_raw, ["im_total_required","im tong","im yeu cau","tong ky quy ban dau","ky quy ban dau yeu cau usd"], required=False)

mgm = mgm_raw.copy()
mgm["TKGD_KEY"]     = mgm[m_tkgd].astype(str)
mgm["Tên TKGD"]     = mgm[m_ten].astype(str) if m_ten else ""
mgm["Equity_now"]   = _num_col(mgm, m_equ)
mgm["Margin_now_%"] = _num_col(mgm, m_ratio)
mgm["IM_total_mgm"] = _num_col(mgm, m_imtot)

im_from_rows = (pos.groupby("TKGD_KEY", dropna=False)["IM_row_USD"]
                  .sum(min_count=1).rename("IM_total_from_rows").reset_index())

acct = (mgm[["TKGD_KEY","Tên TKGD","Equity_now","Margin_now_%","IM_total_mgm"]]
        .merge(im_from_rows, on="TKGD_KEY", how="left"))

def _pick_im_total(row):
    if pd.notna(row["IM_total_mgm"]) and row["IM_total_mgm"]>0: return row["IM_total_mgm"]
    if pd.notna(row["IM_total_from_rows"]) and row["IM_total_from_rows"]>0: return row["IM_total_from_rows"]
    if pd.notna(row["Margin_now_%"]) and row["Margin_now_%"]>0 and pd.notna(row["Equity_now"]):
        return row["Equity_now"]/(row["Margin_now_%"]/100.0)
    return np.nan

acct["IM_total_required"] = acct.apply(_pick_im_total, axis=1)

# -------------------- ĐỘ NHẠY & % MOVE CẦN THIẾT --------------------
base = pos[["TKGD_KEY","NetQty","Contract_Multiplier","Giá TT"]].copy()
base["NetQty"]              = _safe_num(base["NetQty"])
base["Contract_Multiplier"] = _safe_num(base["Contract_Multiplier"])
base["Giá TT"]              = _safe_num(base["Giá TT"])

base["dPnL_up_1pct"]   = base["NetQty"] * base["Contract_Multiplier"] * (base["Giá TT"] * 0.01)
base["dPnL_down_1pct"] = -base["dPnL_up_1pct"]
base["adverse_up"]     = np.where(base["dPnL_up_1pct"]   < 0, base["dPnL_up_1pct"],   0.0)
base["adverse_down"]   = np.where(base["dPnL_down_1pct"] < 0, base["dPnL_down_1pct"], 0.0)

acc_delta = (base.groupby("TKGD_KEY", dropna=False)
                 .agg(k_up=("adverse_up","sum"),
                      k_down=("adverse_down","sum"))
                 .reset_index())
acct = acct.merge(acc_delta, on="TKGD_KEY", how="left").fillna({"k_up":0.0,"k_down":0.0})

def _need_pct_points(eq, im, k_up, k_dn, target_pct):
    if not (pd.notna(im) and im>0 and pd.notna(eq)): 
        return (np.nan, "down")
    target = im * (target_pct/100.0)
    over = eq - target
    if over <= 0:
        return (0.0, "down")
    loss_up  = abs(min(0.0, k_up))
    loss_dn  = abs(min(0.0, k_dn))
    need_up = np.inf if loss_up==0 else over / loss_up
    need_dn = np.inf if loss_dn==0 else over / loss_dn
    if need_up <= need_dn:
        return (float(max(0.0, need_up)), "up")
    else:
        return (float(max(0.0, need_dn)), "down")

rows = []
for _, r in acct.iterrows():
    eq, im, kup, kdn = r["Equity_now"], r["IM_total_required"], r["k_up"], r["k_down"]
    need, dire = {}, {}
    for t in MARGIN_TARGETS:
        npt, d = _need_pct_points(eq, im, kup, kdn, t)
        need[f"need_to_{t}%_pt"] = npt
        dire[f"dir_{t}%"]        = d
    rows.append({"TKGD_KEY": r["TKGD_KEY"], **need, **dire})
acct = acct.merge(pd.DataFrame(rows), on="TKGD_KEY", how="left")

# -------------------- THỐNG KÊ NHÓM (GLOBAL) --------------------
def build_product_group_stats(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return pd.DataFrame()
    x = df.copy()
    x["Notional"]      = _safe_num(x["NetQty"]) * _safe_num(x["Contract_Multiplier"]) * _safe_num(x["Giá TT"])
    x["GrossNotional"] = x["Notional"].abs()
    x["GrossQty"]      = _safe_num(x["NetQty"]).abs()
    agg = (x.groupby(["SP_Nhom","SP_Base_norm","SP_Ten"], dropna=False)
             .agg(n_contracts=("Mã HĐ","nunique"),
                  NetQty=("NetQty","sum"),
                  GrossQty=("GrossQty","sum"),
                  GrossNotional=("GrossNotional","sum"))
             .reset_index())
    total = agg["GrossNotional"].sum()
    agg["Share_%"] = np.where(total>0, agg["GrossNotional"]/total*100.0, 0.0)
    return agg.sort_values(["SP_Nhom","GrossNotional"], ascending=[True, False])

with st.expander("📊 Thống kê nhóm — Toàn bộ dữ liệu", expanded=False):
    stats_all = build_product_group_stats(pos)
    if not stats_all.empty:
        st.dataframe(stats_all, use_container_width=True)
        fig = px.pie(stats_all, names="SP_Base_norm", values="GrossNotional", hole=0.55,
                     title="Thị phần theo Gross Notional (toàn bộ)")
        fig.update_layout(height=420, margin=dict(l=10,r=10,t=60,b=10))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Chưa có dữ liệu sau khi map catalog.")

# -------------------- TRA CỨU TÀI KHOẢN --------------------
def kpi_row(acc_row: pd.Series, n_positions: int):
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Equity (USD)", f"{float(acc_row['Equity_now']):,.2f}")
    c2.metric("IM tổng (USD)", f"{float(acc_row['IM_total_required']):,.2f}")
    c3.metric("Margin hiện tại", f"{float(acc_row['Margin_now_%']):.2f}%")
    c4.metric("Số vị thế", f"{n_positions}")

q = st.text_input("Nhập **Tên KH** hoặc **Mã TKGD** (có thể 4–7 số cuối, vd `0006886-A`)").strip()
if not q: st.stop()

acct_idx = (acct[["TKGD_KEY","Tên TKGD","Equity_now","Margin_now_%","IM_total_required"]]
            .drop_duplicates())

if _looks_like_account(q):
    hits = _resolve_account(q, acct_idx, "TKGD_KEY")
    if hits.empty:
        st.warning(f"Không thấy tài khoản: {q}")
        st.stop()
    if len(hits) > 1:
        st.info("Có nhiều tài khoản trùng đuôi. Chọn một:")
        st.dataframe(hits, use_container_width=True)
        st.stop()

    acc_row = hits.iloc[0]
    key = str(acc_row["TKGD_KEY"])

    sub = pos[pos["TKGD_KEY"].astype(str).str.upper()==key.upper()].copy()
    if sub.empty:
        st.info("Tài khoản không có vị thế (sau khi map catalog/alias).")
        st.stop()

    # ====== GIÁ CẦN CHẠM ======
    for t in MARGIN_TARGETS:
        col_need = f"need_to_{t}%_pt"
        need_pt = acct.loc[acct["TKGD_KEY"]==key, col_need].values[0] if col_need in acct.columns else np.nan
        d       = acct.loc[acct["TKGD_KEY"]==key, f"dir_{t}%"].values[0] if f"dir_{t}%" in acct.columns else "down"

        if not pd.notna(need_pt):
            sub[f"Price_to_{t}"]=np.nan; sub[f"Delta_to_{t}_abs"]=np.nan; sub[f"Delta_to_{t}_%"]=np.nan
            sub[f"Reachable_{t}"]=False; sub[f"Note_{t}"]="Thiếu IM/Equity hoặc k_up/k_down."
            continue

        need_frac = float(need_pt) / 100.0
        raw_price = sub["Giá TT"]*(1+need_frac) if d=="up" else sub["Giá TT"]*(1-need_frac)

        tick = sub["TickSize"].fillna(0.0)
        price = pd.Series([(round(p/t)*t if (t and t>0) else p) for p,t in zip(raw_price, tick)], index=sub.index)

        unreachable = (d=="down") & (price<0)
        price_display = price.where(~unreachable, 0.0)

        sub[f"Price_to_{t}"]      = price_display
        sub[f"Delta_to_{t}_abs"]  = price_display - sub["Giá TT"]
        sub[f"Delta_to_{t}_%"]    = (price_display/sub["Giá TT"] - 1.0) * 100.0
        sub[f"Reachable_{t}"]     = ~unreachable
        sub[f"Note_{t}"]          = np.where(unreachable, "Không thể chạm mốc trước khi giá về 0.", "")

    # ====== BẢNG THEO VỊ THẾ (kèm lịch) ======
    cols_pos = ["SP_Nhom","SP_Ten","SP_Base_norm","SP_RawToken","Tên HĐ","Mã HĐ",
                "KL Mua","KL Bán","NetQty","Giá TB","Giá TT",
                "Contract_Multiplier","TickSize","IM_per_lot_USD","IM_row_USD",
                "FND","LTD","Days_to_FND"]
    positions_df = sub[[c for c in cols_pos if c in sub.columns]].copy()

    # ====== FND theo TÀI KHOẢN ======
    soon_mask_acc = positions_df["FND"].notna() & (positions_df["Days_to_FND"]>=0) & (positions_df["Days_to_FND"] <= int(alert_days))
    soon_acc = positions_df.loc[soon_mask_acc, ["Mã HĐ","SP_Ten","FND","LTD","Days_to_FND"]].drop_duplicates().sort_values(["Days_to_FND","Mã HĐ"])

    # ====== BẢNG GIÁ CẦN CHẠM ======
    view_cols = cols_pos + \
        [f"Price_to_{t}" for t in MARGIN_TARGETS] + \
        sum([[f"Delta_to_{t}_abs",f"Delta_to_{t}_%"] for t in MARGIN_TARGETS], []) + \
        [f"Reachable_{t}" for t in MARGIN_TARGETS] + [f"Note_{t}" for t in MARGIN_TARGETS]
    price_by_position_df = sub[[c for c in dict.fromkeys(view_cols).keys() if c in sub.columns]].copy()

    # ====== Gộp theo hợp đồng ======
    grp = pd.DataFrame()
    if {"Mã HĐ","SP_Base_norm"}.issubset(sub.columns):
        agg_map = {"NetQty_contract":("NetQty","sum"),
                   "GiaTT_last":("Giá TT","last"),
                   "PnL_mult":("Contract_Multiplier","first"),
                   "IM_per_lot_USD":("IM_per_lot_USD","first"),
                   "FND":("FND","first"), "LTD":("LTD","first")}
        for t in MARGIN_TARGETS:
            agg_map[f"Price_to_{t}"]=(f"Price_to_{t}","last")
            agg_map[f"Reachable_{t}"]=(f"Reachable_{t}","all")
        grp = (sub.groupby(["SP_Nhom","SP_Ten","SP_Base_norm","Tên HĐ","Mã HĐ"], dropna=False)
                 .agg(**agg_map).reset_index())
        grp["Days_to_FND"] = (pd.to_datetime(grp["FND"]) - today).dt.days

    # ====== TẠO exp_df CHO TAB ĐÁO HẠN HĐ MỞ (FIX NameError) ======
    # Hợp đồng đang mở: NetQty != 0
    open_mask = _safe_num(sub["NetQty"]) != 0
    exp_df = sub.loc[open_mask, ["SP_Ten","Tên HĐ","Mã HĐ","FND","LTD"]].drop_duplicates().copy()
    exp_df["FND"] = pd.to_datetime(exp_df["FND"], errors="coerce")
    exp_df["Expiry"] = pd.to_datetime(exp_df["LTD"], errors="coerce")
    exp_df["Days_to_FND"] = (exp_df["FND"] - today).dt.days
    exp_df["Days_to_Expiry"] = (exp_df["Expiry"] - today).dt.days
    exp_df["FND_Status"] = np.where(exp_df["Days_to_FND"].between(0, int(alert_days), inclusive="both"), "SẮP TỚI",
                                    np.where(exp_df["Days_to_FND"]<0, "ĐÃ QUA", "XA"))
    exp_df["Expiry_Status"] = np.where(exp_df["Days_to_Expiry"].between(0, int(alert_days), inclusive="both"), "SẮP TỚI",
                                       np.where(exp_df["Days_to_Expiry"]<0, "ĐÃ QUA", "XA"))

    n_positions = len(positions_df)
    acc_view = acct.loc[acct["TKGD_KEY"]==key].iloc[0]

    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        ["🔎 Tổng quan","📜 Vị thế (có IM)","🎯 Giá phải chạm","📦 Nhóm SP","🧪 Stress test","🗂️ Xuất file","📅 Đáo hạn HĐ mở"]
    )

    with tab1:
        st.subheader("Tổng quan tài khoản")
        kpi_row(acc_view, n_positions)

        # Hộp cảnh báo FND theo tài khoản
        with st.container():
            if soon_acc.empty:
                st.info(f"Không có hợp đồng sắp **FND** trong {alert_days} ngày.")
            else:
                st.warning(f"⏰ Hợp đồng sắp FND trong {alert_days} ngày: **{len(soon_acc)}**")
                st.dataframe(soon_acc, use_container_width=True)

                # Lưu nhắc lịch riêng cho tài khoản (CSV + ICS)
                if st.button("💾 Lưu nhắc lịch FND (CSV + ICS) — tài khoản đang xem"):
                    df_to_save = soon_acc.assign(TKGD_KEY=key, **{"Tên TKGD": acc_row["Tên TKGD"]})
                    csv_path, ics_path = save_reminders_csv_ics(df_to_save, f"expiry_reminders_{key.replace('/','_')}")
                    st.success(f"Đã lưu: `{csv_path}` và `{ics_path}`")

        try:
            fig = go.Figure(go.Indicator(
                mode="gauge+number", value=float(acc_view["Margin_now_%"]),
                gauge={"axis":{"range":[None, 300]},
                       "threshold":{"line":{"color":"red","width":4},"thickness":0.75,"value":60}},
                title={"text":"Margin % (mốc cảnh báo 60%)"}))
            st.plotly_chart(fig, use_container_width=True)
        except Exception:
            st.info(f"Margin hiện tại: **{float(acc_view['Margin_now_%']):.2f}%**")
        st.dataframe(pd.DataFrame([acc_view])[["TKGD_KEY","Tên TKGD","Equity_now","IM_total_required","Margin_now_%"]],
                     use_container_width=True)

    with tab2:
        st.markdown("#### Vị thế hiện tại (GIỮ rõ **SP_Nhom** + **IM_per_lot_USD**/**IM_row_USD**)")
        st.dataframe(style_positions(positions_df), use_container_width=True)

    with tab3:
        st.markdown("#### Giá cần đạt theo **từng vị thế** (60/50/40%)")
        st.dataframe(style_positions(price_by_position_df.copy()), use_container_width=True)
        st.markdown("#### Giá cần đạt — gộp theo hợp đồng")
        if not grp.empty:
            st.dataframe(grp, use_container_width=True)
        else:
            st.info("Thiếu Mã HĐ / SP_Base_norm để gộp.")

    with tab4:
        st.markdown("#### Thống kê nhóm sản phẩm — tài khoản đang xem")
        stats = build_product_group_stats(sub)
        if stats.empty:
            st.info("Không xác định được nhóm sản phẩm.")
        else:
            st.dataframe(stats, use_container_width=True)
            fig = px.pie(stats, names="SP_Base_norm", values="GrossNotional", hole=0.55,
                         title="Thị phần theo Gross Notional (tài khoản)")
            fig.update_layout(height=420, margin=dict(l=10,r=10,t=60,b=10))
            st.plotly_chart(fig, use_container_width=True)

    with tab5:
        st.markdown("#### Stress test theo % biến động giá")
        shock = st.slider("Chọn mức shock đồng loạt (%)", -20.0, 20.0, 0.0, 0.5)
        stressed = positions_df.copy()
        if "Giá TT" in stressed.columns:
            stressed["Giá_TT_stress"] = stressed["Giá TT"] * (1 + shock/100.0)
        st.dataframe(stressed[[c for c in ["SP_Nhom","SP_Ten","Tên HĐ","Mã HĐ","Giá TT","Giá_TT_stress"] if c in stressed.columns]].round(4),
                     use_container_width=True)

    with tab6:
        st.caption(f"POS: **{pos_sheet}** @header {pos_header} | MGM: **{mgm_sheet}** @header {mgm_header}")
        st.download_button("⬇️ CSV: per-position (có IM)",
                           price_by_position_df.round(6).to_csv(index=False).encode("utf-8"),
                           "gia_can_cham_positions.csv","text/csv")
        if not grp.empty:
            st.download_button("⬇️ CSV: per-contract",
                               grp.round(6).to_csv(index=False).encode("utf-8"),
                               "gia_can_cham_contracts.csv","text/csv")

    # ---------------- TAB 7: ĐÁO HẠN HĐ MỞ (ĐÃ FIX exp_df) ----------------
    with tab7:
        st.markdown("### 📅 Đáo hạn các **hợp đồng đang mở**")
        # KPI nhanh
        nearest_fnd = exp_df["Days_to_FND"].dropna().min() if "Days_to_FND" in exp_df.columns else np.nan
        nearest_exp = exp_df["Days_to_Expiry"].dropna().min() if "Days_to_Expiry" in exp_df.columns else np.nan
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("HĐ đang mở", f"{len(exp_df):,}")
        c2.metric("Sắp FND (≤ ngưỡng)", f"{int((exp_df['FND_Status']=='SẮP TỚI').sum())}")
        c3.metric("Sắp đáo hạn (≤ ngưỡng)", f"{int((exp_df['Expiry_Status']=='SẮP TỚI').sum())}")
        val_min = np.nanmin([nearest_fnd, nearest_exp]) if not (pd.isna(nearest_fnd) and pd.isna(nearest_exp)) else np.nan
        c4.metric("Gần nhất (ngày)", f"{int(val_min) if pd.notna(val_min) else '—'}")

        st.markdown(f"#### 🔔 Sắp **FND** trong ≤ {alert_days} ngày")
        soon_fnd = exp_df.loc[exp_df["FND_Status"]=="SẮP TỚI"].sort_values(["Days_to_FND","Mã HĐ"])
        if soon_fnd.empty:
            st.info("Không có hợp đồng nào sắp FND trong ngưỡng.")
        else:
            st.dataframe(soon_fnd, use_container_width=True)

        st.markdown(f"#### ⏳ Sắp **ĐÁO HẠN** trong ≤ {alert_days} ngày")
        soon_exp = exp_df.loc[exp_df["Expiry_Status"]=="SẮP TỚI"].sort_values(["Days_to_Expiry","Mã HĐ"])
        if soon_exp.empty:
            st.info("Không có hợp đồng nào sắp ĐÁO HẠN trong ngưỡng.")
        else:
            st.dataframe(soon_exp, use_container_width=True)

        st.markdown("#### Danh sách đầy đủ (HĐ đang mở)")
        st.dataframe(exp_df.sort_values(["Expiry","FND","Mã HĐ"]), use_container_width=True)

        # Xuất CSV riêng cho tab này
        st.download_button(
            "⬇️ CSV: Đáo hạn HĐ đang mở (tài khoản)",
            exp_df.to_csv(index=False).encode("utf-8"),
            file_name=f"expiry_open_positions_{key.replace('/','_')}.csv",
            mime="text/csv"
        )

        # Lưu nhắc lịch cho tất cả HĐ mở (FND/Expiry)
        if st.button("💾 Lưu nhắc lịch FND/Expiry (CSV + ICS) — tất cả HĐ đang mở"):
            try:
                data_to_save = exp_df.assign(TKGD_KEY=key, **{"Tên TKGD": acc_row["Tên TKGD"]})
                csv_path, ics_path = save_reminders_csv_ics(
                    data_to_save, f"expiry_all_open_{key.replace('/','_')}"
                )
                st.success(f"Đã lưu: `{csv_path}` và `{ics_path}`")
            except Exception as e:
                st.error(f"Không lưu được file nhắc lịch: {e}")

else:
    # Tìm theo Tên KH
    kh_key = _strip_accents(q)
    idx = acct_idx.copy()
    idx["KH_KEY"] = idx["Tên TKGD"].map(_strip_accents)
    hits = idx[idx["KH_KEY"].str.contains(kh_key, na=False)]
    if hits.empty:
        st.warning(f"Không thấy KH: {q}")
        st.stop()
    st.info("Các tài khoản thuộc KH:")
    st.dataframe(hits[["TKGD_KEY","Tên TKGD"]], use_container_width=True)
    st.caption("→ Nhập chính xác **Mã TKGD** ở ô trên để xem chi tiết.")
