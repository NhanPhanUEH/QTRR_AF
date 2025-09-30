# app.py  — Streamlit webapp: upload 2 Excel, nhập tên KH / mã TKGD → trả bảng + giá cần chạm 60/50/40
import io, re, unicodedata
import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Ký quỹ & Kịch bản giá", layout="wide")

# =============== Utils ===============
DEFAULT_PREFIX = "068C"
MARGIN_TARGETS = [60, 50, 40]

def _strip_accents(s: str) -> str:
    """Bỏ dấu hoàn toàn, chuyển đ/Đ → d, sạch ký tự lạ, lower-case."""
    if s is None: return ""
    s = str(s)
    # normalize + remove combining marks
    s = unicodedata.normalize("NFKD", s).replace("đ", "d").replace("Đ", "D")
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # keep letters/numbers, replace the rest with space
    s = re.sub(r"[^0-9a-zA-Z]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _norm_cols(df: pd.DataFrame):
    return {c: _strip_accents(c) for c in df.columns}

def _find_col(df: pd.DataFrame, candidates, required=False, default=None):
    """Tìm 1 cột theo nhiều biến thể tên (đã strip accents)."""
    norm_map = _norm_cols(df)
    inv = {v:k for k,v in norm_map.items()}
    for cand in candidates:
        key = _strip_accents(cand)
        if key in inv:
            return inv[key]
    if required:
        raise ValueError(f"Thiếu cột bắt buộc: {candidates} — cột có: {list(norm_map.values())}")
    return default

def _to_num(x):
    if isinstance(x, (float,int)) or pd.isna(x): return x
    s = str(x).replace(",", "").replace(" ", "")
    s = s.replace("\u00a0","")  # non-breaking space
    try: return float(s)
    except: return np.nan

def _num_col(df, col, fill=None):
    s = pd.to_numeric(df[col].map(_to_num), errors="coerce") if col in df.columns else pd.Series(np.nan, index=df.index)
    if fill is not None: s = s.fillna(fill)
    return s

def _recompute_netqty(df, col_net="NetQty", col_buy="KL Mua", col_sell="KL Bán", col_basis="Qty_basis"):
    a = _num_col(df, col_net)
    b = _num_col(df, col_buy, 0)
    c = _num_col(df, col_sell, 0)
    d = _num_col(df, col_basis)
    a = a.where(a.notna(), b - c)
    a = a.where(a.notna(), d)
    return a.fillna(0.0)

def _looks_like_account(q):
    Q = str(q).strip().upper()
    return bool(re.search(r"[0-9A-Z]", Q)) or Q.endswith("-A")

def _full_acc_from_suffix(suffix):
    s = str(suffix).strip().upper()
    if s.startswith(DEFAULT_PREFIX): return s
    m = re.fullmatch(r"(\d{4,})(-A)?", s)
    if m: return f"{DEFAULT_PREFIX}{m.group(1)}{m.group(2) or ''}"
    return s

def _resolve_account(q, acc_df, col_key):
    Q = str(q).strip().upper()
    hit = acc_df[acc_df[col_key].astype(str).str.upper() == Q]
    if not hit.empty: return hit
    Q2 = _full_acc_from_suffix(Q)
    hit = acc_df[acc_df[col_key].astype(str).str.upper() == Q2]
    if not hit.empty: return hit
    suf = acc_df[acc_df[col_key].astype(str).str.upper().str.endswith(Q)]
    if suf.empty and Q2!=Q:
        suf = acc_df[acc_df[col_key].astype(str).str.upper().str.endswith(Q2)]
    return suf

# =============== Sidebar inputs ===============
st.sidebar.header("Tải dữ liệu")
pos_file = st.sidebar.file_uploader("1) Trạng thái mở (Excel)", type=["xlsx","xls"])
mgm_file = st.sidebar.file_uploader("2) Danh sách quản lý ký quỹ (Excel)", type=["xlsx","xls"])
debug = st.sidebar.checkbox("Hiển thị debug cột")

st.title("Tra cứu ký quỹ & Giá phải chạm (60/50/40%)")

if not pos_file or not mgm_file:
    st.info("Hãy tải **cả hai** file Excel để bắt đầu.")
    st.stop()

# =============== Read files (sheet auto) ===============
def _read_first_sheet(f):
    xl = pd.ExcelFile(f)
    # ưu tiên sheet có chữ 'sheet1' hoặc sheet đầu tiên
    sheet = None
    for s in xl.sheet_names:
        if _strip_accents(s) in ("sheet1","trang thai mo","trangthai mo"): sheet = s; break
    if sheet is None: sheet = xl.sheet_names[0]
    df = xl.parse(sheet, header=0)
    return df, sheet

pos_raw, pos_sheet = _read_first_sheet(pos_file)
mgm_raw, mgm_sheet = _read_first_sheet(mgm_file)

if debug:
    st.caption(f"POS sheet: **{pos_sheet}** | MARGIN sheet: **{mgm_sheet}**")
    st.write("Cột POS (chuẩn hoá):", list(_norm_cols(pos_raw).values()))
    st.write("Cột MARGIN (chuẩn hoá):", list(_norm_cols(mgm_raw).values()))

# =============== Map columns ===============
# POS
c_tkgd   = _find_col(pos_raw, ["mã tkgd","ma tkgd","tkgd"], required=True)
c_ten    = _find_col(pos_raw, ["tên tkgd","ten tkgd","khach hang","khach hang ten"], required=True)
c_mahd   = _find_col(pos_raw, ["mã hđ","ma hd","ma hop dong","mã hợp đồng"], required=True)
c_tenhd  = _find_col(pos_raw, ["tên hđ","ten hd","ten hop dong","tên hợp đồng"], required=True)
c_sp     = _find_col(pos_raw, ["sp_magd","mã giao dịch","ma gd","ma hang","ma sp"], required=False)
c_buy    = _find_col(pos_raw, ["kl mua","so luong mua"], required=False)
c_sell   = _find_col(pos_raw, ["kl bán","kl ban","so luong ban"], required=False)
c_net    = _find_col(pos_raw, ["netqty","net qty","kl rong"], required=False)
c_basis  = _find_col(pos_raw, ["qty_basis","qty basis","co so kl"], required=False)
c_giatb  = _find_col(pos_raw, ["giá tb","gia tb","gia vao lenh"], required=True)
c_giatt  = _find_col(pos_raw, ["giá tt","gia tt","gia thi truong","gia thuc te"], required=True)
c_mult   = _find_col(pos_raw, ["contract_multiplier","multiplier","he so lot"], required=False)

pos = pos_raw.copy()
pos["TKGD_KEY"] = pos[c_tkgd].astype(str)
pos["Tên TKGD"] = pos[c_ten].astype(str)
pos["Mã HĐ"]     = pos[c_mahd].astype(str)
pos["Tên HĐ"]    = pos[c_tenhd].astype(str)
pos["SP_MaGD"]   = pos[c_sp].astype(str) if c_sp else ""
pos["Giá TB"]    = _num_col(pos, c_giatb)
pos["Giá TT"]    = _num_col(pos, c_giatt)
pos["Contract_Multiplier"] = _num_col(pos, c_mult, 1.0) if c_mult else 1.0
pos["KL Mua"]    = _num_col(pos, c_buy, 0.0) if c_buy else 0.0
pos["KL Bán"]    = _num_col(pos, c_sell, 0.0) if c_sell else 0.0
pos["Qty_basis"] = _num_col(pos, c_basis)

pos["NetQty"]    = _recompute_netqty(pos, col_net="NetQty", col_buy="KL Mua", col_sell="KL Bán", col_basis="Qty_basis")

# MARGIN
m_tkgd  = _find_col(mgm_raw, ["mã tkgd","ma tkgd","tkgd"], required=True)
m_ten   = _find_col(mgm_raw, ["tên tkgd","ten tkgd"], required=True)
m_ratio = _find_col(mgm_raw, ["tỷ lệ ký quỹ hiện tại","ty le ky quy hien tai","margin_now","margin ratio"], required=True)
m_equ   = _find_col(mgm_raw, ["giá trị ròng ký quỹ (usd)","gia tri rong ky quy (usd)","equity usd","equity_now"], required=True)

mgm = mgm_raw.copy()
mgm["TKGD_KEY"] = mgm[m_tkgd].astype(str)
mgm["Tên TKGD"] = mgm[m_ten].astype(str)
mgm["Margin_now_%"] = _num_col(mgm, m_ratio)
mgm["Equity_now"]   = _num_col(mgm, m_equ)

# ====== IM (tổng IM kỳ vọng) ======
# Nếu file vị thế đã có IM_row_expect có thể sum; nếu chưa có, dùng tạm cờ "không biết" nhưng **không dừng app**
c_imrow = _find_col(pos_raw, ["im_row_expect","im expect","im_row"], required=False)
if c_imrow:
    pos["IM_row_expect"] = _num_col(pos, c_imrow, 0.0)
    im_sum = pos.groupby("TKGD_KEY", dropna=False)["IM_row_expect"].sum().reset_index().rename(columns={"IM_row_expect":"IM_total_expect"})
else:
    # fallback: suy ra từ Margin_now_% và Equity_now nếu có, để tiếp tục tính mốc
    #  IM ≈ Equity_now / (Margin_now_%/100)
    tmp = mgm.copy()
    tmp["IM_total_expect"] = tmp["Equity_now"] / (tmp["Margin_now_%"]/100.0)
    im_sum = tmp[["TKGD_KEY","IM_total_expect"]].copy()

acct = (mgm[["TKGD_KEY","Tên TKGD","Equity_now","Margin_now_%"]]
        .merge(im_sum, on="TKGD_KEY", how="left"))

# ================== TÍNH % shock bất lợi & GIÁ PHẢI CHẠM ==================
# độ nhạy “bất lợi” theo 1% thay đổi GIÁ TT (theo từng dòng)
base = pos.copy()
base["__qty"]  = pos["NetQty"]
base["__mult"] = pos["Contract_Multiplier"]
base["__tt"]   = pos["Giá TT"]

# 1% PnL theo hướng tăng/giảm
base["dPnL_up_1pct"]   = base["__qty"] * base["__mult"] * (base["__tt"] * 0.01)     # giá ↑1%
base["dPnL_down_1pct"] = -base["dPnL_up_1pct"]                                     # giá ↓1%
# phần **bất lợi**
base["adverse_up"]   = np.where(base["dPnL_up_1pct"]   < 0, base["dPnL_up_1pct"],   0.0)
base["adverse_down"] = np.where(base["dPnL_down_1pct"] < 0, base["dPnL_down_1pct"], 0.0)

acc_delta = (base.groupby("TKGD_KEY", dropna=False)
                .agg(k_up=("adverse_up","sum"), k_down=("adverse_down","sum"))
                .reset_index())

acct = acct.merge(acc_delta, on="TKGD_KEY", how="left").fillna({"k_up":0.0,"k_down":0.0})

def _need_pct(eq, im, k_dir, target_pct):
    """Bao nhiêu % biên động (theo hướng bất lợi) để Equity chạm target_pct*IM."""
    if im is None or not np.isfinite(im) or im <= 0: return np.nan
    tgt = im * (target_pct/100.0)
    if k_dir == 0:
        # không có độ nhạy (không có vị thế hoặc Giá TT=0) → coi như vô hạn nếu đang cao hơn target
        return np.inf if (np.isfinite(eq) and eq > tgt) else 0.0
    x = (tgt - (eq if np.isfinite(eq) else 0.0)) / k_dir
    return float(max(0.0, x))

rows = []
for _, r in acct.iterrows():
    im, eq, kup, kdn = r["IM_total_expect"], r["Equity_now"], r["k_up"], r["k_down"]
    need = {}
    dire = {}
    for t in MARGIN_TARGETS:
        x_up = _need_pct(eq, im, kup, t)
        x_dn = _need_pct(eq, im, kdn, t)
        choose = ("up", x_up) if (np.nan_to_num(x_up, nan=np.inf) < np.nan_to_num(x_dn, nan=np.inf)) else ("down", x_dn)
        need[f"need_to_{t}%"] = choose[1]
        dire[f"dir_{t}%"]     = choose[0]
    rows.append({"TKGD_KEY": r["TKGD_KEY"], **need, **dire})
thr = pd.DataFrame(rows)

acct = acct.merge(thr, on="TKGD_KEY", how="left")

# ============== INPUT TRA CỨU ==============
q = st.text_input("Nhập **Tên KH** hoặc **Mã TKGD** (chấp nhận 4–7 số cuối, ví dụ `0006886-A`):").strip()

if q:
    if _looks_like_account(q):
        hits = _resolve_account(q, acct, "TKGD_KEY")
        if hits.empty:
            st.warning(f"Không thấy tài khoản: {q}")
            st.stop()
        if len(hits) > 1:
            st.info("Có nhiều tài khoản trùng đuôi. Chọn một:")
            st.dataframe(hits[["TKGD_KEY","Tên TKGD","Margin_now_%","Equity_now","IM_total_expect"]])
            st.stop()
        acc = hits.iloc[0]
        key = str(acc["TKGD_KEY"])

        st.subheader("1) Tổng quan tài khoản")
        st.dataframe(pd.DataFrame([acc])[["TKGD_KEY","Tên TKGD","IM_total_expect","Equity_now","Margin_now_%"]])

        # 2) Giá cần đạt cho TỪNG VỊ THẾ
        sub = pos[pos["TKGD_KEY"].astype(str).str.upper()==key.upper()].copy()
        if sub.empty:
            st.info("Tài khoản không có vị thế.")
            st.stop()

        # map % + hướng bất lợi
        for t in MARGIN_TARGETS:
            need = float(acc.get(f"need_to_{t}%", np.nan))
            d    = acc.get(f"dir_{t}%", "down")
            price = sub["Giá TT"] * (1 + need/100.0) if d=="up" else sub["Giá TT"] * (1 - need/100.0)
            sub[f"Price_to_{t}"] = price
            sub[f"Delta_to_{t}_abs"] = price - sub["Giá TT"]
            sub[f"Delta_to_{t}_%"]   = (price/sub["Giá TT"] - 1.0) * 100.0

        view_cols = ["Tên HĐ","Mã HĐ","SP_MaGD","NetQty","Giá TT","Contract_Multiplier"] + \
                    [f"Price_to_{t}" for t in MARGIN_TARGETS] + \
                    sum([[f"Delta_to_{t}_abs",f"Delta_to_{t}_%"] for t in MARGIN_TARGETS], [])
        view_cols = [c for c in view_cols if c in sub.columns]

        st.subheader("2) Giá cần đạt theo **TỪNG VỊ THẾ** để ký quỹ CHẠM 60/50/40% (áp dụng hướng bất lợi)")
        st.dataframe(sub[view_cols].round(6))

        # 3) Gộp theo HỢP ĐỒNG (mỗi mã 1 dòng)
        if {"Mã HĐ","SP_MaGD"}.issubset(sub.columns):
            grp = (sub.groupby(["Tên HĐ","Mã HĐ","SP_MaGD"], dropna=False)
                      .agg(NetQty_contract=("NetQty","sum"),
                           GiaTT_last=("Giá TT","last"),
                           PnL_mult_USD=("Contract_Multiplier","first"),
                           **{f"Price_to_{t}": (f"Price_to_{t}","last") for t in MARGIN_TARGETS})
                   ).reset_index()
            st.subheader("3) **HỢP ĐỒNG** — Giá cần đạt (mỗi hợp đồng 1 dòng)")
            st.dataframe(grp.round(6))
        else:
            st.info("Không thể gộp theo hợp đồng do thiếu cột Mã HĐ / SP_MaGD.")

    else:
        # Tra theo tên KH (gom các TK)
        kh_key = _strip_accents(q)
        # dựng chỉ mục tên-không-dấu
        idx = pos[["TKGD_KEY","Tên TKGD"]].drop_duplicates().copy()
        idx["KH_KEY"] = idx["Tên TKGD"].map(_strip_accents)
        hits = idx[idx["KH_KEY"].str.contains(kh_key, na=False)]
        if hits.empty:
            st.warning(f"Không thấy KH: {q}")
            st.stop()
        st.info("Các tài khoản thuộc KH:")
        st.dataframe(hits[["TKGD_KEY","Tên TKGD"]])
        st.caption("→ Nhập chính xác **Mã TKGD** ở ô trên để xem chi tiết giá phải chạm cho từng vị thế.")
