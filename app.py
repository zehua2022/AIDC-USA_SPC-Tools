# -*- coding: utf-8 -*-
# ──────────────────────────────────────────────────────────────────────────────
# SPC Tool（Web 版，精簡整理版）
# 功能：
# 1) 多檔 CSV 上傳 → 清理(移空列/去無用資訊/套標題) → Main/Ref 分群 → 交錯整併
# 2) 匯出 merged_groups_result.xlsx
# 3) 頁面直接查詢：輸入「圈碼(數字) + 群組類型(Main/Ref)」→ 繪 Upper/Dimension/Lower 與各來源散點
# 4) 修正：欄名重複（補充1/補充2 + 欄名唯一化）；使用快取與 session_state 保存處理結果
# 依賴：streamlit, pandas, numpy, matplotlib, openpyxl, XlsxWriter
# ──────────────────────────────────────────────────────────────────────────────

import io
import re
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# === 固定欄位標題（12 欄）→ 避免重複欄名 ===
COLUMNS = [
    "圈碼", "程式位置", "補充1", "類型", "值",
    "目標尺碼", "量測值", "上偏差", "下偏差",
    "補充2", "差異值", "過切值"
]

# === 解析圈碼的 Regex（Main/Ref 分群）===
NO_ANY          = re.compile(r'(?i)\bno\.?\s*(\d+)')                # 任意位置的 No.xxx（擷取數字）
NO_HEADER_FULL  = re.compile(r'(?i)^\s*no\.?\s*(\d+)\s*$')          # 只有 No.xxx 視為「主群組標頭」
REF_ANY         = re.compile(r'(?i)\bref\b')                        # 含有 Ref
REF_HEADER_FULL = re.compile(r'(?i)^\s*[\*\.\s]*ref\b.*?\bno\.?\s*(\d+)[^\d]*$')  # 參考群組標頭

# ──────────────────────────────────────────────────────────────────────────────
# 公用：欄名唯一化（Arrow/Streamlit 需唯一欄名）
# ──────────────────────────────────────────────────────────────────────────────
def make_unique_columns(cols: list[str]) -> list[str]:
    seen, out = {}, []
    for c in cols:
        if c in seen:
            seen[c] += 1
            out.append(f"{c}{seen[c]}")  # 補上流水號
        else:
            seen[c] = 0
            out.append(c)
    return out

# ──────────────────────────────────────────────────────────────────────────────
# 清理單一 CSV：移空列→刪無用資訊→套標題→欄名唯一化
# ──────────────────────────────────────────────────────────────────────────────
def clean_one_csv(df: pd.DataFrame, name: str) -> pd.DataFrame:
    df = df.dropna(how="all")
    for col in df.columns:
        if df[col].dtype == "object":
            df = df[~df[col].astype(str).str.contains(r"Part Name|Order No\.|Date:", na=False)]
    df = df.reset_index(drop=True)
    if len(df.columns) == len(COLUMNS):
        df.columns = COLUMNS
    else:
        st.warning(f"⚠️ {name} 欄位數量與預期不符（{len(df.columns)}≠12），未套用標題。")
    df.columns = make_unique_columns(list(df.columns))
    return df

# ──────────────────────────────────────────────────────────────────────────────
# 分群與序號：No.xxx(Main) 與 Ref(No.xxx)(Ref) 區分、嚴格標頭、資料列編序
# ──────────────────────────────────────────────────────────────────────────────
def tag_groups_and_sequence(df: pd.DataFrame, src_name: str) -> pd.DataFrame:
    if "圈碼" not in df.columns:
        df["圈碼"] = pd.NA

    df = df.copy()
    df["是否群組標頭"] = False
    df["群組"] = pd.NA              # 舊相容：純數字
    df["群組數字"] = pd.NA          # 新：純數字
    df["群組類型"] = pd.NA          # 新：Main / Ref

    DATA_COLS = [c for c in ["程式位置","類型","值","目標尺碼","量測值","上偏差","下偏差","差異值","過切值"] if c in df.columns]

    current_group_num = None
    current_group_type = None  # "Main" or "Ref"

    for i, val in enumerate(df["圈碼"]):
        s = "" if pd.isna(val) else str(val)

        m_no = NO_ANY.search(s)
        grp_num = m_no.group(1) if m_no else None
        is_ref_like = bool(REF_ANY.search(s))

        # 該行是否無實質資料（判定標頭用）
        right_all_empty = True
        for c in DATA_COLS:
            v = df.at[i, c]
            if not (pd.isna(v) or str(v).strip() == ""):
                right_all_empty = False
                break

        # 嚴格標頭判定
        is_header, header_type = False, None
        if grp_num is not None and right_all_empty:
            if NO_HEADER_FULL.match(s):
                is_header, header_type = True, "Main"
            elif REF_HEADER_FULL.match(s):
                is_header, header_type = True, "Ref"

        # 有抓到 No.xxx 就更新目前群組（即便非標頭）
        if grp_num is not None:
            current_group_num = grp_num
            current_group_type = "Ref" if is_ref_like else "Main"

        # 套用群組欄位
        if current_group_num is not None:
            df.at[i, "群組"] = current_group_num
            df.at[i, "群組數字"] = current_group_num
            df.at[i, "群組類型"] = current_group_type

        # 寫入嚴格標頭
        if is_header:
            df.at[i, "是否群組標頭"] = True
            df.at[i, "群組類型"] = header_type  # 以標頭判定為準

    # 在每個 (群組數字, 群組類型) 內，對非標頭列編「序號」
    df["序號"] = pd.NA
    for (gnum, gtype), gdf in df.groupby(["群組數字","群組類型"], dropna=True, sort=False):
        seq = 0
        for idx, row in gdf.iterrows():
            if not row["是否群組標頭"]:
                seq += 1
                df.at[idx, "序號"] = seq

    df["來源檔名"] = src_name
    df["序號"] = pd.to_numeric(df["序號"], errors="coerce")
    return df

# ──────────────────────────────────────────────────────────────────────────────
# 交錯排序 + 標頭去重：同 (群組數字, 群組類型) 保留一條標頭
# ──────────────────────────────────────────────────────────────────────────────
def interleave_and_dedup_headers(combined: pd.DataFrame) -> pd.DataFrame:
    type_order = {"Main": 0, "Ref": 1}
    c = combined.copy()

    # 初次排序鍵
    c["__grp_num_sort"] = pd.to_numeric(c.get("群組數字"), errors="coerce")
    c["__type_sort"]    = c.get("群組類型").map(type_order).fillna(99).astype(int)
    c["__header_rank"]  = c["是否群組標頭"].apply(lambda x: 0 if bool(x) else 1)
    c["__seq_sort"]     = pd.to_numeric(c.get("序號"), errors="coerce")

    sorted_once = c.sort_values(
        by=["__grp_num_sort","群組數字","__type_sort","群組類型","__header_rank","__seq_sort","來源檔名"],
        ascending=[True,True,True,True,True,True,True],
        kind="mergesort"
    )

    # 每個 (群組數字, 群組類型) 的標頭僅保留第一筆
    mask_header = sorted_once["是否群組標頭"]
    headers_one = sorted_once.loc[mask_header].drop_duplicates(subset=["群組數字","群組類型"], keep="first")
    non_headers = sorted_once.loc[~mask_header]
    merged_once = pd.concat([headers_one, non_headers], ignore_index=True)

    # 最終排序：確保標頭在前
    m = merged_once
    m["__grp_num_sort"] = pd.to_numeric(m.get("群組數字"), errors="coerce")
    m["__type_sort"]    = m.get("群組類型").map(type_order).fillna(99).astype(int)
    m["__header_rank"]  = m["是否群組標頭"].apply(lambda x: 0 if bool(x) else 1)
    m["__seq_sort"]     = pd.to_numeric(m.get("序號"), errors="coerce")

    final_combined = m.sort_values(
        by=["__grp_num_sort","群組數字","__type_sort","群組類型","__header_rank","__seq_sort","來源檔名"],
        ascending=[True,True,True,True,True,True,True],
        kind="mergesort"
    ).drop(columns=["__grp_num_sort","__type_sort","__header_rank","__seq_sort"])

    # 欄名最後再保險一次唯一化
    final_combined.columns = make_unique_columns(list(final_combined.columns))
    return final_combined

# ──────────────────────────────────────────────────────────────────────────────
# 繪圖：Upper / Dimension / Lower + 各來源檔名散點；X 軸整數；圖例圖外右側
# ──────────────────────────────────────────────────────────────────────────────
def plot_group(sub: pd.DataFrame, grp_num: str, grp_type: str):
    for col in ["序號", "目標尺碼", "上偏差", "下偏差", "量測值"]:
        sub[col] = pd.to_numeric(sub[col], errors="coerce")
    sub = sub.dropna(subset=["序號", "目標尺碼"]).sort_values(["序號", "來源檔名"])

    sub["上界線"] = sub["目標尺碼"] + sub["上偏差"].fillna(0)
    sub["名義線"] = sub["目標尺碼"]
    sub["下界線"] = sub["目標尺碼"] - sub["下偏差"].fillna(0)

    base = sub.groupby("序號", as_index=False).first()[["序號","上界線","名義線","下界線"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(base["序號"], base["上界線"], label="Upper")
    ax.plot(base["序號"], base["名義線"], label="Dimension")
    ax.plot(base["序號"], base["下界線"], label="Lower")

    for src, g in sub.groupby("來源檔名"):
        g2 = g.dropna(subset=["量測值", "序號"])
        if not g2.empty:
            ax.scatter(g2["序號"], g2["量測值"], label=f"{src}")

    ax.set_title(f"No.{grp_num} ({grp_type}) SPC")
    ax.set_xlabel("Part Order）")
    ax.set_ylabel("Dimension")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))  # 圖外右側
    ax.grid(True)
    fig.tight_layout(rect=[0, 0, 0.8, 1])
    return fig

# ──────────────────────────────────────────────────────────────────────────────
# 快取處理：把上傳檔案內容(bytes)交進來，回傳 combined_long 與 merged xlsx 的 bytes
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=True)
def process_files(files_payload: list[dict]) -> tuple[pd.DataFrame, bytes]:
    parts = []
    for item in files_payload:
        name = item["name"]
        df_raw = pd.read_csv(io.BytesIO(item["bytes"]))
        df_clean = clean_one_csv(df_raw, name)
        df_tagged = tag_groups_and_sequence(df_clean, name)
        parts.append(df_tagged)

    combined = pd.concat(parts, ignore_index=True)
    combined_long = interleave_and_dedup_headers(combined)
    combined_long.columns = make_unique_columns(list(combined_long.columns))  # 保險

    out_buf = io.BytesIO()
    with pd.ExcelWriter(out_buf, engine="xlsxwriter") as writer:
        combined_long.to_excel(writer, sheet_name="combined_long", index=False)
        group_counts = (
            combined_long.loc[~combined_long["是否群組標頭"]]
            .groupby(["群組數字","群組類型","來源檔名"], dropna=True)
            .size().rename("列數").reset_index()
            .sort_values(["群組數字","群組類型","來源檔名"])
        )
        group_counts.to_excel(writer, sheet_name="group_counts", index=False)
        seq_alignment = (
            combined_long.loc[~combined_long["是否群組標頭"]]
            .groupby(["群組數字","群組類型","序號","來源檔名"], dropna=True)
            .size().rename("count").reset_index()
            .pivot_table(index=["群組數字","群組類型","序號"], columns="來源檔名", values="count", fill_value=0)
            .sort_index()
        )
        seq_alignment.to_excel(writer, sheet_name="sequence_alignment")
    out_buf.seek(0)
    return combined_long, out_buf.read()

# ──────────────────────────────────────────────────────────────────────────────
# Streamlit 介面
# ──────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="SPC Tool (Web)", layout="wide")
st.title("SPC Tool（Web 版）")

# 初始化 session_state
for key in ("files_payload", "combined_long", "merged_xlsx"):
    if key not in st.session_state:
        st.session_state[key] = None

# 側欄：上傳與處理
with st.sidebar:
    st.header("1) 上傳 CSV（可多個）")
    files = st.file_uploader("選擇 CSV", type=["csv"], accept_multiple_files=True, key="csv_uploader")

    if st.button("清理 + 分群 + 合併"):
        if not files:
            st.warning("請先上傳至少一個 CSV。")
        else:
            payload = [{"name": f.name, "bytes": f.getvalue()} for f in files]
            st.session_state["files_payload"] = payload
            combined_long, merged_xlsx = process_files(payload)
            st.session_state["combined_long"] = combined_long
            st.session_state["merged_xlsx"] = merged_xlsx
            st.success("✅ 合併完成（資料已保存，可直接查詢圈碼）")

    # 下載 merged xlsx
    if st.session_state["merged_xlsx"]:
        st.download_button(
            "下載 merged_groups_result.xlsx",
            data=st.session_state["merged_xlsx"],
            file_name="merged_groups_result.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# 主區：顯示合併後資料
combined_long = st.session_state["combined_long"]
if combined_long is not None:
    st.header("2) 合併後資料")
    st.dataframe(combined_long, use_container_width=True, height=300)

    # 查詢與繪圖
    st.header("3) 查詢與繪圖（不需重上傳）")
    c1, c2, c3 = st.columns(3)
    grp_num = c1.text_input("圈碼（只需數字，例如 92）", "")
    grp_type = c2.selectbox("群組類型", ["Main","Ref"], index=0)
    if c3.button("繪製 SPC 圖"):
        if grp_num.strip().isdigit():
            sub = combined_long[
                (combined_long["群組數字"].astype(str) == grp_num.strip()) &
                (combined_long["群組類型"] == grp_type) &
                (~combined_long["是否群組標頭"])
            ].copy()
            if sub.empty:
                st.info(f"找不到群組 No.{grp_num}（{grp_type}）的資料。")
            else:
                fig = plot_group(sub, grp_num.strip(), grp_type)
                st.pyplot(fig, use_container_width=True)
        else:
            st.warning("請輸入純數字的圈碼（例如 92）。")

# 可選：直接讀 merged 檔查詢
with st.expander("只用 merged_groups_result.xlsx 進行查詢（可選）"):
    merged_up = st.file_uploader("上傳 merged_groups_result.xlsx", type=["xlsx"], key="merged_uploader")
    if merged_up is not None and st.button("載入 merged 供查詢"):
        try:
            xl = pd.ExcelFile(merged_up)
            combined_from_merged = xl.parse("combined_long")
            combined_from_merged.columns = make_unique_columns(list(combined_from_merged.columns))
            st.session_state["combined_long"] = combined_from_merged
            st.info("✅ 已載入 merged 的 combined_long，現在可直接查詢圈碼。")
        except Exception as e:
            st.error(f"讀取失敗：{e}")

