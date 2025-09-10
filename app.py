# -*- coding: utf-8 -*-
import io
import os
import re
import numpy as np
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

# ===== 固定欄位名稱（若欄數=12 就套用）=====
COLUMNS = [
    "圈碼", "程式位置", "補充", "類型", "值",
    "目標尺碼", "量測值", "上偏差", "下偏差",
    "補充", "差異值", "過切值"
]

# --- regex：擷取主群組 / 參考群組 ---
NO_ANY = re.compile(r'(?i)\bno\.?\s*(\d+)')                # 任意位置的 No.xxx（抽數字用）
NO_HEADER_FULL = re.compile(r'(?i)^\s*no\.?\s*(\d+)\s*$')  # 僅有 No.xxx 視為「主群組標頭」
REF_ANY = re.compile(r'(?i)\bref\b')                       # 是否包含 Ref 字樣
# 放寬的「參考群組標頭」判定：一整行以 Ref 主體且包含 No.xxx
REF_HEADER_FULL = re.compile(r'(?i)^\s*[\*\.\s]*ref\b.*?\bno\.?\s*(\d+)[^\d]*$')

# -------------- 資料清理 --------------
def clean_one_csv(file_like, file_name: str) -> pd.DataFrame:
    """
    讀取並清理單一 CSV（支援 BytesIO / 上傳物件）：
    1) 移除全空列
    2) 刪除任何欄位包含 'Part Name' / 'Order No.' / 'Date:' 的列
    3) 欄位數量為 12 則套用固定欄位名
    回傳清理後的 DataFrame（不在網頁版落地 _clean.csv）
    """
    df = pd.read_csv(file_like)
    # 1) 移除全空白列
    df = df.dropna(how="all")
    # 2) 移除包含關鍵字的列（檢查所有文字欄位）
    for col in df.columns:
        if df[col].dtype == "object":
            df = df[~df[col].astype(str).str.contains(r"Part Name|Order No\.|Date:", na=False)]
    df = df.reset_index(drop=True)
    # 3) 套用固定欄位名（若欄數符合）
    if len(df.columns) == len(COLUMNS):
        df.columns = COLUMNS
    else:
        st.warning(f"⚠️ {file_name} 欄位數量與預期不符（{len(df.columns)}≠12），無法套用標題。")
    return df

# -------------- 分群 + 序號 --------------
def tag_groups_and_sequence(df: pd.DataFrame, src_name: str) -> pd.DataFrame:
    """
    嚴格分群與標頭判定：
      - Main 群組：含 No.xxx，且「圈碼」整列完全等於 No.xxx 且右側欄位空 → 主群組標頭
      - Ref  群組：含 Ref 且含 No.xxx，且右側欄位空 → 參考群組標頭
    若同列同時有 No.xxx 但右側已有資料，視為「資料列」（非標頭），同時更新目前群組。
    產生欄位：是否群組標頭 / 群組（數字）/ 群組數字 / 群組類型(Main/Ref) / 序號 / 來源檔名
    """
    if "圈碼" not in df.columns:
        df["圈碼"] = pd.NA

    df = df.copy()
    df["是否群組標頭"] = False
    df["群組"] = pd.NA           # 舊欄位：純數字
    df["群組數字"] = pd.NA       # 新欄位：純數字
    df["群組類型"] = pd.NA       # 新欄位：Main / Ref

    # 判斷「同列是否為資料列而非標頭」的關鍵欄
    DATA_COLS = [c for c in ["程式位置","類型","值","目標尺碼","量測值","上偏差","下偏差","差異值","過切值"] if c in df.columns]

    current_group_num = None
    current_group_type = None  # "Main" or "Ref"

    for i, val in enumerate(df["圈碼"]):
        s = "" if pd.isna(val) else str(val)

        # 抽任何 No.xxx
        m_no_any = NO_ANY.search(s)
        grp_num = m_no_any.group(1) if m_no_any else None

        # 是否 Ref 類型
        is_ref_like = bool(REF_ANY.search(s))

        # 右側關鍵欄位是否全空
        right_all_empty = True
        for col in DATA_COLS:
            v = df.at[i, col]
            if not (pd.isna(v) or str(v).strip() == ""):
                right_all_empty = False
                break

        # 是否為「標頭」
        is_header = False
        header_type = None  # "Main" / "Ref"

        if grp_num is not None and right_all_empty:
            if NO_HEADER_FULL.match(s):
                is_header = True
                header_type = "Main"
            elif REF_HEADER_FULL.match(s):
                is_header = True
                header_type = "Ref"

        # 更新目前群組（本列若含 No.xxx，不管是否標頭，都更新 current）
        if grp_num is not None:
            cur_type = "Ref" if is_ref_like else "Main"
            current_group_num = grp_num
            current_group_type = cur_type

        # 設定群組欄位
        if current_group_num is not None:
            df.at[i, "群組"] = current_group_num
            df.at[i, "群組數字"] = current_group_num
            df.at[i, "群組類型"] = current_group_type

        # 標記是否群組標頭（以嚴格規則）
        if is_header:
            df.at[i, "是否群組標頭"] = True
            df.at[i, "群組類型"] = header_type  # 標頭類型以判定為準

    # 在各 (群組數字, 群組類型) 內，對「非群組標頭列」編序號
    df["序號"] = pd.NA
    if "群組數字" in df.columns and "群組類型" in df.columns:
        for (gnum, gtype), gdf in df.groupby(["群組數字","群組類型"], dropna=True, sort=False):
            seq = 0
            for idx, row in gdf.iterrows():
                if not row["是否群組標頭"]:
                    seq += 1
                    df.at[idx, "序號"] = seq

    df["來源檔名"] = src_name
    df["序號"] = pd.to_numeric(df["序號"], errors="coerce")
    return df

# -------------- 交錯排序 + 標頭去重 --------------
def interleave_and_dedup_headers(combined: pd.DataFrame) -> pd.DataFrame:
    """
    交錯排序，並將同一 (群組數字, 群組類型) 的群組標頭只保留 1 列。
    排序鍵：群組數字 → 群組類型(Main 再 Ref) → 標頭先 → 序號 → 來源檔名
    """
    type_order = {"Main": 0, "Ref": 1}
    combined = combined.copy()

    combined["__grp_num_sort"] = pd.to_numeric(combined.get("群組數字"), errors="coerce")
    combined["__type_sort"]    = combined.get("群組類型").map(type_order).fillna(99).astype(int)
    combined["__header_rank"]  = combined["是否群組標頭"].apply(lambda x: 0 if bool(x) else 1)
    combined["__seq_sort"]     = pd.to_numeric(combined.get("序號"), errors="coerce")

    sorted_once = combined.sort_values(
        by=["__grp_num_sort", "群組數字", "__type_sort", "群組類型", "__header_rank", "__seq_sort", "來源檔名"],
        ascending=[True,            True,         True,         True,            True,          True,        True],
        kind="mergesort"
    )

    # 同 (群組數字, 群組類型) 的標頭只保留第一筆
    mask_header = sorted_once["是否群組標頭"]
    headers_one = sorted_once.loc[mask_header].drop_duplicates(subset=["群組數字","群組類型"], keep="first")
    non_headers = sorted_once.loc[~mask_header]
    merged_once = pd.concat([headers_one, non_headers], ignore_index=True)

    # 最終排序（確保標頭仍在各群組最前）
    merged_once["__grp_num_sort"] = pd.to_numeric(merged_once.get("群組數字"), errors="coerce")
    merged_once["__type_sort"]    = merged_once.get("群組類型").map(type_order).fillna(99).astype(int)
    merged_once["__header_rank"]  = merged_once["是否群組標頭"].apply(lambda x: 0 if bool(x) else 1)
    merged_once["__seq_sort"]     = pd.to_numeric(merged_once.get("序號"), errors="coerce")

    final_combined = merged_once.sort_values(
        by=["__grp_num_sort", "群組數字", "__type_sort", "群組類型", "__header_rank", "__seq_sort", "來源檔名"],
        ascending=[True,            True,         True,         True,            True,          True,        True],
        kind="mergesort"
    ).drop(columns=["__grp_num_sort","__type_sort","__header_rank","__seq_sort"])

    return final_combined

# -------------- 畫圖 --------------
def plot_group_matplotlib(sub: pd.DataFrame, grp_num: str, grp_type: str):
    """以 matplotlib 畫三條線 + 多來源散點；X 軸整數刻度；圖例置於圖外右側。"""
    # 數值化 & 排序
    for col in ["序號", "目標尺碼", "上偏差", "下偏差", "量測值"]:
        sub[col] = pd.to_numeric(sub[col], errors="coerce")
    sub = sub.dropna(subset=["序號", "目標尺碼"]).sort_values(["序號", "來源檔名"])

    # 三條線
    sub["上界線"] = sub["目標尺碼"] + sub["上偏差"].fillna(0)
    sub["名義線"] = sub["目標尺碼"]
    sub["下界線"] = sub["目標尺碼"] - sub["下偏差"].fillna(0)

    line_base = sub.groupby("序號", as_index=False).first()[["序號", "上界線", "名義線", "下界線"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(line_base["序號"], line_base["上界線"], label="Upper")
    ax.plot(line_base["序號"], line_base["名義線"], label="Dimension")
    ax.plot(line_base["序號"], line_base["下界線"], label="Lower")

    for src, g in sub.groupby("來源檔名"):
        g_valid = g.dropna(subset=["量測值", "序號"])
        if g_valid.empty:
            continue
        ax.scatter(g_valid["序號"], g_valid["量測值"], label=f"{src}")

    ax.set_title(f"No.{grp_num} ({grp_type}) SPC")
    ax.set_xlabel("序號（群組內順序）")
    ax.set_ylabel("尺寸")
    ax.xaxis.set_major_locator(MaxNLocator(integer=True))
    ax.grid(True)

    # 圖例移到圖外右側
    ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
    fig.tight_layout(rect=[0, 0, 0.8, 1])
    return fig

# -------------- Streamlit UI --------------
st.set_page_config(page_title="SPC Tool (Web)", layout="wide")
st.title("SPC Tool（Web 版）")

with st.sidebar:
    st.header("1) 上傳 CSV（可多個檔案）")
    files = st.file_uploader("選擇要處理的 CSV", type=["csv"], accept_multiple_files=True)
    run_btn = st.button("清理 + 分群 + 合併")

# state: 儲存合併結果，避免每次操作都重跑
if "combined_long" not in st.session_state:
    st.session_state["combined_long"] = None

if run_btn:
    if not files:
        st.warning("請先上傳至少一個 CSV 檔。")
    else:
        parts = []
        for f in files:
            try:
                df_clean = clean_one_csv(f, f.name)
                df_tagged = tag_groups_and_sequence(df_clean, f.name)
                parts.append(df_tagged)
            except Exception as e:
                st.error(f"無法處理 {f.name}: {e}")

        if parts:
            combined = pd.concat(parts, ignore_index=True)
            combined_long = interleave_and_dedup_headers(combined)
            st.session_state["combined_long"] = combined_long

            # 產生 xlsx 並提供下載
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
            st.success("✅ 合併完成！")
            st.download_button("下載 merged_groups_result.xlsx", data=out_buf,
                               file_name="merged_groups_result.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# 顯示資料表
combined_long = st.session_state.get("combined_long")
if combined_long is not None:
    st.header("2) 檢視合併後資料")
    st.dataframe(combined_long, use_container_width=True, height=300)

    st.header("3) 查詢與繪圖")
    cols = st.columns(3)
    grp_num = cols[0].text_input("圈碼（只需數字，例如 92）", "")
    grp_type = cols[1].selectbox("群組類型", ["Main", "Ref"], index=0)
    plot_btn = cols[2].button("繪製 SPC 圖")

    if plot_btn:
        if not grp_num.strip().isdigit():
            st.warning("請輸入純數字的圈碼（例如 92）。")
        else:
            sub = combined_long[
                (combined_long["群組數字"].astype(str) == grp_num.strip()) &
                (combined_long["群組類型"] == grp_type) &
                (~combined_long["是否群組標頭"])
            ].copy()

            if sub.empty:
                st.info(f"找不到群組 No.{grp_num}（{grp_type}）的資料。")
            else:
                fig = plot_group_matplotlib(sub, grp_num.strip(), grp_type)
                st.pyplot(fig, use_container_width=True)
else:
    st.info("請先於左側上傳 CSV 並按「清理 + 分群 + 合併」。")

