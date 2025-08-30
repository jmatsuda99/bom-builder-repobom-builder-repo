# parts_bom_streamlit_app.py
# --- BOM Builder 完全版 (Python + Streamlit) ---
# 要件まとめ（2025-08-30）
# 1) DB(部品カタログ)の作成/更新：SQLite
# 2) 多様な資料から取り込み：CSV/Excel/テキスト（PDFは任意）
# 3) DBを参照してBOM作成/出力
# 4) 運用:
#    - 起動時に既存DB/マスターファイル検出（parts_master.xlsx/csv）
#    - 取り込みは「候補→確認→確定」2段階
#    - DB更新やBOM操作の前に自動バックアップ
# 5) UI:
#    - カテゴリ1/2マルチセレクトで絞り込み
#    - 行チェックボックスで複数一括選択→BOMへ追加
#    - 価格モデル: fixed / per_kwh / per_year
#      → BOM画面で容量(kWh)・年数(years)を入力して金額自動計算
# 6) EMS:
#    - Software license の価格のみ採用（per_kwhに落とし込む想定）
#    - notes 自動補助（EMS関連の文を要約）※CSV/Excelではそのまま

import io
import os
import re
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

DB_PATH = "parts_bom.db"
MASTER_CANDIDATES = ["parts_master.xlsx", "parts_master.csv"]
BACKUP_DIR = Path("_db_backups")

PART_COLS = [
    "partNo","description","manufacturer","category",
    "unit","unitPrice","notes",
    "category1","category2","pricingModel",
    "unitPricePerKWh","unitPricePerYear","refCapacityKWh",
]

CREATE_SQL = {
    "parts": """
        CREATE TABLE IF NOT EXISTS parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            partNo TEXT NOT NULL,
            description TEXT,
            manufacturer TEXT,
            category TEXT,
            unit TEXT,
            unitPrice REAL,
            notes TEXT,
            category1 TEXT,
            category2 TEXT,
            pricingModel TEXT DEFAULT 'fixed',
            unitPricePerKWh REAL,
            unitPricePerYear REAL,
            refCapacityKWh REAL
        );
    """,
    "boms": """
        CREATE TABLE IF NOT EXISTS boms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            createdAt INTEGER NOT NULL
        );
    """,
    "bom_items": """
        CREATE TABLE IF NOT EXISTS bom_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bomId INTEGER NOT NULL,
            partId INTEGER NOT NULL,
            qty REAL NOT NULL DEFAULT 1,
            altText TEXT,
            FOREIGN KEY (bomId) REFERENCES boms(id) ON DELETE CASCADE,
            FOREIGN KEY (partId) REFERENCES parts(id) ON DELETE CASCADE
        );
    """,
}

def get_conn():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def init_db():
    with get_conn() as con:
        cur = con.cursor()
        for sql in CREATE_SQL.values():
            cur.execute(sql)
        con.commit()

def backup_db_file() -> Optional[Path]:
    dbp = Path(DB_PATH)
    if not dbp.exists():
        return None
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    dst = BACKUP_DIR / f"{dbp.stem}.backup-{ts}{dbp.suffix}"
    shutil.copy2(dbp, dst)
    return dst

def count_parts() -> int:
    with get_conn() as con:
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM parts")
        (n,) = cur.fetchone()
        return int(n or 0)

def find_master_file() -> Optional[Path]:
    for name in MASTER_CANDIDATES:
        p = Path(name)
        if p.exists():
            return p
    return None

def load_master_to_df(p: Path) -> pd.DataFrame:
    if p.suffix.lower() == ".csv":
        return pd.read_csv(p)
    return pd.read_excel(p)

HEADER_GUESS_PATTERNS: List[Tuple[str, str]] = [
    (r"^part\s*no|^pn$|^品番|^部品番号", "partNo"),
    (r"^desc|^description|^仕様|^名称|^品名|^内容", "description"),
    (r"^maker|^manufacturer|^製造|^メーカー", "manufacturer"),
    (r"^cat$|^category|^分類|^カテゴリ$", "category"),
    (r"^category1|^カテゴリ1", "category1"),
    (r"^category2|^カテゴリ2", "category2"),
    (r"^unit|^単位", "unit"),
    (r"^price|^単価|^unit\s*price|^cost", "unitPrice"),
    (r"^note|^備考|^comment", "notes"),
    (r"pricing|price\s*model|価格.*モデル", "pricingModel"),
    (r"per\s*kwh|unitPricePerKWh", "unitPricePerKWh"),
    (r"per\s*year|unitPricePerYear", "unitPricePerYear"),
]

def guess_field(header: str) -> Optional[str]:
    h = str(header).strip().lower()
    for pat, key in HEADER_GUESS_PATTERNS:
        if re.search(pat, h):
            return key
    return None

def parse_free_text(text: str) -> pd.DataFrame:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    rows = []
    for line in lines:
        cols = re.split(r"\t+|\s{2,}", line)
        if len(cols) >= 2:
            part_no = cols[0]
            desc = cols[1]
            rows.append({
                "partNo": part_no,
                "description": desc,
                "manufacturer": "",
                "category": "",
                "unit": "set",
                "unitPrice": None,
                "notes": "",
                "category1": "",
                "category2": "",
                "pricingModel": "fixed",
                "unitPricePerKWh": None,
                "unitPricePerYear": None,
                "refCapacityKWh": None,
            })
    return pd.DataFrame(rows, columns=PART_COLS)

def insert_parts(df: pd.DataFrame) -> int:
    if not set(PART_COLS).issubset(df.columns):
        for c in PART_COLS:
            if c not in df.columns:
                df[c] = None if c not in ["manufacturer","category","unit","notes","category1","category2","pricingModel"] else ""
    if not st.session_state.get("allow_update", False):
        if not st.checkbox("このセッションでDB更新を許可する（確認用）"):
            st.warning("チェックを入れるとDB更新が有効になります。")
            return 0
        st.session_state["allow_update"] = True

    with get_conn() as con:
        df = df.copy()
        df["unitPrice"] = pd.to_numeric(df["unitPrice"], errors="coerce")
        df["unitPricePerKWh"] = pd.to_numeric(df["unitPricePerKWh"], errors="coerce")
        df["unitPricePerYear"] = pd.to_numeric(df["unitPricePerYear"], errors="coerce")
        exist = pd.read_sql_query("SELECT partNo, description FROM parts", con)
        if not exist.empty:
            key_new = (df["partNo"].str.lower()+"|"+df["description"].str.lower())
            key_exist = (exist["partNo"].str.lower()+"|"+exist["description"].str.lower()).tolist()
            df = df[~key_new.isin(key_exist)]
        if df.empty:
            return 0
        df[PART_COLS].to_sql("parts", con, if_exists="append", index=False)
        return len(df)

def read_parts(q: str = "") -> pd.DataFrame:
    with get_conn() as con:
        base = """
        SELECT id, partNo, description, manufacturer, category, category1, category2,
               unit, unitPrice, pricingModel, unitPricePerKWh, unitPricePerYear, refCapacityKWh, notes
        FROM parts
        """
        if q:
            like = f"%{q}%"
            return pd.read_sql_query(
                base + """ WHERE partNo LIKE ? OR description LIKE ? OR manufacturer LIKE ?
                           OR category LIKE ? OR category1 LIKE ? OR category2 LIKE ?
                           OR unit LIKE ? OR notes LIKE ?""",
                con, params=[like, like, like, like, like, like, like, like],
            )
        return pd.read_sql_query(base, con)

def delete_part(part_id: int):
    with get_conn() as con:
        con.execute("DELETE FROM parts WHERE id = ?", (part_id,))
        con.commit()

def create_bom(name: str) -> int:
    with get_conn() as con:
        cur = con.cursor()
        cur.execute("INSERT INTO boms(name, createdAt) VALUES(?, ?)", (name, int(time.time()*1000)))
        con.commit()
        return cur.lastrowid

def list_boms() -> pd.DataFrame:
    with get_conn() as con:
        return pd.read_sql_query("SELECT * FROM boms ORDER BY createdAt DESC", con)

def list_bom_items(bom_id: int) -> pd.DataFrame:
    with get_conn() as con:
        sql = """
        SELECT bi.id as itemId, bi.bomId, bi.partId, bi.qty, bi.altText,
               p.partNo, p.description, p.manufacturer,
               p.category, p.category1, p.category2,
               p.unit, p.unitPrice,
               p.pricingModel, p.unitPricePerKWh, p.unitPricePerYear, p.refCapacityKWh, p.notes
        FROM bom_items bi
        JOIN parts p ON p.id = bi.partId
        WHERE bi.bomId = ?
        ORDER BY bi.id ASC
        """
        return pd.read_sql_query(sql, con, params=[bom_id])

def add_item(bom_id: int, part_id: int, qty: float = 1.0):
    with get_conn() as con:
        con.execute("INSERT INTO bom_items(bomId, partId, qty) VALUES(?, ?, ?)", (bom_id, part_id, qty))
        con.commit()

def remove_item(item_id: int):
    with get_conn() as con:
        con.execute("DELETE FROM bom_items WHERE id = ?", (item_id,))
        con.commit()

def update_qty(item_id: int, qty: float):
    with get_conn() as con:
        con.execute("UPDATE bom_items SET qty = ? WHERE id = ?", (qty, item_id))
        con.commit()

# -------- Streamlit App --------
st.set_page_config(page_title="BOM Builder", page_icon="🧩", layout="wide")
init_db()

st.sidebar.header("起動時の読み込み")
parts_count = count_parts()
master = find_master_file()
if master is not None:
    st.sidebar.success(f"マスターファイル検出: {master.name}")
    st.sidebar.caption(f"最終更新: {datetime.fromtimestamp(Path(master).stat().st_mtime):%Y-%m-%d %H:%M:%S}")
    if parts_count == 0:
        if st.sidebar.button("このマスターファイルを読み込む"):
            try:
                dfm = load_master_to_df(master)
                all_cols = list(dfm.columns)
                mapping = {}
                for key in PART_COLS:
                    if key in dfm.columns:
                        mapping[key] = key
                    else:
                        guessed = next((c for c in all_cols if guess_field(str(c)) == key), None)
                        mapping[key] = guessed
                df_norm = pd.DataFrame({k: (dfm[mapping[k]] if mapping[k] else "") for k in PART_COLS})
                df_norm["unitPrice"] = pd.to_numeric(df_norm["unitPrice"], errors="coerce")
                n = insert_parts(df_norm)
                st.sidebar.success(f"{n} 件を読み込みました")
            except Exception as e:
                st.sidebar.error(f"読み込み失敗: {e}")
else:
    st.sidebar.info("parts_master.xlsx / parts_master.csv を同じフォルダに置くと自動検出します。")

st.sidebar.header("バックアップ")
if Path(DB_PATH).exists():
    if st.sidebar.button("DBを手動バックアップ"):
        dst = backup_db_file()
        if dst:
            st.sidebar.success(f"バックアップ作成: {dst}")
else:
    st.sidebar.caption("DBファイル未作成（まだデータ未保存）")

st.title("部品表ブラウザアプリ（完全版）")
TAB_DB, TAB_INGEST, TAB_BOM = st.tabs(["データベース", "資料取り込み", "BOM 作成"])

with TAB_DB:
    st.subheader("検索・一覧")
    q = st.text_input("フリーワード（品番・品名・カテゴリ1/2・備考…）", value="")
    df_all = read_parts(q)

    for c in ["category1","category2","pricingModel","unit"]:
        if c in df_all.columns:
            df_all[c] = df_all[c].fillna("")

    cat1_options = sorted([c for c in df_all["category1"].unique().tolist() if c])
    cat2_options = sorted([c for c in df_all["category2"].unique().tolist() if c])

    c1, c2 = st.columns(2)
    with c1:
        sel_c1 = st.multiselect("カテゴリ1で絞り込み（複数可）", options=cat1_options, default=[])
    with c2:
        sel_c2 = st.multiselect("カテゴリ2で絞り込み（複数可）", options=cat2_options, default=[])

    df_view = df_all.copy()
    if sel_c1:
        df_view = df_view[df_view["category1"].isin(sel_c1)]
    if sel_c2:
        df_view = df_view[df_view["category2"].isin(sel_c2)]

    if "_select" not in df_view.columns:
        df_view.insert(0, "_select", False)

    st.caption("BOMに追加したい行にチェックを入れてください。")
    edited = st.data_editor(
        df_view[["_select","id","partNo","description","category1","category2",
                 "pricingModel","unit","unitPrice","unitPricePerKWh","unitPricePerYear","notes"]],
        use_container_width=True, height=450, hide_index=True
    )
    selected_ids = edited.loc[edited["_select"] == True, "id"].astype(int).tolist()
    st.session_state["selected_part_ids"] = selected_ids

    st.markdown("---")
    st.subheader("新規パーツ追加（任意）")
    with st.form("new_part_form", clear_on_submit=True):
        cA, cB, cC = st.columns([2,2,1])
        with cA:
            partNo = st.text_input("品番 *")
            description = st.text_input("品名/仕様")
            category1 = st.text_input("カテゴリ1")
            category2 = st.text_input("カテゴリ2")
        with cB:
            unit = st.text_input("単位", value="set")
            unitPrice = st.number_input("固定単価 (通貨任意)", min_value=0.0, step=100.0, value=0.0, format="%.2f")
            pricingModel = st.selectbox("価格モデル", options=["fixed","per_kwh","per_year"], index=0)
        with cC:
            unitPricePerKWh = st.number_input("単価 (/kWh)", min_value=0.0, step=0.01, value=0.0, format="%.4f")
            unitPricePerYear = st.number_input("単価 (/year)", min_value=0.0, step=100.0, value=0.0, format="%.2f")
            notes = st.text_area("備考", height=80)
        if st.form_submit_button("保存"):
            if not partNo.strip():
                st.warning("品番は必須です")
            else:
                backup_db_file()
                row = pd.DataFrame([{
                    "partNo": partNo.strip(),
                    "description": description.strip(),
                    "manufacturer": "",
                    "category": "",
                    "unit": unit.strip() or "set",
                    "unitPrice": float(unitPrice) if unitPrice else None,
                    "notes": notes.strip(),
                    "category1": category1.strip(),
                    "category2": category2.strip(),
                    "pricingModel": pricingModel,
                    "unitPricePerKWh": float(unitPricePerKWh) if unitPricePerKWh else None,
                    "unitPricePerYear": float(unitPricePerYear) if unitPricePerYear else None,
                    "refCapacityKWh": None,
                }])[PART_COLS]
                n = insert_parts(row)
                st.success(f"{n} 件を追加しました")
                st.experimental_rerun()

    st.markdown("---")
    st.subheader("選択削除（ID指定）")
    part_id_to_delete = st.number_input("削除する部品ID", min_value=0, step=1)
    if st.button("削除実行"):
        try:
            backup_db_file()
            delete_part(int(part_id_to_delete))
            st.success("削除しました")
            st.experimental_rerun()
        except Exception as e:
            st.error(str(e))

with TAB_INGEST:
    st.subheader("一括取り込み（CSV/Excel/テキスト）")
    if "pending_import" not in st.session_state:
        st.session_state["pending_import"] = None

    up = st.file_uploader("CSV または Excel(xlsx) を選択", type=["csv","xlsx"])
    if up is not None:
        if up.type == "text/csv" or up.name.lower().endswith(".csv"):
            df_raw = pd.read_csv(up)
        else:
            df_raw = pd.read_excel(up)
        st.write("プレビュー（先頭20行）")
        st.dataframe(df_raw.head(20), use_container_width=True, height=300)

        st.markdown("### 列マッピング（未指定は空扱い）")
        all_cols = list(df_raw.columns)
        map_cols: Dict[str, str] = {}
        for key in PART_COLS:
            guessed = next((c for c in all_cols if guess_field(str(c)) == key), "")
            map_cols[key] = st.selectbox(f"{key}", options=[""]+all_cols, index=(all_cols.index(guessed)+1) if guessed in all_cols else 0, key=f"map_{key}")

        if st.button("この設定で取り込み候補を作成"):
            df_norm = pd.DataFrame({k: (df_raw[map_cols[k]] if map_cols[k] else "") for k in PART_COLS})
            df_norm["partNo"] = df_norm["partNo"].astype(str).str.strip()
            df_norm = df_norm[df_norm["partNo"] != ""]
            for c in ["unitPrice","unitPricePerKWh","unitPricePerYear","refCapacityKWh"]:
                df_norm[c] = pd.to_numeric(df_norm[c], errors="coerce")
            df_norm["unit"] = df_norm["unit"].replace("", "set")
            df_norm["pricingModel"] = df_norm["pricingModel"].replace("", "fixed")
            st.session_state["pending_import"] = df_norm
            st.success("取り込み候補を作成しました。下で確認してください。")

    st.markdown("---")
    st.markdown("### テキスト貼り付け解析（PDF/メールからコピペ可）")
    txt = st.text_area("タブ or 2スペース以上区切り", height=160, placeholder="例)\nSGCS-E30    EMS element – Basic Functions ...")
    if st.button("解析して取り込み候補を作成"):
        df = parse_free_text(txt)
        if df.empty:
            st.warning("解析できませんでした。列区切り（タブ/2スペース以上）で貼り付けてください。")
        else:
            st.session_state["pending_import"] = df
            st.success("取り込み候補を作成しました。下で確認してください。")

    if st.session_state["pending_import"] is not None:
        st.markdown("---")
        st.subheader("取り込み候補の確認")
        dfp: pd.DataFrame = st.session_state["pending_import"]
        st.dataframe(dfp.head(100), use_container_width=True, height=360)
        confirm = st.checkbox("この内容でデータベース（SQLite: parts_bom.db）を更新してよい")
        col_c1, col_c2 = st.columns(2)
        with col_c1:
            if st.button("⏪ 取り込み候補を破棄"):
                st.session_state["pending_import"] = None
                st.info("取り込み候補を破棄しました。")
        with col_c2:
            if st.button("✅ 確定してDB更新", disabled=not confirm):
                try:
                    backup_db_file()
                    n = insert_parts(dfp)
                    st.success(f"{n} 件をDBに反映しました")
                    st.session_state["pending_import"] = None
                except Exception as e:
                    st.error(str(e))

with TAB_BOM:
    st.subheader("BOM 選択/作成")

    cB1, cB2 = st.columns([2,1])
    with cB1:
        bom_name = st.text_input("新しいBOM名")
    with cB2:
        if st.button("作成"):
            if not bom_name.strip():
                st.warning("BOM名を入力してください")
            else:
                backup_db_file()
                new_id = create_bom(bom_name.strip())
                st.success(f"BOMを作成しました (id={new_id})")
                st.experimental_rerun()

    df_boms = list_boms()
    st.dataframe(df_boms, use_container_width=True, height=180)

    target_bom_id = st.number_input("操作対象のBOM ID", min_value=0, step=1, value=int(df_boms.iloc[0]["id"]) if not df_boms.empty else 0)

    st.markdown("---")
    st.markdown("### 選択済み部品をBOMへ追加")
    sel_ids = st.session_state.get("selected_part_ids", [])
    st.write(f"選択中の部品ID: {sel_ids}")
    if st.button("選択行をBOMに追加"):
        if not sel_ids:
            st.warning("DBタブで部品をチェックしてください。")
        elif target_bom_id <= 0:
            st.warning("BOM ID を入力してください。")
        else:
            for pid in sel_ids:
                add_item(int(target_bom_id), int(pid), qty=1.0)
            st.success(f"{len(sel_ids)} 件をBOM({target_bom_id})へ追加しました。")

    st.markdown("---")
    st.subheader("BOM 明細（容量・年数を反映）")

    st.markdown("#### 見積パラメータ")
    colp1, colp2, colp3 = st.columns(3)
    with colp1:
        proj_kwh = st.number_input("対象容量 (kWh)", min_value=0.0, step=100.0, value=0.0)
    with colp2:
        proj_years = st.number_input("契約年数 (years)", min_value=0.0, step=1.0, value=0.0)
    with colp3:
        override_rate = st.number_input("EMS等のkWh単価を一時上書き（任意）", min_value=0.0, step=0.01, value=0.0, help="0 のままならDB値を使用")

    if target_bom_id > 0:
        df_items = list_bom_items(int(target_bom_id))
        if not df_items.empty:
            df = df_items.copy()
            df["displayUnitPrice"] = df["unitPrice"]
            if "unitPricePerKWh" in df.columns:
                per_mask = df["pricingModel"].astype(str).str.lower().eq("per_kwh") & (proj_kwh > 0)
                base_rate = df["unitPricePerKWh"]
                if override_rate > 0:
                    base_rate = override_rate
                df.loc[per_mask, "displayUnitPrice"] = (base_rate * proj_kwh).round(2)
            if "unitPricePerYear" in df.columns:
                py_mask = df["pricingModel"].astype(str).str.lower().eq("per_year") & (proj_years > 0)
                df.loc[py_mask, "displayUnitPrice"] = (df.loc[py_mask, "unitPricePerYear"] * proj_years).round(2)

            df["amount"] = (df["displayUnitPrice"].fillna(0) * df["qty"].fillna(0)).round(0)
            total = int(df["amount"].sum())

            st.dataframe(df, use_container_width=True, height=380)
            st.write(f"**合計**: {total:,}")

            st.markdown("#### 明細編集")
            item_id = st.number_input("更新対象 itemId", min_value=0, step=1)
            new_qty = st.number_input("新しい数量", min_value=0.0, step=1.0, value=1.0)
            cc1, cc2 = st.columns(2)
            with cc1:
                if st.button("数量更新"):
                    try:
                        backup_db_file()
                        update_qty(int(item_id), float(new_qty))
                        st.success("更新しました")
                    except Exception as e:
                        st.error(str(e))
            with cc2:
                if st.button("明細削除"):
                    try:
                        backup_db_file()
                        remove_item(int(item_id))
                        st.success("削除しました")
                    except Exception as e:
                        st.error(str(e))

            out_cols = ["partNo","description","category1","category2","unit","displayUnitPrice","qty","amount","notes"]
            export_df = df[out_cols].rename(columns={"displayUnitPrice":"unitPrice"})
            st.download_button(
                label="BOMをCSVでダウンロード",
                data=export_df.to_csv(index=False).encode("utf-8-sig"),
                file_name=f"bom_{int(target_bom_id)}.csv",
                mime="text/csv",
            )
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                export_df.to_excel(writer, index=False, sheet_name="BOM")
                pd.DataFrame([["","","","","","合計", "", total, ""]],
                             columns=["partNo","description","category1","category2","unit","unitPrice","qty","amount","notes"]
                            ).to_excel(writer, index=False, startrow=len(export_df)+2, sheet_name="BOM", header=False)
            st.download_button(
                label="BOMをExcelでダウンロード",
                data=bio.getvalue(),
                file_name=f"bom_{int(target_bom_id)}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        else:
            st.info("明細がありません。DBタブで部品を選び、上のボタンでBOMへ追加してください。")
    else:
        st.info("BOMを選択/作成してください。")

st.caption("SQLite/CSV/Excel対応・確認付き取り込み・カテゴリ1/2マルチフィルタ・チェック選択・per_kWh/per_year計算・バックアップ搭載。")
