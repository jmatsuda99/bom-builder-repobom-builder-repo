# BOM Builder (Streamlit)

部品データベースからBOMを作成するシンプルなWebアプリ（Streamlit）。

## 主な機能
- SQLiteベースのDB管理（parts / boms / bom_items）
- CSV/Excel/テキスト取り込み（**候補→確認→確定**の2段階）
- カテゴリ1/2の**マルチセレクト**でフィルタ
- 行ごとの**チェックボックス**選択でBOMに一括追加
- 価格モデル：`fixed` / `per_kwh` / `per_year`
  - BOM画面で容量(kWh)、年数(years)を入力 → 金額に反映
- 自動バックアップ（`_db_backups/`）

## セットアップ
```bash
pip install -r requirements.txt
streamlit run parts_bom_streamlit_app.py
```

## 運用メモ
- 起動時、`parts_master.xlsx` / `parts_master.csv` が同一フォルダにあれば提示されます。
- 取り込みは即DBに書かれず、「取り込み候補」を作って確認後に確定保存します。
- DBファイルは `parts_bom.db`（リポジトリ直下）。バックアップは `_db_backups/`。

## データ項目（parts）
- partNo, description, manufacturer, category, unit, unitPrice, notes
- category1, category2, pricingModel, unitPricePerKWh, unitPricePerYear, refCapacityKWh

## ライセンス
MIT
