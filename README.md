# Stockfrom脩 量化選股站

每日自動生成靜態網站。

## 結構

```
選股網站/
├── generate_site.py          # 主程式：解析 MD -> 生成 HTML
├── refresh_prices.py         # 直接接 TWSE/TPEx 官方介面更新股價快取
├── data/prices/              # 官方 OHLCV 股價快取（日K/週K/月K來源）
├── docs/                     # GitHub Pages 根目錄
│   ├── index.html            # 首頁
│   ├── daily.html            # 今日精選
│   ├── baskets.html          # 雙籃儀表板（行進籃 / 盤整籃）
│   ├── signals.html          # 訊號追蹤與歷史摘要
│   ├── stocks.html           # 個股總覽與搜尋
│   ├── radar.html            # 買點雷達：收盤價 vs 建議買點
│   ├── stocks/股票代號.html  # 個股資訊卡、買點、日K/週K/月K
│   ├── history.html          # 歷史報告
│   └── daily/YYYY-MM-DD.html # 個別日期完整報告
└── .github/workflows/
    └── daily_update.yml      # 每日自動執行
```

## 本地執行

```bash
python refresh_prices.py
python generate_site.py
```

`refresh_prices.py` 會直接從 TWSE 與 TPEx 官方介面抓取上市、上櫃股票的 OHLCV，依交易日合併並寫入 `data/prices/`，同時產生 `data/price_refresh_summary.json`。它不需要 FinMind 訂閱；舊 FinMind 籌碼／股權輔助更新預設停用，只能用 `ENABLE_FINMIND_AUX=1` 明確開啟。個股頁會用官方快取顯示最新收盤、日K/週K/月K、MA120/MA240、成交量與大量K。

`generate_site.py` 會同步寫入 `data/site_reports.json`。這是 GitHub Actions 的備援資料源，避免雲端 runner 讀不到本機 OneDrive 報告時無法重建網站。

## 資料合約與 freshness

官方免費資料的來源路由、欄位 schema、涵蓋市場、更新頻率與 freshness SLA
定義在 `contracts/taiwan_stock_data_contracts.json`；人工可讀矩陣在
`contracts/freshness_matrix.md`。目前官方 primary 只使用 TWSE、TPEx、TDCC
與 MOPS 的 owner-operated 介面，既有 FinMind 路徑只能作為 manifest 中明確
揭露的 fallback。

```bash
python data_contract.py validate-registry
python data_contract.py validate-manifest data/freshness_manifest.json
```

交易日型資料的 manifest 必須提供官方交易日清單與來源 ID，並分開記錄
`data_date`、`trading_date`、`expected_data_date`、`fetched_at`、`row_count`、
schema version、SHA-256、fallback 與缺漏狀態。

## 每日決策合約

`daily_decisions.py` 會把既有 MDA candidate pool、CaryBot B1/B2 timing、
shared traffic light、官方注意／處置風險與 freshness 狀態整理成
`data/daily_decisions.json`。
這是每日操作建議的結構化證據層，不會改變選股門檻、訊號規則、出場規則，
也不會自動下單。

`attention_disposition.py` 只讀 TWSE／TPEx 官方公開端點，輸出
`data/attention_disposition.json`。2026-08-10 新制以版本化規則處理：一般
處置 5 個營業日、涉及當沖比注意條件者 7 日、一般處置約每 2 分鐘撮合；
跨生效日案件採官方公告修正後的迄日。來源缺漏時不會把個股標成無風險。

```bash
python attention_disposition.py
python daily_decisions.py
```

## GitHub Pages 部署步驟

1. 建立 GitHub repo（可設為 Private）
2. `git init && git add . && git commit -m "init"`
3. `git remote add origin https://github.com/你的帳號/stock-site.git`
4. `git push -u origin main`
5. GitHub repo -> Settings -> Pages -> Source 選 `main` branch, `/docs` folder
6. 網站網址：`https://你的帳號.github.io/stock-site/`

## 資料來源

每日選股報告 MD 來自 `台灣交易機器人 v44` 的排程輸出，
路徑設定在 `generate_site.py` 的 `REPORTS_DIR`。

可用環境變數覆蓋：

```bash
REPORTS_DIR=/path/to/reports python generate_site.py
```

## 策略頁

`docs/baskets.html` 會把每日報告先分成：

- **行進籃**：SFZ 波段候選，原訊號可試單，TA3 作加碼/確認，MA20 管理。
- **盤整籃**：MABC + 量價早買觀察，重點是量縮價穩、籌碼不離開、轉強型態。
- **過熱/風險**：不追高，等待回測 MA5/MA10/箱頂後再處理。

## 訊號追蹤

`docs/signals.html` 會把每日報告整理成歷史訊號台帳，包含個股首次入選、最近入選、入選次數、最新買點與出現日期。雙籃資訊卡也會顯示同一份歷史訊號摘要，方便確認哪些股票已經反覆進入觀察。

若 Telegram 發送程式有成功推播紀錄，可在 repo 根目錄放 `signal_push_log.csv`，欄位支援：

```csv
date,stock_id,status,sent_at,channel
2026-04-30,6213,sent,2026-04-30 17:35,telegram
```

網站會自動比對「入選訊號」與「推播紀錄」，用來檢查是否有買點漏推。
