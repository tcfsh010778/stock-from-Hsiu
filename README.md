# Stockfrom脩 量化選股站

每日自動生成靜態網站。

## 結構

```
選股網站/
├── generate_site.py          # 主程式：解析 MD -> 生成 HTML
├── refresh_prices.py         # 直接接 FinMind 更新股價快取
├── data/prices/              # FinMind 股價快取（日K/週K/月K來源）
├── docs/                     # GitHub Pages 根目錄
│   ├── index.html            # 首頁
│   ├── daily.html            # 今日精選
│   ├── baskets.html          # 雙籃儀表板（行進籃 / 盤整籃）
│   ├── signals.html          # 訊號追蹤與歷史摘要
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

`refresh_prices.py` 會從 v44 的 `.env` 或環境變數讀取 `FINMIND_TOKEN`，抓取目前報告出現過的股票近 12 個月股價，寫入 `data/prices/`。個股頁會用這份資料顯示 FinMind 最新收盤、日K、週K、月K。

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
- **盤整籃**：MABC + CaryBot 早買觀察，重點是量縮價穩、籌碼不離開、轉強型態。
- **過熱/風險**：不追高，等待回測 MA5/MA10/箱頂後再處理。

## 訊號追蹤

`docs/signals.html` 會把每日報告整理成歷史訊號台帳，包含個股首次入選、最近入選、入選次數、最新買點與出現日期。雙籃資訊卡也會顯示同一份歷史訊號摘要，方便確認哪些股票已經反覆進入觀察。

若 Telegram 發送程式有成功推播紀錄，可在 repo 根目錄放 `signal_push_log.csv`，欄位支援：

```csv
date,stock_id,status,sent_at,channel
2026-04-30,6213,sent,2026-04-30 17:35,telegram
```

網站會自動比對「入選訊號」與「推播紀錄」，用來檢查是否有買點漏推。
