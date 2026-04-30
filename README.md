# Stockfrom脩 量化選股站

仿 chengwaye.com 深色主題，每日自動生成靜態網站。

## 結構

```
選股網站/
├── generate_site.py          # 主程式：解析 MD -> 生成 HTML
├── docs/                     # GitHub Pages 根目錄
│   ├── index.html            # 首頁
│   ├── daily.html            # 今日精選
│   ├── history.html          # 歷史報告
│   └── daily/YYYY-MM-DD.html # 個別日期完整報告
└── .github/workflows/
    └── daily_update.yml      # 每日自動執行
```

## 本地執行

```bash
python generate_site.py
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
