# ⚖️ Legal Scout — Lawyer Enrichment System

מערכת אוטומטית לסיווג עורכי דין לפי תחומי עיסוק מתוך תוכן האתר שלהם.

---

## 🚀 פריסה ב-Railway (מומלץ)

### שלב 1 — העלה ל-GitHub

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/YOUR_USERNAME/lawyer-enricher.git
git push -u origin main
```

### שלב 2 — צור פרויקט ב-Railway

1. היכנס ל-[railway.app](https://railway.app) והתחבר עם GitHub
2. לחץ **"New Project"** → **"Deploy from GitHub repo"**
3. בחר את ה-repo שיצרת

### שלב 3 — הגדר משתני סביבה

בפרויקט ב-Railway: לחץ על ה-Service → **Variables** → הוסף:

```
ANTHROPIC_API_KEY = sk-ant-...
```

### שלב 4 — הוסף Volume לשמירת נתונים

ב-Railway: לחץ על ה-Service → **Volumes** → **Add Volume**
- Mount Path: `/data`

זה מבטיח שה-DB וקבצי ה-Excel לא יימחקו בין פריסות.

### שלב 5 — פרוס!

Railway יפרוס אוטומטית. אחרי כמה שניות תקבל URL כמו:
`https://lawyer-enricher-production.up.railway.app`

---

## 💻 הרצה מקומית

```bash
export ANTHROPIC_API_KEY=sk-ant-...
chmod +x start.sh
./start.sh
```

פתח: **http://localhost:5000**

---

## מבנה הקובץ

```
lawyer-enricher/
├── backend/
│   └── app.py          # Flask + כל לוגיקת העיבוד
├── frontend/
│   └── index.html      # ממשק משתמש (עברית / RTL)
├── Procfile            # הוראות הפעלה ל-Railway
├── runtime.txt         # גרסת Python
├── requirements.txt    # חבילות Python
├── start.sh            # סקריפט הפעלה מקומי
└── .gitignore
```

---

## מבנה קובץ ה-Excel הנדרש

עמודות חובה (בדיוק כך):

| עמודה | תיאור |
|-------|-------|
| שם בית העסק | שם עו"ד / משרד |
| קטגוריה עסקית | **מוזרם לפלט בלבד — מוזנח לחלוטין בסיווג** |
| אתר בית | כתובת אתר |
| עמוד פייסבוק | עמוד פייסבוק (אופציונלי) |
| ישוב | עיר |
| + שאר העמודות... | |

---

## עמודות פלט (מוסיפות לקובץ המקורי)

| עמודה | תיאור |
|-------|-------|
| site_final | URL סופי אחרי redirect |
| site_status | CRAWLED / NO_SITE / CACHED / FETCH_FAILED |
| primary_area_1/2/3 | עד 3 תחומים עיקריים |
| secondary_areas | תחומים משניים (מופרדים בפסיקים) |
| confidence | ביטחון בסיווג (0-100) |
| recommendation | YES / NO / MAYBE |
| recommendation_reason | הסבר קצר |
| evidence_1/2 | ציטוטים מהאתר שתמכו בסיווג |
| facebook_found | URL פייסבוק (שנמצא או קיים) |
| checked_at | חותמת זמן |
