# Daily Sales Portal

Excel-based Flask portal for promoters to submit daily sales and for admins to review category performance, targets, and achievement.

## Features

- Secure login with separate admin and promoter roles
- Promoter sales update form with SKU dropdown, quantity, and selling price
- Automatic sale amount calculation from quantity x selling price
- Promoter date-filtered sales download to CSV
- Promoter correction limited to same-day entries
- Admin dashboard with category-wise sales, targets, achievement percentage, and promoter performance
- Admin target management by date and category
- Admin user management, SKU master management, and audit log review
- Local Excel workbook storage with separate sheets for users, SKUs, targets, sales, and audit log

## Run locally

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python app.py
```

Then open [http://127.0.0.1:5000](http://127.0.0.1:5000).

## Deploy

This app is now set up for Python hosts such as Render.

- `render.yaml` provisions a web service and a persistent disk.
- `Procfile` and `gunicorn app:app` provide a production start command.
- `APP_DATA_DIR` controls where the SQLite database, workbook, and import previews are stored.
- `SECRET_KEY` should be set in the host environment for production.

### Render

1. Push this project to a Git provider.
2. Create a new Render Blueprint or Web Service from the repo.
3. Keep the generated `SECRET_KEY`.
4. Make sure the persistent disk is mounted at `/var/data`.
5. Deploy.

The service will store its writable files here:

- `/var/data/sales_portal.db`
- `/var/data/daily_sales_portal.xlsx`
- `/var/data/_import_previews/`

If you deploy somewhere else, set these environment variables as needed:

- `PORT`
- `SECRET_KEY`
- `APP_DATA_DIR` or `DATABASE_PATH` / `DATA_FILE` / `IMPORT_PREVIEW_DIR`

## Default Login

- Admin: `admin` / `admin123`
- Promoter: `promoter1` / `promoter123`

## Data

The app stores data in a local Excel workbook named `daily_sales_portal.xlsx` in the project folder, so it does not require SQL or a database server.

Sheets created automatically:

- `Users`
- `SKUs`
- `Targets`
- `Sales`
- `Audit Log`
