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

- `render.yaml` provisions a web service and a managed Postgres database.
- `Procfile` and `gunicorn app:app` provide a production start command.
- `DATABASE_URL` switches the app from local SQLite to hosted Postgres automatically.
- `SECRET_KEY` should be set in the host environment for production.

### Render

1. Push this project to a Git provider.
2. Create a new Render Blueprint or Web Service from the repo.
3. Keep the generated `SECRET_KEY`.
4. Let Render create the managed Postgres database declared in `render.yaml`.
5. Deploy.

If you deploy somewhere else, set these environment variables as needed:

- `PORT`
- `SECRET_KEY`
- `DATABASE_URL` for hosted Postgres, or `DATABASE_PATH` for local SQLite
- `DATA_FILE` only if you want to seed from or export around a workbook path

## Default Login

- Admin: `admin` / `admin123`
- Promoter: `promoter1` / `promoter123`

## Data

For local development, the app stores data in `sales_portal.db` and can seed from `daily_sales_portal.xlsx`.

For hosted deployment, set `DATABASE_URL` and the app will use Postgres instead of local files.

Sheets created automatically:

- `Users`
- `SKUs`
- `Targets`
- `Sales`
- `Audit Log`
