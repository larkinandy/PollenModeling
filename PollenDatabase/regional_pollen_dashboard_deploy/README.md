# Regional Pollen Dashboard Deploy

This folder is a self-contained Streamlit Community Cloud version of the regional pollen dashboard.

## Files

- `streamlit_app.py`: Streamlit app entry point.
- `data/sites.parquet`: site and current sensor-status snapshot.
- `data/pollen_hourly.parquet`: zero-filled hourly pollen and particulate records used by the dashboard.
- `requirements.txt`: Streamlit Cloud Python dependencies.
- `export_dashboard_data.py`: local refresh script for rebuilding the Parquet files from the local `pollen_dashboard` Postgres database.

## Optional Password

In Streamlit Community Cloud, open the app settings and add this under Secrets:

```toml
dashboard_password = "your-shared-password"
```

If `dashboard_password` is not set, the app runs without a password prompt.

## Refreshing Data Locally

From the repository root, run:

```powershell
C:\Users\larki\AppData\Local\Python\pythoncore-3.14-64\python.exe regional_pollen_dashboard_deploy\export_dashboard_data.py
```

Then redeploy or push the updated files in `regional_pollen_dashboard_deploy/data/`.

## Streamlit Cloud Setup

Use:

- Main file path: `regional_pollen_dashboard_deploy/streamlit_app.py`
- Python dependencies: `regional_pollen_dashboard_deploy/requirements.txt`
