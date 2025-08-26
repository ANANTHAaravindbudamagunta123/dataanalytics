from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import uvicorn
import os
import io
import json
import pandas as pd
from services.data_service import DataService  # make sure this file exists and is correct

# Directories
BASE_DIR = os.path.dirname(__file__)
UPLOAD_DIR = os.path.join(BASE_DIR, "uploads")
DASH_DIR = os.path.join(BASE_DIR, "dashboards")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(DASH_DIR, exist_ok=True)

# FastAPI app
app = FastAPI()
app.mount('/static', StaticFiles(directory=os.path.join(BASE_DIR, 'static')), name='static')
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, 'templates'))

# Data service
data_service = DataService()


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "message": None})


@app.post("/upload", response_class=HTMLResponse)
async def upload(request: Request, file: UploadFile = File(...)):
    try:
        # Read raw bytes
        contents = await file.read()
        buffer = io.BytesIO(contents)
        filename = file.filename

        # Parse CSV or Excel
        if filename.endswith(".csv"):
            df = pd.read_csv(buffer)
        elif filename.endswith((".xls", ".xlsx")):
            df = pd.read_excel(buffer)
        else:
            return templates.TemplateResponse(
                "index.html",
                {"request": request, "message": "Unsupported file format. Upload CSV or Excel."}
            )

        # Save uploaded file
        save_path = os.path.join(UPLOAD_DIR, filename)
        with open(save_path, "wb") as f:
            f.write(contents)

        # Get summary, columns, sample
        summary = data_service.get_summary(df)
        cols = data_service.get_columns(df)
        sample = df.head(100).to_dict(orient="records")

        # Store in JSON (for charts/dashboards)
        temp_store = os.path.join(UPLOAD_DIR, filename + ".json")
        df.head(1000).to_json(temp_store, orient="records", date_format="iso")

        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "filename": filename,
                "columns": cols,
                "summary": summary,
                "sample": sample,
            },
        )

    except Exception as e:
        return templates.TemplateResponse(
            "index.html",
            {"request": request, "message": f"Error reading file: {e}"}
        )


@app.post("/generate_chart")
async def generate_chart(request: Request):
    form = await request.form()
    filename = form.get("filename")
    chart_type = form.get("chart_type")
    x_col = form.get("x_col")
    y_col = form.get("y_col")
    agg = form.get("agg") or "sum"

    temp_store = os.path.join(UPLOAD_DIR, filename + ".json")
    if not os.path.exists(temp_store):
        return {"error": "Uploaded data expired. Please upload again."}

    with open(temp_store, "r") as f:
        records = json.load(f)

    df = data_service.from_records(records)
    chart_data = data_service.prepare_chart_data(df, x_col, y_col, chart_type, agg)
    return chart_data


@app.post("/save_dashboard")
async def save_dashboard(name: str = Form(...), config: str = Form(...)):
    path = os.path.join(DASH_DIR, f"{name}.json")
    with open(path, "w") as f:
        f.write(config)
    return {"status": "ok", "path": path}


@app.get("/list_dashboards")
async def list_dashboards():
    files = [f for f in os.listdir(DASH_DIR) if f.endswith(".json")]
    return {"dashboards": files}


if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
