FROM python:3.12-slim
WORKDIR /kerhnel
COPY src/app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["uvicorn", "src.app.main:app", "--host", "0.0.0.0", "--port", "9000"]
EXPOSE 9000