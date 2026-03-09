FROM python.3.13.0

ENV MODE = PRODUCTION

WORKDIR /app

RUN apt-get -y update && apt-get -y install git

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/

EXPOSE 8000

CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]

