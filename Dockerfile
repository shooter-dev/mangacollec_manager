FROM python:3.13

RUN apt-get update && apt-get install -y cron nano

# Définir le répertoire de travail
WORKDIR /app


COPY requirements.txt /app/requirements.txt
RUN pip install -r requirements.txt


COPY ./manager_app/manager.py /app/manager.py
COPY cronjob.txt /etc/cron.d/my-cron-job
COPY .env /app/.env

RUN chmod 0644 /etc/cron.d/my-cron-job && crontab /etc/cron.d/my-cron-job

RUN mkdir -p /var/log/cron

# Lancer l'application manager
# CMD ["python3", "manager.py"]
CMD ["cron", "-f"]