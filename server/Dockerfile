# Datasette for the Cylc monitor: the upload endpoint and the dashboard (plugins/, datasette.yaml).
FROM python:3.13-slim
RUN pip install --no-cache-dir 'datasette==1.0a41' 'datasette-auth-passwords==1.1.1' 'datasette-dashboards==0.8.0'
WORKDIR /app
COPY datasette.yaml schema.sql make_token.py /app/
COPY plugins /app/plugins
