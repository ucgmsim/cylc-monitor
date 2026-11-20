# Datasette for the Cylc monitor: the upload endpoints and the dashboards (plugins/, datasette.yaml),
# auto_scan's included (plugins/scan_upload.py, scan_views.sql, cmt_views.sql).
FROM python:3.13-slim
RUN pip install --no-cache-dir 'datasette==1.0a41' 'datasette-auth-passwords==1.1.1' 'datasette-dashboards==0.8.0'
WORKDIR /app
COPY datasette.yaml schema.sql scan_views.sql cmt_views.sql make_token.py /app/
COPY plugins /app/plugins
COPY static /app/static
