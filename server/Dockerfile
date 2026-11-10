# Datasette for the Cylc monitor, with its workflow status page (plugins/, templates/).
FROM python:3.13-slim
RUN pip install --no-cache-dir 'datasette==1.0a41' 'datasette-auth-passwords==1.1.1'
WORKDIR /app
COPY datasette.yaml schema.sql make_token.py /app/
COPY plugins /app/plugins
COPY templates /app/templates
