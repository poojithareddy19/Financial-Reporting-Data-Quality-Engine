# Lambda container image: one image, six handlers (CMD is set per function in the CDK stack).
FROM public.ecr.aws/lambda/python:3.11

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY sql ./sql
COPY config ./config

RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir . \
 && python -c "import fin_dq_engine, sklearn, matplotlib"

ENV FIN_DQ_CONFIG=/app/config/settings.yaml \
    FIN_DQ__PATHS__SQL_DIR=/app/sql \
    FIN_DQ__PATHS__CONFIG_DIR=/app/config \
    FIN_DQ__PATHS__OUT_DIR=/tmp/out \
    FIN_DQ__PATHS__DATA_DIR=/tmp/data \
    MPLCONFIGDIR=/tmp/mpl

CMD ["fin_dq_engine.orchestration.lambda_handlers.dispatch_handler"]
