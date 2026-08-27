FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade -r requirements.txt \
    && useradd --create-home --uid 1000 appuser

COPY --chown=appuser:appuser . .
USER appuser

EXPOSE 7860
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:7860/mathquizgen/lti/status', timeout=3)"

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "7860"]