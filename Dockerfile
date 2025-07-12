FROM ghcr.io/astral-sh/uv:alpine

WORKDIR /app

COPY . .

RUN apk update --no-cache && apk upgrade --no-cache

RUN uv sync --frozen --no-cache

EXPOSE 80

ENTRYPOINT ["uv", "run", "hypercorn", "main:app", "--bind", "0.0.0.0:80", "-w", "1"]
