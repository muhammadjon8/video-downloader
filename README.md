# video-downloader

Telegram bot that downloads videos (Instagram/YouTube/TikTok) via yt-dlp and sends them back,
with a Redis-backed download queue and a MongoDB cache for repeat links.

## Run it anywhere

The image is published at `bekhruzbekswe/video-downloader`. You only need `docker-compose.yml`
and a `.env` file on the target machine — no repo checkout, no build:

```bash
# copy docker-compose.yml from this repo to the target machine
echo "BOT_TOKEN=your-token-here" > .env
docker compose pull
docker compose up -d
```

That's it — Mongo and Redis run as bundled containers with no extra setup. Uploads are capped
at Telegram's standard 50MB per file.

## Building from source instead

```bash
cp .env.example .env
# edit .env and set BOT_TOKEN
docker compose up -d --build
```

## Bigger uploads (up to 2GB)

Requires a local Telegram Bot API server, which needs its own app credentials (separate from
your bot token) from https://my.telegram.org/apps.

```bash
# in .env, set:
#   TELEGRAM_API_ID=...
#   TELEGRAM_API_HASH=...
#   TELEGRAM_API_ROOT=http://telegram-bot-api:8081

docker compose --profile local-api up -d --build
```

## YouTube blocking downloads ("Sign in to confirm you're not a bot")

`worker.ts` passes `--extractor-args youtube:player_client=android` to yt-dlp, which avoids
YouTube's bot-check wall for most videos without needing any account or cookies.

## Logs

```bash
docker compose logs -f bot worker
```
