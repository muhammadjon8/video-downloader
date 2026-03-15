import { Telegraf, Context } from 'telegraf';
import { Agent } from 'https';
import * as dotenv from 'dotenv';
import { MongoClient, Collection, Db } from 'mongodb';
import { normalizeUrl } from './util';
import { videoQueue } from './queue';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const DB_URL = process.env.DB_URL;

if (!BOT_TOKEN) throw new Error('BOT_TOKEN must be provided in .env file');
if (!DB_URL) throw new Error('DB_URL must be provided in .env file');

// ─── Bot Setup ────────────────────────────────────────────────────────────────
const bot = new Telegraf(BOT_TOKEN, {
    handlerTimeout: 90_000,
    telegram: {
        agent: new Agent({ keepAlive: true, family: 4 }),
        apiRoot: 'https://api.telegram.org',
    },
});

// ─── MongoDB (cache reads only in bot.ts) ─────────────────────────────────────
const mongoClient = new MongoClient(DB_URL, {
    maxPoolSize: 10,
    minPoolSize: 2,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    retryWrites: true,
    retryReads: true,
    tls: true,
    tlsAllowInvalidCertificates: false,
});

let db: Db;
let collection: Collection;
let isDbConnected = false;

async function getCachedVideo(url: string): Promise<{ fileId: string; caption?: string } | null> {
    if (!isDbConnected) return null;
    try {
        const result = await collection.findOne({ url }, { maxTimeMS: 2000 });
        if (!result) return null;
        return { fileId: result.fileId as string, caption: result.caption as string };
    } catch (err: any) {
        console.error('DB Get Error:', err.message);
        return null;
    }
}

// ─── Commands ─────────────────────────────────────────────────────────────────
bot.command('start', (ctx) => {
    ctx.reply(
        '👋 Hello! Send me a link to a video from Instagram, YouTube, or TikTok and I will download it for you.\n\n' +
        '⚡ Downloads are processed in the background — you\'ll receive your video as soon as it\'s ready!'
    );
});

// ─── Main Message Handler ─────────────────────────────────────────────────────
bot.on('text', async (ctx: Context) => {
    const messageText = ctx.message && 'text' in ctx.message ? ctx.message.text : undefined;
    const chatId = ctx.chat?.id;
    const messageId = ctx.message?.message_id;

    if (!messageId || !messageText || !chatId) return;

    // Only process messages that contain a URL
    const urlMatch = messageText.match(/(https?:\/\/[^\s]+)/);
    if (!urlMatch) return;

    const rawUrl = urlMatch[0];
    const cleanUrl = normalizeUrl(rawUrl);

    try {
        // ── 1. Fast cache check — respond instantly if we've seen this URL ──
        const cached = await getCachedVideo(cleanUrl);

        if (cached) {
            console.log('⚡ CACHE HIT! Sending instantly...');
            await ctx.sendChatAction('upload_video');
            await ctx.replyWithVideo(cached.fileId, {
                caption: cached.caption || '🎥 Here is your video! (Delivered instantly)',
                parse_mode: 'HTML',
                reply_parameters: { message_id: messageId },
            });
            return;
        }

        // ── 2. Cache miss — queue the job and immediately acknowledge ────────
        console.log(`📥 Queue: Adding job for ${cleanUrl.substring(0, 60)}...`);

        // Send a "queued" status message and pass its ID to the worker
        // so it can update the user with real-time progress.
        const statusMessage = await ctx.reply(
            '📥 Your request has been queued!\n⏳ Downloading in the background — I\'ll send the video here when it\'s ready.',
            { reply_parameters: { message_id: messageId } }
        );

        await videoQueue.add(
            'download',
            {
                chatId,
                messageId,
                statusMessageId: statusMessage.message_id,
                cleanUrl,
                rawUrl,
            },
            {
                // Use cleanUrl as deduplication key so multiple users
                // sending the same link don't trigger redundant downloads
                jobId: `url:${cleanUrl}`,
            }
        );

        console.log(`✅ Job enqueued for chat ${chatId}`);
    } catch (error: any) {
        console.error('Error enqueuing job:', error.message);
        await ctx.reply(`❌ Failed to queue your request: ${error.message}`, {
            reply_parameters: { message_id: messageId },
        });
    }
});

// ─── MongoDB init ─────────────────────────────────────────────────────────────
async function initializeMongoDB() {
    console.log('🔌 Connecting to MongoDB...');
    try {
        await mongoClient.connect();
        await mongoClient.db('admin').command({ ping: 1 });
        db = mongoClient.db('media_downloader');
        collection = db.collection('video_cache');
        await collection.createIndex({ url: 1 }, { unique: true, background: true });
        isDbConnected = true;
        console.log('✅ MongoDB Connection Successful!');
    } catch (err: any) {
        console.error('❌ MongoDB Connection Failed:', err.message);
        console.warn('⚠️ Bot will continue WITHOUT caching functionality');
        isDbConnected = false;
    }
}

// ─── Startup ──────────────────────────────────────────────────────────────────
async function startBot() {
    await initializeMongoDB();
    await bot.launch();
    console.log('🚀 Bot is now listening!');
    console.log(`📊 Cache Status: ${isDbConnected ? 'ENABLED ✅' : 'DISABLED ⚠️'}`);
    console.log('📦 Queue: BullMQ ready — start worker.ts to process downloads');
}

startBot().catch((err) => {
    console.error('Failed to start bot:', err);
    process.exit(1);
});

// ─── Graceful Shutdown ────────────────────────────────────────────────────────
const shutdown = async (signal: string) => {
    console.log(`\n⏹️ Shutting down gracefully (${signal})...`);
    bot.stop(signal);
    if (isDbConnected) {
        await mongoClient.close();
        console.log('✅ MongoDB connection closed');
    }
    await videoQueue.close();
    process.exit(0);
};

process.once('SIGINT', () => shutdown('SIGINT'));
process.once('SIGTERM', () => shutdown('SIGTERM'));