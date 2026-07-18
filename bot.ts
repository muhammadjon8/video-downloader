import { Telegraf, Context } from 'telegraf';
import { Agent as HttpsAgent } from 'https';
import { Agent as HttpAgent } from 'http';
import * as dotenv from 'dotenv';
import { MongoClient, Collection, Db } from 'mongodb';
import { normalizeUrl } from './util';
import { videoQueue } from './queue';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const DB_URL = process.env.DB_URL;
const TELEGRAM_API_ROOT = process.env.TELEGRAM_API_ROOT?.trim() || 'https://api.telegram.org';

if (!BOT_TOKEN) throw new Error('BOT_TOKEN must be provided in .env file');
if (!DB_URL) throw new Error('DB_URL must be provided in .env file');

// ─── Bot Setup ────────────────────────────────────────────────────────────────
// The agent class must match the apiRoot's protocol — an https.Agent used
// against a plain-http local Bot API server silently hangs (it attempts a
// TLS handshake the server never responds to in kind) instead of erroring.
const AgentClass = TELEGRAM_API_ROOT.startsWith('https:') ? HttpsAgent : HttpAgent;

const bot = new Telegraf(BOT_TOKEN, {
    handlerTimeout: 90_000,
    telegram: {
        agent: new AgentClass({ keepAlive: true, family: 4 }),
        apiRoot: TELEGRAM_API_ROOT,
    },
});

// ─── MongoDB (cache reads only in bot.ts) ─────────────────────────────────────
const mongoClient = new MongoClient(DB_URL, {
    maxPoolSize: 10,
    minPoolSize: 2,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 10000,
    retryWrites: true,
    retryReads: true,
});

let db: Db;
let collection: Collection;
let isDbConnected = false;

async function getCachedVideo(url: string): Promise<{ fileId: string; caption?: string } | null> {
    if (!isDbConnected) return null;
    try {
        // Wrap in Promise.race with a hard timeout to prevent handler from hanging
        const result = await Promise.race([
            collection.findOne({ url }, { maxTimeMS: 2000 }),
            new Promise((_, reject) =>
                setTimeout(() => reject(new Error('Cache lookup timeout')), 3000)
            ),
        ]) as any;

        if (!result) return null;
        return { fileId: result.fileId as string, caption: result.caption as string };
    } catch (err: any) {
        // Silently return null on any error to avoid blocking the handler
        return null;
    }
}

// ─── Commands ─────────────────────────────────────────────────────────────────
bot.command('start', (ctx) => {
    ctx.reply(
        '👋 Assalomu alaykum! Menga Instagram, YouTube yoki TikTok dan video havolasini yuboring va men uni sizga yuklab beraman.\n\n' +
        '⚡ Videongiz qisqa vaqt ichida tayyor bo\'ladi!'
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
            
            // Wrap Telegram calls in timeout to prevent handler hang
            await Promise.race([
                (async () => {
                    await ctx.sendChatAction('upload_video');
                    await ctx.replyWithVideo(cached.fileId, {
                        caption: cached.caption || '🎥 Mana sizning videongiz!',
                        parse_mode: 'HTML',
                        reply_parameters: { message_id: messageId },
                    });
                })(),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Telegram API timeout')), 20000)
                ),
            ]);
            return;
        }

        // ── 2. Cache miss — queue the job and immediately acknowledge ────────
        console.log(`📥 Queue: Adding job for ${cleanUrl.substring(0, 60)}...`);

        // Send a friendly status message and pass its ID to the worker
        // so it can update the user with real-time progress.
        const statusMessage = await Promise.race([
            ctx.reply(
                '⏳ Video yuklanmoqda... Bir oz kuting.',
                { reply_parameters: { message_id: messageId } }
            ),
            new Promise((_, reject) => 
                setTimeout(() => reject(new Error('Telegram API timeout')), 10000)
            ),
        ]) as any;

        await videoQueue.add(
            'download',
            {
                chatId,
                messageId,
                statusMessageId: statusMessage.message_id,
                cleanUrl,
                rawUrl,
            }
            // No fixed jobId — each request gets its own job so failed/stuck
            // jobs from previous attempts don't block the same URL from retrying.
        );

        console.log(`✅ Job enqueued for chat ${chatId}`);
    } catch (error: any) {
        console.error('Error processing message:', error.message);
        try {
            await Promise.race([
                ctx.reply(`❌ Xatolik yuz berdi. Iltimos, keyinroq qayta urinib ko'ring.`, {
                    reply_parameters: { message_id: messageId },
                }),
                new Promise((_, reject) => 
                    setTimeout(() => reject(new Error('Telegram API timeout')), 5000)
                ),
            ]);
        } catch {
            // Error reply also failed - just log and continue
        }
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