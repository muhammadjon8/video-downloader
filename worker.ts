/**
 * worker.ts — Background BullMQ worker that handles CPU-intensive video downloads.
 *
 * Run this alongside bot.ts:
 *   npx ts-node worker.ts
 * Or add it to the dev script in package.json.
 *
 * It picks up jobs from the 'video-download' queue and:
 *   1. Fetches video metadata
 *   2. Downloads the video with yt-dlp
 *   3. Uploads it to Telegram
 *   4. Saves the file_id to MongoDB cache
 *   5. Sends status updates back to the user through the Telegram API
 */

import { Worker, Job } from 'bullmq';
import youtubedl from 'youtube-dl-exec';
import { Telegraf } from 'telegraf';
import { Agent } from 'https';
import path from 'path';
import fs from 'fs';
import { createReadStream } from 'fs';
import { MongoClient, Collection, Db } from 'mongodb';
import * as dotenv from 'dotenv';
import { redisConnection, VideoJobData, QUEUE_NAME } from './queue';
// NOTE: No separate ioredis import — we use plain ConnectionOptions from queue.ts

dotenv.config();

// ─── Configuration ────────────────────────────────────────────────────────────
const BOT_TOKEN = process.env.BOT_TOKEN;
const DB_URL = process.env.DB_URL;

if (!BOT_TOKEN) throw new Error('BOT_TOKEN must be provided in .env file');
if (!DB_URL) throw new Error('DB_URL must be provided in .env file');

// Telegraf bot instance (used only to send messages FROM the worker)
const telegram = new Telegraf(BOT_TOKEN, {
    telegram: {
        agent: new Agent({ keepAlive: true, family: 4 }),
        apiRoot: 'https://api.telegram.org',
    },
}).telegram;

// ─── MongoDB ──────────────────────────────────────────────────────────────────
const mongoClient = new MongoClient(DB_URL, {
    maxPoolSize: 5,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    retryWrites: true,
    tls: true,
    tlsAllowInvalidCertificates: false,
});

let db: Db;
let collection: Collection;
let isDbConnected = false;

async function initMongo() {
    try {
        await mongoClient.connect();
        await mongoClient.db('admin').command({ ping: 1 });
        db = mongoClient.db('media_downloader');
        collection = db.collection('video_cache');
        await collection.createIndex({ url: 1 }, { unique: true, background: true });
        isDbConnected = true;
        console.log('✅ Worker: MongoDB connected');
    } catch (err: any) {
        console.error('❌ Worker: MongoDB failed:', err.message);
        isDbConnected = false;
    }
}

async function saveFileIdToDb(url: string, fileId: string, caption: string) {
    if (!isDbConnected) return;
    try {
        await collection.updateOne(
            { url },
            { $set: { fileId, caption, updatedAt: new Date() } },
            { upsert: true, maxTimeMS: 5000 }
        );
        console.log(`💾 Saved to MongoDB: ${url.substring(0, 50)}...`);
    } catch (err: any) {
        console.error('DB Save Error:', err.message);
    }
}

// ─── Helper: Send / Edit status message ───────────────────────────────────────
async function editStatus(chatId: number, msgId: number | null, text: string, html = false) {
    if (!msgId) return;
    try {
        await telegram.editMessageText(chatId, msgId, undefined, text, {
            parse_mode: html ? 'HTML' : undefined,
        });
    } catch {
        // Message might have been deleted or is already up-to-date; ignore
    }
}

// ─── Video Job Processor ──────────────────────────────────────────────────────
async function processVideoJob(job: Job<VideoJobData>) {
    const { chatId, messageId, statusMessageId, cleanUrl } = job.data;

    console.log(`\n🎬 [Job ${job.id}] Processing: ${cleanUrl.substring(0, 60)}...`);

    // Update progress so the queue dashboard shows it
    await job.updateProgress(5);

    // ── Step 1: Fetch metadata ──────────────────────────────────────────────
    let title = 'Video';
    let fileSize = 0;
    let durationFormatted = 'Unknown';
    let sizeMB: string = 'Unknown';

    try {
        await editStatus(chatId, statusMessageId, '🔍 Video tayyorlanmoqda...');

        const info = await youtubedl(cleanUrl, {
            dumpSingleJson: true,
            noPlaylist: true,
            noCheckCertificates: true,
            preferFreeFormats: true,
        }) as any;

        title = info.title || 'Video';
        fileSize = info.filesize || info.filesize_approx || 0;
        sizeMB = fileSize ? (fileSize / (1024 * 1024)).toFixed(2) : 'Unknown';

        if (info.duration) {
            const totalSeconds = Math.round(info.duration);
            const mins = Math.floor(totalSeconds / 60);
            const secs = totalSeconds % 60;
            durationFormatted = `${mins}:${secs.toString().padStart(2, '0')}`;
        }

        // Check Telegram's 50 MB upload limit (pre-download)
        if (fileSize > 50 * 1024 * 1024) {
            await editStatus(
                chatId,
                statusMessageId,
                `⚠️ Uzr, videoning hajmi juda katta (${sizeMB} MB). Men faqat 50 MB gacha bo'lgan videolarni yuklay olaman.`
            );
            return;
        }
    } catch (metaErr: any) {
        console.warn(`⚠️ [Job ${job.id}] Metadata fetch failed:`, metaErr.message);
        // Continue with defaults — yt-dlp will still download
    }

    await job.updateProgress(20);
    await editStatus(
        chatId,
        statusMessageId,
        `⏳ Video yuklanmoqda... \n(Hajmi: ${sizeMB} MB)`,
        true
    );

    // ── Step 2: Download ────────────────────────────────────────────────────
    const outputPath = path.resolve(process.cwd(), `video_${job.id}_${Date.now()}.mp4`);

    try {
        await youtubedl(cleanUrl, {
            output: outputPath,
            format: 'best[ext=mp4]/best',
            noPlaylist: true,
            noCheckCertificates: true,
        });
    } catch (dlErr: any) {
        console.error(`❌ [Job ${job.id}] Download error:`, dlErr.message);
        await editStatus(chatId, statusMessageId, `❌ Videoni yuklab olishda xatolik yuz berdi. Iltimos, keyinroq qayta urinib ko'ring.`);
        throw dlErr; // Let BullMQ handle retry
    }

    if (!fs.existsSync(outputPath)) {
        const msg = '❌ Download failed: File not created on disk.';
        await editStatus(chatId, statusMessageId, `❌ Videoni yuklab olishda xatolik yuz berdi.`);
        throw new Error(msg);
    }

    await job.updateProgress(65);

    const stats = fs.statSync(outputPath);
    const actualSizeMB = (stats.size / (1024 * 1024)).toFixed(2);

    // Re-check size after actual download
    if (stats.size > 50 * 1024 * 1024) {
        fs.unlinkSync(outputPath);
        await editStatus(
            chatId,
            statusMessageId,
            `⚠️ Uzr, yuklab olingan video hajmi juda katta (${actualSizeMB} MB). Men faqat 50 MB gacha bo'lgan videolarni yuklay olaman.`
        );
        return;
    }

    // ── Step 3: Upload ──────────────────────────────────────────────────────
    await editStatus(chatId, statusMessageId, `📤 Telegramga jo'natilmoqda...`);
    await telegram.sendChatAction(chatId, 'upload_video');

    const caption =
        `🎬 <b>${title}</b>\n\n` +
        `📦 <b>Hajmi:</b> ${actualSizeMB} MB\n` +
        `⏱️ <b>Davomiyligi:</b> ${durationFormatted}`;

    try {
        const sentMessage = await telegram.sendVideo(
            chatId,
            { source: createReadStream(outputPath) },
            {
                caption,
                parse_mode: 'HTML',
                reply_parameters: { message_id: messageId },
                supports_streaming: true,
            }
        );

        await job.updateProgress(95);

        // ── Step 4: Cache ───────────────────────────────────────────────────
        const newFileId = sentMessage.video?.file_id;
        if (newFileId) {
            saveFileIdToDb(cleanUrl, newFileId, caption).catch((err) =>
                console.error('Background save failed:', err.message)
            );
        }

        // Delete the status message now that we've sent the video
        if (statusMessageId) {
            await telegram.deleteMessage(chatId, statusMessageId).catch(() => {});
        }

        await job.updateProgress(100);
        console.log(`✅ [Job ${job.id}] Done!`);
    } catch (uploadErr: any) {
        console.error(`❌ [Job ${job.id}] Upload error:`, uploadErr.message);

        if (uploadErr.message?.includes('timeout') || uploadErr.message?.includes('ETIMEOUT')) {
            await telegram.sendMessage(
                chatId,
                `⚠️ Uzr, serverda yuklash vaqti uzayib ketdi. Iltimos, keyinroq qayta urinib ko'ring.`,
                { reply_parameters: { message_id: messageId } }
            );
        } else {
            await telegram.sendMessage(
                chatId,
                `❌ Videoni yuborishda xatolik yuz berdi. Iltimos, keyinroq qayta urinib ko'ring.`,
                { reply_parameters: { message_id: messageId } }
            );
        }
        throw uploadErr; // Trigger BullMQ retry
    } finally {
        if (fs.existsSync(outputPath)) fs.unlinkSync(outputPath);
    }
}

// ─── Worker Setup ─────────────────────────────────────────────────────────────
async function startWorker() {
    await initMongo();

    const worker = new Worker<VideoJobData>(
        QUEUE_NAME,
        processVideoJob,
        {
            connection: redisConnection,
            concurrency: 3,   // Handle up to 3 downloads simultaneously
            limiter: {
                max: 10,      // At most 10 jobs per duration window
                duration: 60_000, // per minute
            },
        }
    );

    worker.on('active', (job) => {
        console.log(`▶️  [Job ${job.id}] started for chat ${job.data.chatId}`);
    });

    worker.on('completed', (job) => {
        console.log(`✅ [Job ${job.id}] completed`);
    });

    worker.on('failed', (job, err) => {
        console.error(`❌ [Job ${job?.id}] failed (attempt ${job?.attemptsMade}):`, err.message);
    });

    worker.on('error', (err) => {
        console.error('Worker error:', err);
    });

    console.log(`\n🚀 Worker is running | concurrency: 3 | queue: "${QUEUE_NAME}"`);

    // Graceful shutdown
    const shutdown = async (signal: string) => {
        console.log(`\n⏹️  Worker shutting down (${signal})...`);
        await worker.close();
        if (isDbConnected) await mongoClient.close();
        // Note: BullMQ manages the Redis connection lifecycle when using ConnectionOptions
        process.exit(0);
    };

    process.once('SIGINT', () => shutdown('SIGINT'));
    process.once('SIGTERM', () => shutdown('SIGTERM'));
}

startWorker().catch((err) => {
    console.error('Failed to start worker:', err);
    process.exit(1);
});
