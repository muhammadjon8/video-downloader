import { Telegraf, Context } from 'telegraf';
import youtubedl from 'youtube-dl-exec';
import path from 'path';
import fs from 'fs';
import { createReadStream } from 'fs';
import { Agent } from 'https';
import * as dotenv from 'dotenv';
import { MongoClient, Collection, Db } from 'mongodb';
import { normalizeUrl } from './util';

dotenv.config();

const BOT_TOKEN = process.env.BOT_TOKEN;
const DB_URL = process.env.DB_URL;

if (!BOT_TOKEN) throw new Error("BOT_TOKEN must be provided in .env file");
if (!DB_URL) throw new Error("DB_URL must be provided in .env file");

// IPv4 Fix for Telegram API + Increased timeout
const bot = new Telegraf(BOT_TOKEN, {
    handlerTimeout: 300000,
    telegram: {
        agent: new Agent({ keepAlive: true, family: 4 }),
        apiRoot: 'https://api.telegram.org',
    }
});

// MongoDB Client with optimized configuration
const client = new MongoClient(DB_URL, {
    maxPoolSize: 10,
    minPoolSize: 2,
    serverSelectionTimeoutMS: 5000,
    socketTimeoutMS: 45000,
    connectTimeoutMS: 10000,
    retryWrites: true,
    retryReads: true,
    // Add these TLS options if you're having SSL issues
    tls: true,
    tlsAllowInvalidCertificates: false, // Set to true ONLY for development with self-signed certs
});

let db: Db;
let collection: Collection;
let isDbConnected = false;

async function getCachedVideo(url: string): Promise<{ fileId: string, caption?: string } | null> {
    if (!isDbConnected) {
        console.warn("⚠️ DB not connected, skipping cache check");
        return null;
    }
    
    try {
        const result = await collection.findOne({ url }, { maxTimeMS: 2000 });
        if (!result) return null;
        return { 
            fileId: result.fileId as string, 
            caption: result.caption as string 
        };
    } catch (error: any) {
        console.error("DB Get Error:", error.message);
        return null;
    }
}

async function saveFileIdToDb(url: string, fileId: string, caption: string): Promise<void> {
    if (!isDbConnected) {
        console.warn("⚠️ DB not connected, skipping save");
        return;
    }
    
    try {
        await collection.updateOne(
            { url },
            { $set: { fileId, caption, updatedAt: new Date() } },
            { upsert: true, maxTimeMS: 5000 }
        );
        console.log(`💾 Saved to MongoDB: ${url.substring(0, 50)}... -> ${fileId}`);
    } catch (error: any) {
        console.error("DB Save Error:", error.message);
    }
}

interface VideoInfo {
    title?: string;
    duration?: number;
    filesize?: number;
    filesize_approx?: number;
}

bot.command('start', (ctx) => {
    ctx.reply('Hello! Send me a link to a video from Instagram, YouTube, or TikTok and I will download it for you.');
});

bot.on('text', async (ctx: Context) => {
    // 1. Safety Checks
    const messageText = ctx.message && 'text' in ctx.message ? ctx.message.text : undefined;
    const chatId = ctx.chat?.id;
    const messageId = ctx.message?.message_id;

    if (!messageId || !messageText || !chatId) return;

    // 2. Check for Link
    const urlMatch = messageText.match(/(https?:\/\/[^\s]+)/);
    if (!urlMatch) return; 

    const rawUrl = urlMatch[0];
    const cleanUrl = normalizeUrl(rawUrl); 

    let statusMessage: any = null;

    try {
        // 1. CHECK DATABASE FIRST (with quick timeout)
        const cached = await getCachedVideo(cleanUrl);

        if (cached) {
            console.log("⚡ CACHE HIT! Sending instantly...");
            await ctx.sendChatAction('upload_video');
            await ctx.replyWithVideo(cached.fileId, {
                caption: cached.caption || 'Here is your video! 🎥 (Delivered instantly)',
                parse_mode: 'HTML',
                reply_parameters: { message_id: messageId }
            });
            return; 
        }

        // 2. CACHE MISS: FETCH INFO AND DOWNLOAD
        console.log("🐢 CACHE MISS! Fetching info...");
        
        // Send initial status and start fetching info in parallel
        const [statusMsg, info] = await Promise.allSettled([
            ctx.reply("🔍 Fetching video info...", {
                reply_parameters: { message_id: messageId }
            }),
            youtubedl(cleanUrl, {
                dumpSingleJson: true,
                noPlaylist: true,
                noCheckCertificates: true,
                preferFreeFormats: true,
            }).catch((err) => {
                console.warn("Metadata fetch failed, will use defaults:", err.message);
                return { title: "Video", duration: 0, filesize: 0 } as VideoInfo;
            })
        ]);

        // Extract results
        statusMessage = statusMsg.status === 'fulfilled' ? statusMsg.value : null;
        const videoInfo = (info.status === 'fulfilled' ? info.value : { title: "Video", duration: 0, filesize: 0 }) as VideoInfo;

        const title = videoInfo.title || "Video";
        const fileSize = videoInfo.filesize || videoInfo.filesize_approx || 0;
        const sizeMB = fileSize ? (fileSize / (1024 * 1024)).toFixed(2) : "Unknown";
        const durationFormatted = (() => {
            if (!videoInfo.duration) return "Unknown";
            const totalSeconds = Math.round(videoInfo.duration);
            const mins = Math.floor(totalSeconds / 60);
            const secs = totalSeconds % 60;
            return `${mins}:${secs.toString().padStart(2, '0')}`;
        })();

        // Check for size limit (Bot API limit is 50MB for uploads)
        if (fileSize > 50 * 1024 * 1024) {
            const msg = `⚠️ This video is too large (${sizeMB} MB) to upload via Telegram Bot API (Limit: 50MB).`;
            if (statusMessage) {
                await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, msg);
            } else {
                await ctx.reply(msg, { reply_parameters: { message_id: messageId } });
            }
            return;
        }

        // Update status
        if (statusMessage) {
            await ctx.telegram.editMessageText(
                chatId, 
                statusMessage.message_id, 
                undefined, 
                `⏳ Downloading: <b>${title.substring(0, 50)}${title.length > 50 ? '...' : ''}</b>\n(Size: ${sizeMB} MB)`, 
                { parse_mode: 'HTML' }
            ).catch(() => {});
        }
        
        const outputPath = path.resolve(__dirname, `video_${Date.now()}.mp4`);
        
        try {
            await youtubedl(cleanUrl, {
                output: outputPath,
                format: 'best[ext=mp4]/best',
                noPlaylist: true,
                noCheckCertificates: true,
            });
        } catch (downloadErr: any) {
            console.error("Download Error:", downloadErr.message);
            const msg = `❌ Download failed: ${downloadErr.message}`;
            if (statusMessage) {
                await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, msg);
            } else {
                await ctx.reply(msg, { reply_parameters: { message_id: messageId } });
            }
            return;
        }

        // Verify file exists and get actual size
        if (!fs.existsSync(outputPath)) {
            const msg = `❌ Download failed: File not found`;
            if (statusMessage) {
                await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, msg);
            } else {
                await ctx.reply(msg, { reply_parameters: { message_id: messageId } });
            }
            return;
        }

        const stats = fs.statSync(outputPath);
        const actualSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
        
        // Re-check size after download
        if (stats.size > 50 * 1024 * 1024) {
            fs.unlinkSync(outputPath);
            const msg = `⚠️ Downloaded video is too large (${actualSizeMB} MB). Telegram limit: 50MB`;
            if (statusMessage) {
                await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, msg);
            } else {
                await ctx.reply(msg, { reply_parameters: { message_id: messageId } });
            }
            return;
        }
        
        // 3. UPLOAD WITH STREAM
        if (statusMessage) {
            await ctx.telegram.editMessageText(
                chatId, 
                statusMessage.message_id, 
                undefined, 
                `📤 Uploading to Telegram... (${actualSizeMB} MB)`
            ).catch(() => {});
        }
        
        await ctx.sendChatAction('upload_video');

        const caption = 
            `🎬 <b>${title}</b>\n\n` +
            `📦 <b>Size:</b> ${actualSizeMB} MB\n` +
            `⏱️ <b>Duration:</b> ${durationFormatted}`;

        try {
            const sentMessage = await ctx.replyWithVideo(
                { source: createReadStream(outputPath) },
                {
                    caption: caption,
                    parse_mode: 'HTML',
                    reply_parameters: { message_id: messageId },
                    supports_streaming: true,
                }
            );

            // Extract the file_id from the sent message
            // @ts-ignore
            const newFileId = sentMessage.video?.file_id;

            if (newFileId) {
                // 4. SAVE TO DATABASE (don't await, let it happen in background)
                saveFileIdToDb(cleanUrl, newFileId, caption).catch((err) => 
                    console.error("Background save failed:", err.message)
                );
            }
        } catch (uploadErr: any) {
            console.error("Upload Error:", uploadErr.message);
            
            // Handle timeout errors specifically
            if (uploadErr.message.includes('timeout') || uploadErr.message.includes('ETIMEOUT')) {
                await ctx.reply(`⚠️ Upload timeout. The video might still be processing. Please try again in a moment.`, {
                    reply_parameters: { message_id: messageId }
                });
            } else {
                await ctx.reply(`❌ Failed to upload video: ${uploadErr.message}`, {
                    reply_parameters: { message_id: messageId }
                });
            }
        } finally {
            // 5. CLEANUP - Always cleanup regardless of success/failure
            if (statusMessage) {
                await ctx.telegram.deleteMessage(chatId, statusMessage.message_id).catch(() => {});
            }
            if (fs.existsSync(outputPath)) {
                fs.unlinkSync(outputPath);
            }
        }

    } catch (error: any) {
        console.error("Fatal Error:", error.message);
        await ctx.reply(`❌ An unexpected error occurred: ${error.message}`, {
            reply_parameters: { message_id: messageId }
        });
        
        // Cleanup on error
        if (statusMessage) {
            await ctx.telegram.deleteMessage(chatId, statusMessage.message_id).catch(() => {});
        }
    }
});

// Initialize database connection BEFORE launching bot
async function initializeMongoDB() {
    console.log("🔌 Connecting to MongoDB...");
    
    try {
        await client.connect();
        
        // Ping the database to verify connection
        await client.db('admin').command({ ping: 1 });
        
        db = client.db('media_downloader');
        collection = db.collection('video_cache');
        
        // Create index for faster lookups
        await collection.createIndex({ url: 1 }, { unique: true, background: true });
        
        isDbConnected = true;
        console.log("✅ MongoDB Connection Successful!");
        console.log(`📊 Database: ${db.databaseName}`);
        console.log(`📁 Collection: ${collection.collectionName}`);
        
        return true;
    } catch (error: any) {
        console.error("❌ MongoDB Connection Failed:", error.message);
        console.error("Error details:", error);
        console.warn("⚠️ Bot will continue WITHOUT caching functionality");
        isDbConnected = false;
        return false;
    }
}

// Main startup function
async function startBot() {
    // 1. First connect to MongoDB
    await initializeMongoDB();
    
    // 2. Then launch the bot
    await bot.launch();
    
    console.log("🚀 Bot is now listening!");
    console.log(`📊 Cache Status: ${isDbConnected ? 'ENABLED ✅' : 'DISABLED ⚠️'}`);
}

// Start everything
startBot().catch((error) => {
    console.error("Failed to start bot:", error);
    process.exit(1);
});

// Graceful shutdown
process.once('SIGINT', async () => {
    console.log("\n⏹️ Shutting down gracefully...");
    if (isDbConnected) {
        await client.close();
        console.log("✅ MongoDB connection closed");
    }
    bot.stop('SIGINT');
});

process.once('SIGTERM', async () => {
    console.log("\n⏹️ Shutting down gracefully...");
    if (isDbConnected) {
        await client.close();
        console.log("✅ MongoDB connection closed");
    }
    bot.stop('SIGTERM');
});