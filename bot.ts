import { Telegraf, Context } from 'telegraf';
import youtubedl from 'youtube-dl-exec';
import path from 'path';
import fs from 'fs';
import { createReadStream } from 'fs';
import { Agent } from 'https';
import * as dotenv from 'dotenv';
import { MongoClient } from 'mongodb';
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

// MongoDB Client
const client = new MongoClient(DB_URL);
const db = client.db('media_downloader');
const collection = db.collection('video_cache');

async function getCachedVideo(url: string): Promise<{ fileId: string, caption?: string } | null> {
    try {
        const result = await collection.findOne({ url });
        if (!result) return null;
        return { 
            fileId: result.fileId as string, 
            caption: result.caption as string 
        };
    } catch (error) {
        console.error("DB Get Error:", error);
        return null;
    }
}

async function saveFileIdToDb(url: string, fileId: string, caption: string): Promise<void> {
    try {
        await collection.updateOne(
            { url },
            { $set: { fileId, caption, updatedAt: new Date() } },
            { upsert: true }
        );
        console.log(`💾 Saved to MongoDB: ${url} -> ${fileId}`);
    } catch (error) {
        console.error("DB Save Error:", error);
    }
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

    try {
        // 1. CHECK DATABASE FIRST
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
        const statusMessage = await ctx.reply("🔍 Fetching video info...", {
            reply_parameters: { message_id: messageId }
        });

        // Get metadata
        let info: any;
        try {
            // @ts-ignore
            info = await youtubedl(cleanUrl, {
                dumpSingleJson: true,
                noPlaylist: true,
            });
        } catch (infoErr: any) {
            console.error("Metadata Error:", infoErr.message);
            await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, "⚠️ Could not fetch video info, attempting download anyway...");
            info = { title: "Video" }; 
        }

        const title = info.title || "Video";
        const fileSize = info.filesize || info.filesize_approx || 0;
        const sizeMB = fileSize ? (fileSize / (1024 * 1024)).toFixed(2) : "Unknown";
        const duration = info.duration ? Math.floor(info.duration / 60) + ":" + (info.duration % 60).toString().padStart(2, '0') : "Unknown";
        const quality = info.format || info.format_note || "Best";

        // Check for size limit (Bot API limit is 50MB for uploads)
        if (fileSize > 50 * 1024 * 1024) {
             await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, 
                `⚠️ This video is too large (${sizeMB} MB) to upload via Telegram Bot API (Limit: 50MB).`);
             return;
        }

        await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, `⏳ Downloading video: <b>${title}</b>...\n(Size: ${sizeMB} MB)`, { parse_mode: 'HTML' });
        
        const outputPath = path.resolve(__dirname, `video_${Date.now()}.mp4`);
        
        try {
            await youtubedl(cleanUrl, {
                output: outputPath,
                format: 'best[ext=mp4]',
                noPlaylist: true,
            });
        } catch (downloadErr: any) {
            console.error("Download Error:", downloadErr.message);
            await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, `❌ Download failed: ${downloadErr.message}`);
            return;
        }

        // Verify file exists and get actual size
        if (!fs.existsSync(outputPath)) {
            await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, `❌ Download failed: File not found`);
            return;
        }

        const stats = fs.statSync(outputPath);
        const actualSizeMB = (stats.size / (1024 * 1024)).toFixed(2);
        
        // 3. UPLOAD WITH STREAM (KEY FIX HERE)
        await ctx.telegram.editMessageText(chatId, statusMessage.message_id, undefined, `📤 Uploading video to Telegram... (${actualSizeMB} MB)`);
        await ctx.sendChatAction('upload_video');

        const caption = 
            `🎬 <b>${title}</b>\n\n` +
            `📦 <b>Size:</b> ${actualSizeMB} MB\n` +
            `⏱ <b>Duration:</b> ${duration}\n` +
            `🎞 <b>Quality:</b> ${quality}`;

        try {
            // ⭐ KEY FIX: Use createReadStream instead of direct path
            const sentMessage = await ctx.replyWithVideo(
                { source: createReadStream(outputPath) },
                {
                    caption: caption,
                    parse_mode: 'HTML',
                    reply_parameters: { message_id: messageId }
                    
                },
            );

            // Extract the file_id from the sent message
            // @ts-ignore
            const newFileId = sentMessage.video?.file_id;

            if (newFileId) {
                // 4. SAVE TO DATABASE
                await saveFileIdToDb(cleanUrl, newFileId, caption);
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
            await ctx.telegram.deleteMessage(chatId, statusMessage.message_id).catch(() => {});
            if (fs.existsSync(outputPath)) {
                fs.unlinkSync(outputPath);
            }
        }

    } catch (error: any) {
        console.error("Fatal Error:", error.message);
        await ctx.reply(`❌ An unexpected error occurred: ${error.message}`, {
            reply_parameters: { message_id: messageId }
        });
    }
});

bot.launch().then(async () => {
    await client.connect();
    console.log("🚀 Bot is listening with MongoDB cache!");
});

// Graceful stop
process.once('SIGINT', () => {
    client.close();
    bot.stop('SIGINT');
});
process.once('SIGTERM', () => {
    client.close();
    bot.stop('SIGTERM');
});