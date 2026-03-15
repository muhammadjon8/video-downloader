import { Queue, ConnectionOptions } from 'bullmq';
import * as dotenv from 'dotenv';

dotenv.config();

// Use plain connection options so BullMQ uses its *own* bundled ioredis
// and we avoid the dual-ioredis version mismatch TypeScript error.
export const redisConnection: ConnectionOptions = {
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: Number(process.env.REDIS_PORT) || 6379,
    maxRetriesPerRequest: null, // Required by BullMQ
    password: process.env.REDIS_PASSWORD,
};

// Job data shape that gets added to the queue
export interface VideoJobData {
    chatId: number;
    messageId: number;
    statusMessageId: number | null;
    cleanUrl: string;
    rawUrl: string;
}

// Queue name constant — used in both producer (bot.ts) and consumer (worker.ts)
export const QUEUE_NAME = 'video-download';

export const videoQueue = new Queue<VideoJobData, any, string>(QUEUE_NAME, {
    connection: redisConnection,
    defaultJobOptions: {
        attempts: 2,           // Retry once on failure
        backoff: { type: 'exponential', delay: 3000 },
        removeOnComplete: { count: 100 },  // Keep last 100 completed jobs
        removeOnFail: { count: 50 },
    },
});
