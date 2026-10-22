const fs = require('fs');
const path = require('path');
const io = require('socket.io-client');
const parser = require('socket.io-msgpack-parser');
const { createClient } = require('redis');

// Load environment variables from the listener's .env file
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const cookie = process.env.CLOUDFLARE_COOKIE || '';
let redisUrl = process.env.EDGE_REDIS_URL || 'redis://localhost:6380';
const SALE_FEED_CHANNEL = 'skinport:sale_feed';
// The latency benchmark (#175) points the sidecar at a local stand-in for Skinport.
const feedUrl = process.env.SKINPORT_WS_URL || 'https://skinport.com';
// Latency benchmark only: when set, each saleFeed event's timings are kept and written to this file
// once the benchmark's feed server sends `benchFlush`. Times are process.hrtime.bigint() in nanoseconds.
const timingFile = process.env.SIDECAR_TIMING_FILE || '';
const timings = [];

console.log('[SKINPORT WS] Sidecar initializing...');
console.log(`[SKINPORT WS] Target Redis Endpoint: ${redisUrl}`);

const redisClient = createClient({
    url: redisUrl,
    username: 'default',
    password: process.env.REDIS_PASSWORD || undefined
});

redisClient.on('error', (err) => {
    console.error('[SKINPORT WS] Redis client error:', err);
});

async function main() {
    // Connect to local Redis Pub/Sub instance
    await redisClient.connect();
    console.log('[SKINPORT WS] Connected to local Redis successfully.');

    // Connect to Skinport live sale feed
    const socket = io(feedUrl, {
        transports: ['websocket'],
        parser: parser,
        extraHeaders: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Origin': 'https://skinport.com',
            'Referer': 'https://skinport.com/',
            'Cookie': cookie
        }
    });

    socket.on('connect', () => {
        console.log('[SKINPORT WS] WebSocket connected! Subscribing to CS2 USD sale feed...');
        socket.emit('saleFeedJoin', {
            currency: 'USD',
            locale: 'en',
            appid: 730
        });
    });

    if (process.env.DEBUG_TELEMETRY === 'true') {
        socket.onAny((eventName, ...args) => {
            console.log(`[SKINPORT WS ANY] Event: ${eventName}, Args length: ${args.length}`);
            if (eventName === 'saleFeed' && args.length > 0) {
                console.log(`[SKINPORT WS ANY] saleFeed payload:`, JSON.stringify(args[0]).slice(0, 500));
            }
        });
    }

    socket.on('disconnect', (reason) => {
        console.log(`[SKINPORT WS] WebSocket disconnected. Reason: ${reason}`);
    });

    socket.on('connect_error', (error) => {
        console.error('[SKINPORT WS] WebSocket connection error:', error.message || error);
    });

    if (timingFile) {
        socket.on('benchFlush', () => {
            const usage = process.resourceUsage();
            const report = {
                timings,
                cpu_user_us: usage.userCPUTime,
                cpu_system_us: usage.systemCPUTime,
                max_rss_kb: usage.maxRSS,
            };
            fs.writeFileSync(timingFile, JSON.stringify(report));
            socket.emit('benchFlushed', { events: timings.length });
        });
        // Exit normally when the benchmark stops the sidecar, so a --cpu-prof profile gets written.
        process.on('SIGTERM', () => process.exit(0));
    }

    socket.on('saleFeed', async (data) => {
        const callbackAt = timingFile ? process.hrtime.bigint() : 0n;
        try {
            // Every event type is forwarded (listed, sold, ...): sold events are the market's own
            // ground truth for outcome labeling. The raw payload is wrapped untouched with a
            // receive timestamp so the listener can persist it verbatim.
            const eventType = data && data.eventType ? data.eventType : 'unknown';
            const sales = (data && data.sales) || [];
            console.log(`[SKINPORT WS] Received ${eventType} event containing ${sales.length} items.`);

            const envelope = { receivedAt: Date.now(), payload: data };
            const message = JSON.stringify(envelope);
            const publishAt = timingFile ? process.hrtime.bigint() : 0n;
            await redisClient.publish(SALE_FEED_CHANNEL, message);
            if (timingFile) {
                // Keyed by the event's first productId, which the benchmark makes unique.
                const key = sales.length > 0 ? String(sales[0].productId) : '';
                timings.push([key, String(callbackAt), String(publishAt), String(process.hrtime.bigint())]);
            }
        } catch (err) {
            console.error('[SKINPORT WS] Error handling saleFeed event:', err);
        }
    });
}

main().catch((err) => {
    console.error('[SKINPORT WS] Fatal error in main execution loop:', err);
    process.exit(1);
});
