const path = require('path');
const io = require('socket.io-client');
const parser = require('socket.io-msgpack-parser');
const { createClient } = require('redis');

// Load environment variables from the listener's .env file
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const cookie = process.env.CLOUDFLARE_COOKIE || '';
let redisUrl = process.env.EDGE_REDIS_URL || 'redis://localhost:6380';
const SALE_FEED_CHANNEL = 'skinport:sale_feed';

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
    const socket = io('https://skinport.com', {
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

    socket.on('saleFeed', async (data) => {
        try {
            // Every event type is forwarded (listed, sold, ...): sold events are the market's own
            // ground truth for outcome labeling. The raw payload is wrapped untouched with a
            // receive timestamp so the listener can persist it verbatim.
            const eventType = data && data.eventType ? data.eventType : 'unknown';
            const sales = (data && data.sales) || [];
            console.log(`[SKINPORT WS] Received ${eventType} event containing ${sales.length} items.`);

            const envelope = { receivedAt: Date.now(), payload: data };
            await redisClient.publish(SALE_FEED_CHANNEL, JSON.stringify(envelope));
        } catch (err) {
            console.error('[SKINPORT WS] Error handling saleFeed event:', err);
        }
    });
}

main().catch((err) => {
    console.error('[SKINPORT WS] Fatal error in main execution loop:', err);
    process.exit(1);
});
