import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import { Kafka, logLevel } from 'kafkajs';
import { WebSocketServer } from 'ws';

// ===== Config =====
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const TOPIC_MAIN = process.env.TOPIC_MAIN || 'notifications';
const TOPIC_RETRY = process.env.TOPIC_RETRY || 'notifications.retry';
const TOPIC_DLQ = process.env.TOPIC_DLQ || 'notifications.dlq';
const PORT = parseInt(process.env.PORT || '3000', 10);
const MAX_RETRIES = parseInt(process.env.MAX_RETRIES || '3', 10);

// ===== Kafka init =====
const kafka = new Kafka({
  clientId: 'notif-svc',
  brokers: KAFKA_BROKERS,
  logLevel: logLevel.NOTHING
});
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'notif-workers' });

// ===== WebSocket (In-App delivery) =====
const wss = new WebSocketServer({ noServer: true });
// Track clients by userId => Set<WebSocket>
const clients = new Map();

function addClient(userId, ws) {
  if (!clients.has(userId)) clients.set(userId, new Set());
  clients.get(userId).add(ws);

  console.log(`[WS] client connected userId=${userId}, total=${clients.get(userId).size}`);

  ws.on('close', () => {
    clients.get(userId)?.delete(ws);
    console.log(`[WS] client disconnected userId=${userId}`);
  });
}

function pushInApp(userId, payload) {
  const set = clients.get(String(userId));
  if (!set) return;
  const msg = JSON.stringify({ type: 'in-app', payload });
  for (const ws of set) {
    if (ws.readyState === 1) ws.send(msg);
  }
}

// ===== Provider stubs (replace with SES/Twilio/FCM etc.) =====
async function sendEmail({ to, subject, body }) {
  console.log('[EMAIL] to=%s subject=%s', to, subject);
  // throw new Error('email temp fail')
}

async function sendSMS({ to, body }) {
  console.log('[SMS] to=%s body=%s', to, body);
}

async function sendPush({ token, title, body }) {
  console.log('[PUSH] token=%s title=%s', token, title);
}

// ===== Minimal retry handler =====
async function handleFailure(message, reason) {
  const headers = message.headers || {};
  const attempt = parseInt(headers.attempt?.toString() || '0', 10) + 1;
  const key = message.key?.toString();
  const value = message.value;

  if (attempt <= MAX_RETRIES) {
    await producer.send({
      topic: TOPIC_RETRY,
      messages: [
        {
          key,
          value,
          headers: { attempt: Buffer.from(String(attempt)) }
        }
      ]
    });
    console.warn(`requeued -> ${TOPIC_RETRY} attempt=${attempt} reason=${reason}`);
  } else {
    await producer.send({
      topic: TOPIC_DLQ,
      messages: [{ key, value, headers }]
    });
    console.error(`sent to DLQ -> ${TOPIC_DLQ} reason=${reason}`);
  }
}

async function routeAndDeliver(payload) {
  const { channel, userId } = payload;
  if (channel === 'email') return sendEmail(payload);
  if (channel === 'sms') return sendSMS(payload);
  if (channel === 'push') return sendPush(payload);
  if (channel === 'in-app') return pushInApp(userId, payload);
  throw new Error('unknown channel');
}

// ===== Consumers =====
async function startConsumers() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC_MAIN, fromBeginning: false });
  await consumer.subscribe({ topic: TOPIC_RETRY, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        await routeAndDeliver(payload);
      } catch (err) {
        await handleFailure(message, err.message);
      }
    }
  });
}

// ===== API + WebSocket upgrade =====
const app = express();
app.use(cors());
app.use(express.json());

app.get('/', (_req, res) => res.send('🚀 Notification service is running'));
app.get('/health', (_req, res) => res.json({ ok: true }));

// POST /notify -> produces to Kafka
app.post('/notify', async (req, res) => {
  try {
    await producer.send({
      topic: TOPIC_MAIN,
      messages: [
        {
          key: String(req.body.userId || ''),
          value: Buffer.from(JSON.stringify(req.body))
        }
      ]
    });
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Upgrade HTTP -> WS at /ws?userId=123
const server = app.listen(PORT, async () => {
  await producer.connect();
  await startConsumers();
  console.log(`✅ Notification backend running on :${PORT}`);
});

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (url.pathname !== '/ws') return socket.destroy();
  const userId = url.searchParams.get('userId') || 'anon';

  wss.handleUpgrade(req, socket, head, (ws) => {
    addClient(userId, ws);
    ws.send(JSON.stringify({ type: 'welcome', payload: { userId } }));
  });
});
