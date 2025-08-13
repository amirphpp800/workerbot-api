/*
Cloudflare Worker — Telegram WireGuard Bot (Enhanced)

Features:
- Telegram webhook handler
- Workers KV for persistence (binding name: BOT_KV)
- Optional join-to-use check (JOIN_CHAT)
- Upload: user sends a document -> stored metadata in KV -> returns private link /f/<token>
- Download: /f/<token>?uid=<telegram_id>&ref=<referrer_id>
- Enhanced admin panel with authentication, real-time stats, and file management
- Service toggle, file disable/enable, cost management
- Beautiful glassmorphism design with Persian support

Bindings required when deploying:
- KV namespace binding named BOT_KV

Sections (edit guide):
1) Config & Runtime
2) KV helpers
3) Telegram helpers (API wrappers, multipart upload)
4) Utility (time, formatting)
5) Settings & Date helpers
6) Session helpers
7) Inline UI helpers (links, dynamic menus)
8) HTTP entrypoints (fetch, routes)
9) Telegram webhook handling (updates, callbacks)
10) Features & flows:
   - Main menu, Profile & Account
   - Tickets, Balance Transfer
   - Missions, Lottery
   - File management, Gifts
   - Admin panel & Settings (Disable Buttons)
   - Backup (export)
11) Storage helpers (tickets, missions, lottery, files, users)
12) Public endpoints (backup, file download)
*/

/* ==================== 1) Config & Runtime (EDIT HERE) ==================== */
// IMPORTANT: Set secrets in environment variables for production. The values
// below are fallbacks to help local testing. Prefer configuring via `env`.
// EDIT: TELEGRAM_TOKEN, ADMIN_IDS, ADMIN_KEY, WEBHOOK_URL, JOIN_CHAT
const TELEGRAM_TOKEN = "";
const ADMIN_IDS = []; // provide via env `ADMIN_IDS` (comma-separated)
const ADMIN_KEY = ""; // provide via env `ADMIN_KEY`
const WEBHOOK_URL = ""; // provide via env `WEBHOOK_URL`
const JOIN_CHAT = ""; // provide via env `JOIN_CHAT`

// Runtime configuration (populated per-request from env)
let RUNTIME = {
  tgToken: null,
  webhookUrl: null,
  webhookSecret: null,
  adminKey: null,
  adminIds: null,
  joinChat: null,
};

// Main admin and payments config (EDIT: customize display name and packages)
const MAIN_ADMIN_ID = (Array.isArray(ADMIN_IDS) && ADMIN_IDS.length ? ADMIN_IDS : [])[0];
const MAIN_ADMIN_USERNAME = 'minimalcraft'; // for display only
// EDIT: Payment packages (diamonds and prices)
const DIAMOND_PACKAGES = [
  { id: 'd25', diamonds: 25, price_toman: 35000 },
  { id: 'd15', diamonds: 15, price_toman: 25000 }
];
// EDIT: Bank/card details for manual payments
const BANK_CARD_NUMBER = '6219 8619 4308 4037';
const BANK_CARD_NAME = 'امیرحسین سیاهبالائی';

function getDiamondPackageById(id) {
  return DIAMOND_PACKAGES.find(p => p.id === id) || DIAMOND_PACKAGES[0];
}

const TELEGRAM_API = (token) => `https://api.telegram.org/bot${token}`;
const TELEGRAM_FILE_API = (token) => `https://api.telegram.org/file/bot${token}`;

// dynamic admins cache (refreshed per webhook)
let DYNAMIC_ADMIN_IDS = [];

/* ==================== 8) HTTP Entrypoint (router) ==================== */
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    // Populate runtime config from env for this request
    try {
      populateRuntimeFromEnv(env);
    } catch (_) {}

    // Telegram webhook (POST to any path except /api/*) — ack immediately, process in background when possible
    if (request.method === 'POST' && !url.pathname.startsWith('/api/')) {
      // Secret validation disabled per user request
      try {
        if (ctx && request.clone) {
          // ensure webhook stays correct and process update without delaying ack
          ctx.waitUntil(ensureWebhookForRequest(env, request));
          ctx.waitUntil(handleTelegramWebhook(request.clone(), env));
          return new Response('ok');
        }
      } catch (_) {}
      // Fallback inline processing if background not available
      try { await handleTelegramWebhook(request, env); } catch (_) {}
      return new Response('ok');
    }

    // Enhanced main page with admin panel (GET only)
  if (url.pathname === '/' && request.method === 'GET') {
      return handleMainPage(request, env, url, ctx);
    }

    // File public link
    if (url.pathname.startsWith('/f/')) return handleFileDownload(request, env, url);

    // API endpoints for admin panel
  if (url.pathname.startsWith('/api/')) return handleApiRequest(request, env, url, ctx);

    // Health check
    if (url.pathname === '/health') return new Response('ok');

    // 404
    return new Response('Not Found', { status: 404 });
  }
  ,
  // Daily cron handler (configure a Cron Trigger in Cloudflare dashboard)
  async scheduled(controller, env, ctx) {
    const run = runDailyTasks(env);
    if (ctx && typeof ctx.waitUntil === 'function') ctx.waitUntil(run); else await run;
  }
};

/* ==================== 2) KV helpers ==================== */
async function kvGetJson(env, key) {
  const v = await env.BOT_KV.get(key);
  return v ? JSON.parse(v) : null;
}
async function kvPutJson(env, key, obj) {
  return env.BOT_KV.put(key, JSON.stringify(obj));
}
async function kvDelete(env, key) {
  try { return await env.BOT_KV.delete(key); } catch (_) { return; }
}

/* ==================== 3) Telegram helpers ==================== */
function populateRuntimeFromEnv(env) {
  RUNTIME.tgToken = env?.TELEGRAM_TOKEN || TELEGRAM_TOKEN || '';
  RUNTIME.webhookUrl = env?.WEBHOOK_URL || WEBHOOK_URL || '';
  RUNTIME.webhookSecret = null; // secret disabled
  RUNTIME.adminKey = env?.ADMIN_KEY || ADMIN_KEY || '';
  RUNTIME.joinChat = env?.JOIN_CHAT || JOIN_CHAT || '';
  const adminIdsStr = env?.ADMIN_IDS || '';
  if (adminIdsStr && typeof adminIdsStr === 'string') {
    const parsed = adminIdsStr.split(',').map(s => Number(s.trim())).filter(n => Number.isFinite(n));
    if (parsed.length) RUNTIME.adminIds = parsed;
  } else if (!RUNTIME.adminIds || !RUNTIME.adminIds.length) {
    RUNTIME.adminIds = (Array.isArray(ADMIN_IDS) ? ADMIN_IDS : []).map(Number).filter(Number.isFinite);
  }
}

function requireTelegramToken() {
  const token = RUNTIME.tgToken || TELEGRAM_TOKEN;
  if (!token) throw new Error('TELEGRAM_TOKEN is not configured');
  return token;
}

async function tgApi(method, body) {
  const token = requireTelegramToken();
  return fetch(`${TELEGRAM_API(token)}/${method}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body)
  }).then(r => r.json());
}
async function tgGet(path) {
  const token = requireTelegramToken();
  return fetch(`${TELEGRAM_API(token)}/${path}`).then(r => r.json());
}

// Upload helper for multipart/form-data requests (e.g., sendDocument with a file)
async function tgUpload(method, formData) {
  const token = requireTelegramToken();
  return fetch(`${TELEGRAM_API(token)}/${method}`, {
    method: 'POST',
    body: formData
  }).then(r => r.json());
}

// Bot info helpers
async function getBotInfo(env) {
  const token = RUNTIME.tgToken || TELEGRAM_TOKEN;
  const cacheKey = `bot:me:${(token || '').slice(0, 12)}`;
  let info = await kvGetJson(env, cacheKey);
  if (!info) {
    const res = await tgGet('getMe');
    if (res && res.ok) {
      info = res.result;
      await kvPutJson(env, cacheKey, info);
    }
  }
  return info || null;
}
async function getBotUsername(env) {
  const info = await getBotInfo(env);
  return info && info.username ? info.username : null;
}

// Telegram webhook helpers
async function tgSetWebhook(url) {
  try {
    const token = requireTelegramToken();
    const res = await fetch(`${TELEGRAM_API(token)}/setWebhook`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: `url=${encodeURIComponent(url)}`
    });
    return await res.json();
  } catch (_) { return null; }
}
async function tgGetWebhookInfo() {
  try {
    return await tgGet('getWebhookInfo');
  } catch (_) { return null; }
}

/* ==================== 4) Utility ==================== */
function makeToken(len = 20) {
  const bytes = crypto.getRandomValues(new Uint8Array(len));
  return btoa(String.fromCharCode(...bytes)).replace(/[+/=]/g, '').slice(0, len);
}
// Generate a unique 8-digit numeric purchase ID
async function generatePurchaseId(env, maxAttempts = 10) {
  for (let i = 0; i < maxAttempts; i++) {
    const id = String(Math.floor(10000000 + Math.random() * 90000000));
    const exists = await kvGetJson(env, `purchase:${id}`);
    if (!exists) return id;
  }
  const fallback = String(Math.floor(Date.now() % 100000000)).padStart(8, '0');
  return fallback;
}
function now() { return Date.now(); }
function formatDate(timestamp) {
  return new Date(timestamp).toLocaleDateString('fa-IR', { 
    year: 'numeric', month: 'short', day: 'numeric', 
    hour: '2-digit', minute: '2-digit' 
  });
}
function formatFileSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024; const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

/* ==================== 5) Settings & Date helpers ==================== */
let SETTINGS_MEMO = null;
let SETTINGS_MEMO_AT = 0;
async function getSettings(env) {
  const nowTs = now();
  if (SETTINGS_MEMO && (nowTs - SETTINGS_MEMO_AT) < 10000) return SETTINGS_MEMO;
  const s = (await kvGetJson(env, 'bot:settings')) || {};
  SETTINGS_MEMO = {
    welcome_message: s.welcome_message || '',
    daily_limit: Number(s.daily_limit || 0) || 0,
    button_labels: s.button_labels || {},
    disabled_buttons: s.disabled_buttons || {}
  };
  SETTINGS_MEMO_AT = nowTs;
  return SETTINGS_MEMO;
}
async function setSettings(env, settings) {
  await kvPutJson(env, 'bot:settings', settings || {});
  SETTINGS_MEMO = settings || null; SETTINGS_MEMO_AT = now();
}
function isButtonDisabledCached(settings, key) {
  const map = settings && settings.disabled_buttons || {};
  return !!map[key];
}
async function isButtonDisabled(env, key) {
  const s = await getSettings(env);
  return isButtonDisabledCached(s, key);
}
function labelFor(labels, key, fallback) {
  if (!labels) return fallback;
  return (labels[key] && String(labels[key]).trim()) || fallback;
}
function dayKey(ts = now()) {
  const d = new Date(ts);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(d.getUTCDate()).padStart(2, '0');
  return `${y}${m}${dd}`;
}
function weekKey(ts = now()) {
  const d = new Date(ts);
  // ISO week number
  const target = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  const dayNum = (target.getUTCDay() + 6) % 7;
  target.setUTCDate(target.getUTCDate() - dayNum + 3);
  const firstThursday = new Date(Date.UTC(target.getUTCFullYear(), 0, 4));
  const week = 1 + Math.round(((target - firstThursday) / 86400000 - 3 + ((firstThursday.getUTCDay() + 6) % 7)) / 7);
  return `${target.getUTCFullYear()}-W${String(week).padStart(2, '0')}`;
}

/* -------------------- Security helpers -------------------- */
function isValidTokenFormat(token) {
  if (!token || typeof token !== 'string') return false;
  if (token.length < 10 || token.length > 64) return false;
  return /^[A-Za-z0-9_-]+$/.test(token);
}
async function checkRateLimit(env, uid, action, maxOps, windowMs) {
  try {
    const key = `rl:${action}:${uid}`;
    const rec = (await kvGetJson(env, key)) || { start: 0, count: 0 };
    const nowTs = now();
    if (!rec.start || (nowTs - rec.start) > windowMs) {
      await kvPutJson(env, key, { start: nowTs, count: 1 });
      return true;
    }
    if ((rec.count || 0) >= maxOps) return false;
    rec.count = (rec.count || 0) + 1;
    await kvPutJson(env, key, rec);
    return true;
  } catch (_) { return true; }
}

/* ==================== 6) Session helpers ==================== */
async function getSession(env, uid) {
  return (await kvGetJson(env, `session:${uid}`)) || {};
}
async function setSession(env, uid, session) {
  return kvPutJson(env, `session:${uid}`, session || {});
}

/* ==================== 7) Inline UI helpers ==================== */
function domainFromWebhook() {
  const w = RUNTIME.webhookUrl || WEBHOOK_URL;
  if (!w) return '';
  return `https://${new URL(w).host}`;
}
async function getShareLink(env, token) {
  const botUsername = await getBotUsername(env);
  const domain = domainFromWebhook();
  return botUsername ? `https://t.me/${botUsername}?start=d_${token}` : (domain ? `${domain}/f/${token}` : `/f/${token}`);
}
async function buildDynamicMainMenu(env, uid) {
  const isAdminUser = isAdmin(uid);
  const settings = await getSettings(env);

  // Build rows explicitly per requested order
  const rows = [];

  // Row 1: Referral (renamed) side-by-side with User Account
  rows.push([
    { text: '👥 زیرمجموعه گیری', callback_data: 'SUB:REFERRAL' },
    { text: '👤 حساب کاربری', callback_data: 'SUB:ACCOUNT' }
  ]);

  // Row 2: Gift Code | Get by Token
  rows.push([
    { text: labelFor(settings.button_labels, 'gift', '🎁 کد هدیه'), callback_data: 'REDEEM_GIFT' },
    { text: labelFor(settings.button_labels, 'get_by_token', '🔑 دریافت با توکن'), callback_data: 'GET_BY_TOKEN' }
  ]);
  // Add Support under Gift/Token row
  rows.push([
    { text: '🆘 پشتیبانی', callback_data: 'SUPPORT' }
  ]);

  // Row 3: (Support removed per request)

  // Row 4: Lottery side-by-side with Missions
  rows.push([
    { text: labelFor(settings.button_labels, 'lottery', '🎟 قرعه‌کشی'), callback_data: 'LOTTERY' },
    { text: labelFor(settings.button_labels, 'missions', '📆 مأموریت‌ها'), callback_data: 'MISSIONS' }
  ]);

  // Row 5: Buy Diamonds (single)
  rows.push([{ text: labelFor(settings.button_labels, 'buy_points', '💳 خرید الماس'), callback_data: 'BUY_DIAMONDS' }]);

  // Row 6 (bottom): Admin Panel (only for admins)
  if (isAdminUser) {
    rows.push([{ text: '🛠 پنل مدیریت', callback_data: 'ADMIN:PANEL' }]);
  }

  return { inline_keyboard: rows };
}

function buildAdminPanelKeyboard() {
  const rows = [];
  rows.push([
    { text: '📚 راهنما', callback_data: 'HELP' },
    { text: '📊 آمار', callback_data: 'ADMIN:STATS' }
  ]);
  rows.push([
    { text: '🛠 حالت آپدیت', callback_data: 'ADMIN:TOGGLE_UPDATE' }
  ]);
  rows.push([
    { text: '📢 ارسال اعلان', callback_data: 'ADMIN:BROADCAST' },
    { text: '⚙️ تنظیمات سرویس', callback_data: 'ADMIN:SETTINGS' }
  ]);
  rows.push([
    { text: '📂 مدیریت فایل‌ها', callback_data: 'MYFILES:0' },
    { text: '📤 آپلود فایل', callback_data: 'ADMIN:UPLOAD' }
  ]);
  rows.push([
    { text: '📤 آپلود گروهی', callback_data: 'ADMIN:BULK_UPLOAD' },
    { text: '📣 کانال‌های اجباری', callback_data: 'ADMIN:MANAGE_JOIN' }
  ]);
  rows.push([
    { text: '👑 مدیریت ادمین‌ها', callback_data: 'ADMIN:MANAGE_ADMINS' },
    { text: '🎁 مدیریت گیفت‌کد', callback_data: 'ADMIN:GIFTS' }
  ]);
  rows.push([
    { text: '🎯 افزودن الماس', callback_data: 'ADMIN:GIVEPOINTS' },
    { text: '📆 ماموریت‌ها', callback_data: 'ADMIN:MISSIONS' }
  ]);
  rows.push([
    { text: '🗄 تهیه پشتیبان', callback_data: 'ADMIN:BACKUP' }
  ]);
  rows.push([
    { text: '💳 مدیریت پرداخت‌ها', callback_data: 'ADMIN:PAYMENTS' }
  ]);
  rows.push([
    { text: '🚫 غیرفعال‌سازی دکمه‌ها', callback_data: 'ADMIN:DISABLE_BTNS' }
  ]);
  rows.push([
    { text: '🎟 قرعه‌کشی', callback_data: 'ADMIN:LOTTERY' }
  ]);
  rows.push([
    { text: '🧾 مدیریت تیکت‌ها', callback_data: 'ADMIN:TICKETS' }
  ]);
  rows.push([{ text: '🏠 بازگشت به منو', callback_data: 'MENU' }]);
  return { inline_keyboard: rows };
}

function buildFileManageKeyboard(token, file, isAdminUser) {
  const rows = [];
  rows.push([
    { text: '📥 دریافت', callback_data: `SEND:${token}` },
    { text: '🔗 لینک', callback_data: `LINK:${token}` }
  ]);
  if (isAdminUser) {
    rows.push([
      { text: `💰 هزینه (${(file?.cost_points||0)})`, callback_data: `COST:${token}` },
      { text: file?.disabled ? '🟢 فعال‌سازی' : '🔴 غیرفعال', callback_data: `TOGGLE:${token}` },
      { text: '🗑 حذف', callback_data: `DEL:${token}` }
    ]);
    rows.push([
      { text: `🔒 محدودیت (${(file?.max_downloads||0) > 0 ? file.max_downloads : '∞'})`, callback_data: `LIMIT:${token}` },
      { text: `${file?.delete_on_limit ? '🗑 حذف پس از اتمام: روشن' : '🗑 حذف پس از اتمام: خاموش'}`, callback_data: `DELAFTER:${token}` }
    ]);
    rows.push([
      { text: '♻️ جایگزینی محتوا', callback_data: `REPLACE:${token}` }
    ]);
  } else {
    // Regular user: allow proposing new name or viewing details
    rows.push([
      { text: '✏️ تغییر نام', callback_data: `RENAME:${token}` }
    ]);
  }
  rows.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
  return { inline_keyboard: rows };
}
function buildCostKeyboard(token) {
  return {
    inline_keyboard: [
      [
        { text: '0', callback_data: `COST_SET:${token}:0` },
        { text: '1', callback_data: `COST_SET:${token}:1` },
        { text: '2', callback_data: `COST_SET:${token}:2` },
        { text: '5', callback_data: `COST_SET:${token}:5` },
        { text: '10', callback_data: `COST_SET:${token}:10` }
      ],
      [
        { text: '🔢 مقدار دلخواه', callback_data: `COST_CUSTOM:${token}` },
        { text: '⬅️ بازگشت', callback_data: 'MYFILES:0' }
      ]
    ]
  };
}

function buildLimitKeyboard(token) {
  return {
    inline_keyboard: [
      [
        { text: '♾️ بدون محدودیت', callback_data: `LIMIT_SET:${token}:0` },
        { text: '1', callback_data: `LIMIT_SET:${token}:1` },
        { text: '3', callback_data: `LIMIT_SET:${token}:3` },
        { text: '5', callback_data: `LIMIT_SET:${token}:5` },
        { text: '10', callback_data: `LIMIT_SET:${token}:10` }
      ],
      [
        { text: '🔢 مقدار دلخواه', callback_data: `LIMIT_CUSTOM:${token}` },
        { text: '⬅️ بازگشت', callback_data: 'MYFILES:0' }
      ]
    ]
  };
}
async function buildMyFilesKeyboard(env, uid, page = 0, pageSize = 5) {
  const upKey = `uploader:${uid}`;
  const list = (await kvGetJson(env, upKey)) || [];
  const start = Math.max(0, page * pageSize);
  const slice = list.slice(start, start + pageSize);
  const files = [];
  for (const t of slice) {
    const f = await kvGetJson(env, `file:${t}`);
    if (f) files.push(f);
  }
  const isUserAdmin = isAdmin(uid);
  const rows = files.flatMap(f => ([(
    [{ text: `ℹ️ ${f.name || 'file'}`, callback_data: `DETAILS:${f.token}:${page}` }]
  ), (
    [
      { text: `📥 دریافت`, callback_data: `SEND:${f.token}` },
      ...(isUserAdmin ? [{ text: `💰 هزینه (${f.cost_points||0})`, callback_data: `COST:${f.token}` }] : []),
      ...(isUserAdmin ? [{ text: f.disabled ? '🟢 فعال‌سازی' : '🔴 غیرفعال', callback_data: `TOGGLE:${f.token}` }] : []),
      ...(isUserAdmin ? [{ text: '🗑 حذف', callback_data: `DEL:${f.token}` }] : [])
    ]
  )]));
  const nav = [];
  if (start > 0) nav.push({ text: '⬅️ قبلی', callback_data: `MYFILES:${page-1}` });
  if (start + pageSize < list.length) nav.push({ text: 'بعدی ➡️', callback_data: `MYFILES:${page+1}` });
  if (nav.length) rows.push(nav);
  rows.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
  const text = files.length
    ? `📂 ${files.length} فایل اخیر شما (صفحه ${page+1})`
    : 'هنوز فایلی ندارید.';
  return { text, reply_markup: { inline_keyboard: rows } };
}
async function sendMainMenu(env, chatId, uid) {
  await tgApi('sendMessage', {
    chat_id: chatId,
    text: 'لطفا یک گزینه را انتخاب کنید:',
    reply_markup: await buildDynamicMainMenu(env, uid)
  });
}

/* ==================== 9) Telegram webhook handling ==================== */
async function handleTelegramWebhook(req, env) {
  let body;
  try { body = await req.json(); } catch (e) { return new Response('invalid json', { status: 400 }); }
  await handleUpdate(body, env);
  return new Response('ok');
}

// Exported for Cloudflare Pages Functions: processes a Telegram update object
export async function handleUpdate(update, env, ctx) {
  try { populateRuntimeFromEnv(env); } catch (_) {}
  try { await kvPutJson(env, 'bot:last_webhook', now()); } catch (_) {}
  try { DYNAMIC_ADMIN_IDS = (await kvGetJson(env, 'bot:admins'))?.map(Number) || []; } catch (_) { DYNAMIC_ADMIN_IDS = []; }
  try { if (update && update.message) await onMessage(update.message, env); } catch (_) {}
  try { if (update && update.callback_query) await onCallback(update.callback_query, env); } catch (_) {}
}

/* -------------------- Message handlers -------------------- */
async function onMessage(msg, env) {
  const chatId = msg.chat.id;
  const from = msg.from || {};
  const uid = from.id;

  // enforce block
  if (!isAdmin(uid)) {
    try {
      const blocked = await isUserBlocked(env, uid);
      if (blocked) {
        await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ دسترسی شما توسط مدیر محدود شده است.' });
        return;
      }
    } catch (_) {}
  }

  // save/update user
  const userKey = `user:${uid}`;
  let user = (await kvGetJson(env, userKey)) || { 
    id: uid, username: from.username || null, first_name: from.first_name || '',
    diamonds: 0, referrals: 0, joined: false, created_at: now() 
  };
  user.username = from.username || user.username;
  user.first_name = from.first_name || user.first_name;
  user.last_seen = now();
  await kvPutJson(env, userKey, user);

  // ensure users index
  const usersIndex = (await kvGetJson(env, 'index:users')) || [];
  if (!usersIndex.includes(uid)) { usersIndex.push(uid); await kvPutJson(env, 'index:users', usersIndex); }

  // Lottery auto-enroll for new users
  if (usersIndex.length && usersIndex[usersIndex.length - 1] === uid) {
    try { await lotteryAutoEnroll(env, uid); } catch (_) {}
  }

  const text = (msg.text || '').trim();

  // session-driven flows
  const session = await getSession(env, uid);
  if (session.awaiting) {
    // Answering a quiz mission (legacy text-answer). One attempt only.
    if (session.awaiting?.startsWith('mis_quiz_answer:') && text) {
      const id = session.awaiting.split(':')[1];
      const m = await kvGetJson(env, `mission:${id}`);
      if (!m || !m.enabled || m.type !== 'quiz') { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'ماموریت یافت نشد.' }); return; }
      const prog = await getUserMissionProgress(env, uid);
      const markKey = `${m.id}:${weekKey()}`;
      if ((prog.map||{})[markKey]) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'قبلاً پاسخ داده‌اید.' }); return; }
      const correct = String(m.config?.answer || '').trim().toLowerCase();
      const userAns = text.trim().toLowerCase();
      if (correct && userAns === correct) {
        await completeMissionIfEligible(env, uid, m);
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: `✅ درست جواب دادید! ${m.reward} الماس دریافت کردید.` });
      } else {
        prog.map = prog.map || {}; prog.map[markKey] = now(); await setUserMissionProgress(env, uid, prog);
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: '❌ پاسخ نادرست است. امکان پاسخ مجدد وجود ندارد.' });
      }
      return;
    }
    // Answering a weekly question/contest
    if (session.awaiting?.startsWith('mis_question_answer:') && text) {
      const id = session.awaiting.split(':')[1];
      const m = await kvGetJson(env, `mission:${id}`);
      if (!m || !m.enabled || m.type !== 'question') { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'ماموریت یافت نشد.' }); return; }
      const prog = await getUserMissionProgress(env, uid);
      const markKey = `${m.id}:${weekKey()}`;
      if ((prog.map||{})[markKey]) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'قبلاً پاسخ داده‌اید.' }); return; }
      const correct = String(m.config?.answer || '').trim().toLowerCase();
      const userAns = text.trim().toLowerCase();
      if (correct && userAns === correct) {
        await completeMissionIfEligible(env, uid, m);
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: `🏆 پاسخ صحیح! ${m.reward} الماس دریافت کردید.` });
      } else {
        prog.map = prog.map || {}; prog.map[markKey] = now(); await setUserMissionProgress(env, uid, prog);
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: '❌ پاسخ نادرست است. امکان پاسخ مجدد وجود ندارد.' });
      }
      return;
    }
    // Set custom cost for a file
    // Balance: get receiver id
    if (session.awaiting === 'bal:to' && text) {
      const toId = Number(text.trim());
      if (!Number.isFinite(toId) || toId <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی نامعتبر است.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      if (String(toId) === String(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'نمی‌توانید به خودتان انتقال دهید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      const usersIndex = (await kvGetJson(env, 'index:users')) || [];
      if (!usersIndex.includes(toId)) { await tgApi('sendMessage', { chat_id: chatId, text: 'کاربر مقصد یافت نشد.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      await setSession(env, uid, { awaiting: `bal:amount:${toId}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'مبلغ انتقال (الماس) را وارد کنید (حداقل 2 و حداکثر 50):', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (session.awaiting?.startsWith('setcost:') && text) {
      const token = session.awaiting.split(':')[1];
      const amt = Number(text.trim());
      await setSession(env, uid, {});
      if (!Number.isFinite(amt) || amt < 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
      const f = await kvGetJson(env, `file:${token}`);
      if (!f) { await tgApi('sendMessage', { chat_id: chatId, text: 'فایل یافت نشد.' }); return; }
      if (!isAdmin(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'اجازه ندارید.' }); return; }
      f.cost_points = amt; await kvPutJson(env, `file:${token}`, f);
      await tgApi('sendMessage', { chat_id: chatId, text: `هزینه تنظیم شد: ${amt}` });
      return;
    }
    // Set custom download limit for a file
    if (session.awaiting?.startsWith('setlimit:') && text) {
      const token = session.awaiting.split(':')[1];
      const amt = Number(text.trim());
      await setSession(env, uid, {});
      if (!Number.isFinite(amt) || amt < 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
      const f = await kvGetJson(env, `file:${token}`);
      if (!f) { await tgApi('sendMessage', { chat_id: chatId, text: 'فایل یافت نشد.' }); return; }
      if (!isAdmin(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'اجازه ندارید.' }); return; }
      f.max_downloads = Math.max(0, Math.floor(amt));
      await kvPutJson(env, `file:${token}`, f);
      await tgApi('sendMessage', { chat_id: chatId, text: `محدودیت دانلود تنظیم شد: ${f.max_downloads || 'نامحدود'}` });
      return;
    }
    // Balance: get amount
    if (session.awaiting?.startsWith('bal:amount:') && text) {
      const toId = Number(session.awaiting.split(':')[2]);
      const amount = Math.floor(Number(text.trim()));
      if (!Number.isFinite(amount) || amount < 2 || amount > 50) { await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار نامعتبر. باید بین 2 تا 50 الماس باشد.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      const fromUser = (await kvGetJson(env, `user:${uid}`)) || { id: uid, diamonds: 0 };
      if ((fromUser.diamonds || 0) < amount) { await tgApi('sendMessage', { chat_id: chatId, text: 'الماس کافی نیست.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      await setSession(env, uid, {});
      const kb = { inline_keyboard: [
        [{ text: '✅ تایید و انتقال', callback_data: `BAL:CONFIRM:${toId}:${amount}` }],
        [{ text: '❌ انصراف', callback_data: 'CANCEL' }]
      ] };
      await tgApi('sendMessage', { chat_id: chatId, text: `تایید انتقال:\nگیرنده: ${toId}\nمبلغ: ${amount} الماس\n\nآیا تایید می‌کنید؟`, reply_markup: kb });
      return;
    }
    // User replies inside an existing ticket
    if (session.awaiting?.startsWith('tkt_user_reply:')) {
      const ticketId = session.awaiting.split(':')[1];
      const t = await getTicket(env, ticketId);
      if (!t || String(t.user_id) !== String(uid) || t.status === 'closed') { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'ارسال نامعتبر.' }); return; }
      if (!text) { await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً پاسخ را به صورت متن ارسال کنید.' }); return; }
      await setSession(env, uid, {});
      await appendTicketMessage(env, ticketId, { from: 'user', by: uid, at: now(), text });
      // notify all admins
      try {
        const admins = await getAdminIds(env);
        for (const aid of admins) {
          try { await tgApi('sendMessage', { chat_id: aid, text: `پیام جدید در تیکت #${ticketId} از ${uid}:\n${text}` }); } catch (_) {}
        }
      } catch (_) {}
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پیام شما به تیکت افزوده شد.' });
      return;
    }
    // Admin ticket reply flow
    if (session.awaiting?.startsWith('admin_ticket_reply:')) {
      if (!isAdmin(uid)) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین‌ها مجاز هستند.' }); return; }
      const ticketId = session.awaiting.split(':')[1];
      const t = await getTicket(env, ticketId);
      if (!t) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'تیکت یافت نشد.' }); return; }
      if (!text) { await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً پاسخ را به صورت متن ارسال کنید.' }); return; }
      // try sending the message to user first
      let delivered = false;
      try {
        await tgApi('sendMessage', { chat_id: t.user_id, text: `✉️ پاسخ پشتیبانی به تیکت #${t.id}:\n${text}` });
        delivered = true;
      } catch (_) { delivered = false; }
      if (!delivered) { await tgApi('sendMessage', { chat_id: chatId, text: '❌ ارسال پیام به کاربر انجام نشد (ممکن است کاربر پیام‌های ربات را مسدود کرده باشد).' }); return; }
      await setSession(env, uid, {});
      // append message to ticket only after successful delivery
      await appendTicketMessage(env, ticketId, { from: 'admin', by: uid, at: now(), text });
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پاسخ ارسال شد.' });
      return;
    }
    // User ticket creation steps (simplified: Category -> Description -> Submit)
    if (session.awaiting === 'ticket:new:category' && text) {
      const category = text.trim().slice(0, 50);
      const base = { category };
      await setSession(env, uid, { awaiting: `ticket:new:desc:${btoa(encodeURIComponent(JSON.stringify(base)))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'شرح کامل تیکت را ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    // Back-compat: if old subject step appears, treat input as description
    if (session.awaiting?.startsWith('ticket:new:subject:') && text) {
      const base64 = session.awaiting.split(':')[3];
      const base = JSON.parse(decodeURIComponent(atob(base64)));
      const desc = text.trim().slice(0, 2000);
      // Show confirmation
      const preview = `بررسی و تایید:\nدسته: ${base.category}\nشرح:\n${desc.slice(0, 200)}${desc.length>200?'...':''}`;
      const payload = btoa(encodeURIComponent(JSON.stringify({ category: base.category, desc })));
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: preview, reply_markup: { inline_keyboard: [[{ text: '✅ ثبت', callback_data: `TKT:SUBMIT:${payload}` }],[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (session.awaiting?.startsWith('ticket:new:desc:') && text) {
      const base64 = session.awaiting.split(':')[3];
      const base = JSON.parse(decodeURIComponent(atob(base64)));
      const desc = text.trim().slice(0, 2000);
      // Show confirmation before submit
      const preview = `بررسی و تایید:\nدسته: ${base.category}\nشرح:\n${desc.slice(0, 200)}${desc.length>200?'...':''}`;
      const payload = btoa(encodeURIComponent(JSON.stringify({ category: base.category, desc })));
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: preview, reply_markup: { inline_keyboard: [[{ text: '✅ ثبت', callback_data: `TKT:SUBMIT:${payload}` }],[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    // Admin generic upload flow (supports text/media/doc)
    if (session.awaiting === 'upload_wait') {
      if (!isAdmin(uid)) {
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین‌ها مجاز هستند.' });
        return;
      }
      const created = await handleAnyUpload(msg, env, { ownerId: uid });
      if (!created) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'نوع محتوا پشتیبانی نمی‌شود. متن، سند، عکس، ویدیو، صدا یا ویس ارسال کنید.' });
        return;
      }
      await setSession(env, uid, {});
      const manageKb = buildFileManageKeyboard(created.token, created, true);
      const caption = created.type === 'text'
        ? `✅ متن ذخیره شد\nتوکن: ${created.token}`
        : `✅ آیتم ذخیره شد\nنام: ${created.name || created.type}\nتوکن: ${created.token}`;
      await tgApi('sendMessage', { chat_id: chatId, text: caption, reply_markup: manageKb });
      return;
    }
    // Bulk upload: append tokens on each successful upload
    if (session.awaiting === 'bulk_upload') {
      if (!isAdmin(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین‌ها.' }); return; }
      const created = await handleAnyUpload(msg, env, { ownerId: uid });
      if (created) {
        const sess2 = await getSession(env, uid);
        const arr = Array.isArray(sess2.tokens) ? sess2.tokens : [];
        arr.push(created.token);
        await setSession(env, uid, { awaiting: 'bulk_upload', tokens: arr });
        await tgApi('sendMessage', { chat_id: chatId, text: `✅ افزوده شد (${arr.length}): ${created.token}` });
      } else {
        await tgApi('sendMessage', { chat_id: chatId, text: 'نوع محتوا پشتیبانی نمی‌شود.' });
      }
      return;
    }

    // Admin replace existing content
    if (session.awaiting?.startsWith('replace:')) {
      const token = session.awaiting.split(':')[1];
      if (!isAdmin(uid)) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین‌ها مجاز هستند.' }); return; }
      const existed = await kvGetJson(env, `file:${token}`);
      if (!existed) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'آیتم یافت نشد.' }); return; }
      const updated = await handleAnyUpload(msg, env, { ownerId: existed.owner, replaceToken: token, original: existed });
      if (!updated) { await tgApi('sendMessage', { chat_id: chatId, text: 'نوع محتوا پشتیبانی نمی‌شود. متن، سند، عکس، ویدیو، صدا یا ویس ارسال کنید.' }); return; }
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: `✅ محتوا جایگزین شد برای توکن ${token}` });
      return;
    }
    // Support flow: forward next message to main admin
    if (session.awaiting === 'support_wait') {
      const header = `📨 پیام پشتیبانی از کاربر ${uid}${from.username ? ` (@${from.username})` : ''}`;
      let forwarded = false;
      if (msg.text) {
        await tgApi('sendMessage', { chat_id: MAIN_ADMIN_ID, text: `${header}\n\n${msg.text}`, reply_markup: { inline_keyboard: [[{ text: '✉️ پاسخ', callback_data: `SUPREPLY:${uid}` }]] } });
        forwarded = true;
      } else if (msg.photo && msg.photo.length) {
        const p = msg.photo[msg.photo.length - 1];
        await tgApi('sendPhoto', { chat_id: MAIN_ADMIN_ID, photo: p.file_id, caption: header, reply_markup: { inline_keyboard: [[{ text: '✉️ پاسخ', callback_data: `SUPREPLY:${uid}` }]] } });
        forwarded = true;
      } else if (msg.document) {
        await tgApi('sendDocument', { chat_id: MAIN_ADMIN_ID, document: msg.document.file_id, caption: header, reply_markup: { inline_keyboard: [[{ text: '✉️ پاسخ', callback_data: `SUPREPLY:${uid}` }]] } });
        forwarded = true;
      }
      if (forwarded) {
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: '✅ پیام شما به پشتیبانی ارسال شد. پاسخ از طریق همین ربات اطلاع‌رسانی می‌شود.' });
        return;
      }
      await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً متن یا تصویر پیام خود را ارسال کنید.' });
      return;
    }

    // Payment receipt upload
    if (session.awaiting?.startsWith('payment_receipt:')) {
      const purchaseId = session.awaiting.split(':')[1];
      const pKey = `purchase:${purchaseId}`;
      const purchase = await kvGetJson(env, pKey);
      if (!purchase || purchase.user_id !== uid || purchase.status !== 'awaiting_receipt') {
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ درخواست خرید نامعتبر یا منقضی است.' });
        return;
      }
      let fileId = null; let isPhoto = false;
      if (msg.photo && msg.photo.length) { fileId = msg.photo[msg.photo.length - 1].file_id; isPhoto = true; }
      else if (msg.document) { fileId = msg.document.file_id; }
      else if (msg.text) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'برای ادامه، تصویر رسید پرداخت را به صورت عکس یا فایل ارسال کنید.' });
        return;
      }
      if (!fileId) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً تصویر رسید پرداخت را ارسال کنید.' });
        return;
      }
      purchase.receipt_file_id = fileId;
      purchase.status = 'pending_review';
      purchase.updated_at = now();
      await kvPutJson(env, pKey, purchase);
      await setSession(env, uid, {});

      const caption = `درخواست خرید الماس\nشناسه: ${purchase.id}\nکاربر: ${uid}${from.username ? ` (@${from.username})` : ''}\nالماس: ${purchase.diamonds}\nمبلغ: ${purchase.price_toman.toLocaleString('fa-IR')} تومان`;
      const kb = { inline_keyboard: [[
        { text: '✅ تایید و افزودن الماس', callback_data: `PAYAPP:${purchase.id}` },
        { text: '❌ رد', callback_data: `PAYREJ:${purchase.id}` }
      ]] };
      try {
        const admins = await getAdminIds(env);
        let recipients = [];
        if (Array.isArray(admins) && admins.length) {
          recipients = admins;
        } else if (MAIN_ADMIN_ID) {
          recipients = [Number(MAIN_ADMIN_ID)];
        } else if (RUNTIME.adminIds && RUNTIME.adminIds.length) {
          recipients = RUNTIME.adminIds.map(Number);
        }
        if (!recipients.length) {
          await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ مدیر پیکربندی نشده است. رسید ذخیره شد و پس از تنظیم مدیر بررسی می‌شود.' });
        } else {
          for (const aid of recipients) {
            try {
              if (isPhoto) {
                await tgApi('sendPhoto', { chat_id: aid, photo: fileId, caption, reply_markup: kb });
              } else {
                await tgApi('sendDocument', { chat_id: aid, document: fileId, caption, reply_markup: kb });
              }
            } catch (_) {}
          }
        }
      } catch (_) {}
      await tgApi('sendMessage', { chat_id: chatId, text: `✅ رسید دریافت شد.\nشناسه خرید: ${purchase.id}\nنتیجه بررسی به شما اعلام می‌شود.` });
      return;
    }
    if (session.awaiting === 'broadcast' && isAdmin(uid) && text) {
      await tgApi('sendMessage', { chat_id: chatId, text: 'در حال ارسال پیام به همه کاربران...' });
      const res = await broadcast(env, text);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: `ارسال شد ✅\nموفق: ${res.successful}\nناموفق: ${res.failed}` });
      return;
    }
    if (session.awaiting === 'join_add' && isAdmin(uid) && text) {
      const channels = await getRequiredChannels(env);
      const ch = normalizeChannelIdentifier(text);
      if (!channels.includes(ch)) channels.push(ch);
      await setRequiredChannels(env, channels);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: `کانال ${ch} اضافه شد.` });
      return;
    }
    if (session.awaiting === 'add_admin' && isAdmin(uid) && text) {
      const id = Number(text.trim());
      if (!Number.isFinite(id)) { await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی نامعتبر است.' }); return; }
      const admins = await getAdminIds(env);
      if (!admins.includes(id)) admins.push(id);
      await setAdminIds(env, admins);
      DYNAMIC_ADMIN_IDS = admins.slice();
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: `ادمین ${id} اضافه شد.` });
      return;
    }
    if (session.awaiting.startsWith('setcost:') && text) {
      const token = session.awaiting.split(':')[1];
      if (!isAdmin(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین مجاز است.' }); await setSession(env, uid, {}); return; }
      const pts = parseInt(text, 10);
      if (Number.isFinite(pts) && pts >= 0) {
        const file = await kvGetJson(env, `file:${token}`);
        if (file) {
          file.cost_points = pts; await kvPutJson(env, `file:${token}`, file);
    await tgApi('sendMessage', { chat_id: chatId, text: `هزینه فایل روی ${pts} الماس تنظیم شد.` });
          await setSession(env, uid, {});
          const { text: mfText, reply_markup } = await buildMyFilesKeyboard(env, uid, 0);
          await tgApi('sendMessage', { chat_id: chatId, text: mfText, reply_markup });
          return;
        }
      }
      await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار نامعتبر است. یک عدد غیرمنفی ارسال کنید.' });
      return;
    }
    if (session.awaiting === 'get_by_token' && text) {
      const token = text.trim();
      await setSession(env, uid, {});
      if (!isValidTokenFormat(token)) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'توکن نامعتبر است.' });
        return;
      }
      const ok = await checkRateLimit(env, uid, 'get_by_token', 5, 60_000);
      if (!ok) { await tgApi('sendMessage', { chat_id: chatId, text: 'تعداد درخواست بیش از حد. لطفاً بعداً تلاش کنید.' }); return; }
      await handleBotDownload(env, uid, chatId, token, '');
      return;
    }
    if (session.awaiting === 'redeem_gift' && text) {
      await setSession(env, uid, {});
      const code = text.trim();
      const res = await redeemGiftCode(env, uid, code);
      await tgApi('sendMessage', { chat_id: chatId, text: res.message });
      return;
    }
    if (session.awaiting?.startsWith('admin_create_gift:') && isAdmin(uid) && text) {
      const [, field, base] = session.awaiting.split(':');
      const draft = base ? JSON.parse(atob(base)) : {};
      if (field === 'code') draft.code = text.trim();
      if (field === 'amount') draft.amount = Math.max(0, parseInt(text.trim(), 10) || 0);
      if (field === 'max') draft.max_uses = Math.max(1, parseInt(text.trim(), 10) || 1);
      const nextField = field === 'code' ? 'amount' : field === 'amount' ? 'max' : null;
      if (nextField) {
        await setSession(env, uid, { awaiting: `admin_create_gift:${nextField}:${btoa(JSON.stringify(draft))}` });
        const prompt = nextField === 'amount' ? 'مبلغ الماس را وارد کنید:' : 'حداکثر تعداد استفاده کل را وارد کنید:';
        await tgApi('sendMessage', { chat_id: chatId, text: prompt });
      } else {
        const create = await createGiftCode(env, draft);
        if (create.ok) { await addGiftToIndex(env, draft.code); }
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: create.ok ? `کد هدیه ایجاد شد: ${draft.code}` : `خطا: ${create.error||'نامشخص'}` });
      }
      return;
    }
    if (session.awaiting?.startsWith('admin_reply:')) {
      if (!isAdmin(uid)) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'فقط ادمین‌ها مجاز هستند.' }); return; }
      const target = Number(session.awaiting.split(':')[1]);
      if (!text) { await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً پاسخ را به صورت متن ارسال کنید.' }); return; }
      let sent = false;
      try { await tgApi('sendMessage', { chat_id: target, text: `✉️ پاسخ پشتیبانی:\n${text}` }); sent = true; } catch (_) { sent = false; }
      if (!sent) { await tgApi('sendMessage', { chat_id: chatId, text: '❌ ارسال پیام به کاربر ناموفق بود.' }); return; }
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پیام شما به کاربر ارسال شد.' });
      return;
    }
    if (session.awaiting?.startsWith('rename:') && text) {
      const token = session.awaiting.split(':')[1];
      const f = await kvGetJson(env, `file:${token}`);
      if (!f) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'فایل یافت نشد.' }); return; }
      // Only owner or admin can rename
      if (!isAdmin(uid) && String(f.owner) !== String(uid)) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'اجازه تغییر نام ندارید.' }); return; }
      f.name = text.trim().slice(0, 120);
      await kvPutJson(env, `file:${token}`, f);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ نام فایل به‌روزرسانی شد.' });
      return;
    }
    if (session.awaiting === 'givepoints_uid' && text && isAdmin(uid)) {
      const tid = Number(text.trim());
      if (!Number.isFinite(tid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی نامعتبر است.' }); return; }
      await setSession(env, uid, { awaiting: `givepoints_amount:${tid}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'مبلغ الماس را وارد کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (session.awaiting?.startsWith('givepoints_amount:') && text && isAdmin(uid)) {
      const tid = Number(session.awaiting.split(':')[1]);
      const amount = Number(text.trim());
      if (!Number.isFinite(amount)) { await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار نامعتبر است.' }); return; }
      const tKey = `user:${tid}`;
      const target = (await kvGetJson(env, tKey)) || { id: tid, diamonds: 0 };
      target.diamonds = (target.diamonds || 0) + amount;
      await kvPutJson(env, tKey, target);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: `✅ ${amount} الماس به کاربر ${tid} اضافه شد. موجودی جدید: ${target.diamonds}` });
      try { await tgApi('sendMessage', { chat_id: tid, text: `🎯 ${amount} الماس به حساب شما اضافه شد.` }); } catch (_) {}
      return;
    }
    // Settings flows
    if (session.awaiting === 'set_welcome' && isAdmin(uid) && text) {
      const s = await getSettings(env);
      s.welcome_message = text;
      await setSettings(env, s);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پیام خوش‌آمد به‌روزرسانی شد.' });
      return;
    }
    if (session.awaiting === 'set_daily_limit' && isAdmin(uid) && text) {
      const n = Number(text.trim());
      if (!Number.isFinite(n) || n < 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
      const s = await getSettings(env);
      s.daily_limit = n;
      await setSettings(env, s);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ محدودیت روزانه تنظیم شد.' });
      return;
    }
    if (session.awaiting === 'set_buttons' && isAdmin(uid) && text) {
      try {
        const obj = JSON.parse(text);
        const s = await getSettings(env);
        s.button_labels = obj || {};
        await setSettings(env, s);
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: '✅ برچسب دکمه‌ها به‌روزرسانی شد.' });
      } catch (_) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'JSON نامعتبر.' });
      }
      return;
    }

    // Missions create flow (title → reward → period)
    if (session.awaiting === 'mission_create:title' && isAdmin(uid) && text) {
      const draft = { title: text.trim() };
      await setSession(env, uid, { awaiting: `mission_create:reward:${btoa(JSON.stringify(draft))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار الماس جایزه را ارسال کنید (عدد):' });
      return;
    }
    if (session.awaiting?.startsWith('mission_create:reward:') && isAdmin(uid) && text) {
      const base = JSON.parse(atob(session.awaiting.split(':')[2]));
      const reward = Number(text.trim());
      if (!Number.isFinite(reward) || reward <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
      base.reward = reward;
      await setSession(env, uid, { awaiting: `mission_create:period:${btoa(JSON.stringify(base))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'دوره را مشخص کنید: one|daily|weekly' });
      return;
    }
    if (session.awaiting?.startsWith('mission_create:period:') && isAdmin(uid) && text) {
      const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
      const valid = ['one','daily','weekly'];
      const p = text.trim().toLowerCase();
      if (!valid.includes(p)) { await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار نامعتبر. one|daily|weekly' }); return; }
    draft.period = p === 'one' ? 'once' : p;
    const created = await createMission(env, draft);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: created.ok ? `✅ ماموریت ایجاد شد (id=${created.id})` : `خطا در ایجاد ماموریت` });
      return;
    }
    if (session.awaiting === 'mission_edit:id' && isAdmin(uid) && text) {
      const id = text.trim();
      const m = await kvGetJson(env, `mission:${id}`);
      if (!m) { await tgApi('sendMessage', { chat_id: chatId, text: 'شناسه نامعتبر.' }); return; }
      await setSession(env, uid, { awaiting: `mission_edit:field:${btoa(JSON.stringify({ id }))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'کدام فیلد را می‌خواهید ویرایش کنید؟ title|reward|period', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (session.awaiting?.startsWith('mission_edit:field:') && isAdmin(uid) && text) {
      const base = JSON.parse(atob(session.awaiting.split(':')[2]));
      const field = text.trim().toLowerCase();
      if (!['title','reward','period'].includes(field)) { await tgApi('sendMessage', { chat_id: chatId, text: 'فیلد نامعتبر.' }); return; }
      await setSession(env, uid, { awaiting: `mission_edit:value:${field}:${btoa(JSON.stringify(base))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: `مقدار جدید برای ${field} را ارسال کنید:` });
      return;
    }
    if (session.awaiting?.startsWith('mission_edit:value:') && isAdmin(uid) && text) {
      const parts = session.awaiting.split(':');
      const field = parts[2];
      const base = JSON.parse(atob(parts[3]));
      const key = `mission:${base.id}`;
      const m = await kvGetJson(env, key);
      if (!m) { await setSession(env, uid, {}); await tgApi('sendMessage', { chat_id: chatId, text: 'شناسه نامعتبر.' }); return; }
      if (field === 'title') m.title = text.trim();
      if (field === 'reward') m.reward = Math.max(0, Number(text.trim()) || 0);
      if (field === 'period') {
        const pv = text.trim().toLowerCase();
        if (!['once','daily','weekly'].includes(pv)) { await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار period نامعتبر است (once|daily|weekly).' }); return; }
        m.period = pv;
      }
      await kvPutJson(env, key, m);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ ماموریت به‌روزرسانی شد.' });
      return;
    }
  // Quiz mission creation flow
  if (session.awaiting?.startsWith('mission_quiz:q:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    draft.question = text.trim().slice(0, 300);
    await setSession(env, uid, { awaiting: `mission_quiz:opts:${btoa(JSON.stringify(draft))}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'گزینه‌ها را هر کدام در یک خط ارسال کنید (حداقل 2 گزینه):' });
    return;
  }
  if (session.awaiting?.startsWith('mission_quiz:opts:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const options = String(text).split('\n').map(s => s.trim()).filter(Boolean).slice(0, 8);
    if (options.length < 2) { await tgApi('sendMessage', { chat_id: chatId, text: 'حداقل 2 گزینه لازم است.' }); return; }
    draft.options = options;
    await setSession(env, uid, { awaiting: `mission_quiz:correct:${btoa(JSON.stringify(draft))}` });
    const optsList = options.map((o, i) => `${i+1}) ${o}`).join('\n');
    await tgApi('sendMessage', { chat_id: chatId, text: `شماره گزینه صحیح را ارسال کنید (1 تا ${options.length}):\n\n${optsList}` });
    return;
  }
  if (session.awaiting?.startsWith('mission_quiz:correct:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const n = Number(String(text).trim());
    if (!Number.isFinite(n) || n < 1 || n > (draft.options?.length || 0)) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
    draft.correctIndex = n - 1;
    await setSession(env, uid, { awaiting: `mission_quiz:reward:${btoa(JSON.stringify(draft))}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'مقدار جایزه (الماس) را ارسال کنید:' });
    return;
  }
  if (session.awaiting?.startsWith('mission_quiz:reward:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const reward = Number(text.trim());
    if (!Number.isFinite(reward) || reward <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
    draft.reward = reward;
    draft.period = 'weekly';
    const created = await createMission(env, { title: `کوییز: ${draft.question.slice(0, 20)}...`, reward: draft.reward, period: 'weekly', type: 'quiz', config: { question: draft.question, options: draft.options, correctIndex: draft.correctIndex } });
    await setSession(env, uid, {});
    await tgApi('sendMessage', { chat_id: chatId, text: created.ok ? `✅ کوییز ایجاد شد (id=${created.id})` : 'خطا در ایجاد کوییز' });
    return;
  }
  // Weekly question/contest creation
  if (session.awaiting?.startsWith('mission_q:question:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    draft.question = text.trim().slice(0, 400);
    await setSession(env, uid, { awaiting: `mission_q:answer:${btoa(JSON.stringify(draft))}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'پاسخ صحیح مسابقه را ارسال کنید:' });
    return;
  }
  if (session.awaiting?.startsWith('mission_q:answer:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    draft.answer = text.trim().slice(0, 200);
    await setSession(env, uid, { awaiting: `mission_q:reward:${btoa(JSON.stringify(draft))}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'جایزه (الماس) را ارسال کنید:' });
    return;
  }
  if (session.awaiting?.startsWith('mission_q:reward:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const reward = Number(text.trim());
    if (!Number.isFinite(reward) || reward <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
    draft.reward = reward;
    const created = await createMission(env, { title: `سوال هفتگی`, reward: draft.reward, period: 'weekly', type: 'question', config: { question: draft.question, answer: draft.answer } });
    await setSession(env, uid, {});
    await tgApi('sendMessage', { chat_id: chatId, text: created.ok ? `✅ سوال هفتگی ایجاد شد (id=${created.id})` : 'خطا در ایجاد سوال' });
    return;
  }
  // Invite mission creation
  if (session.awaiting?.startsWith('mission_inv:count:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const needed = Number(text.trim());
    if (!Number.isFinite(needed) || needed <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
    draft.needed = needed;
    await setSession(env, uid, { awaiting: `mission_inv:reward:${btoa(JSON.stringify(draft))}` });
    await tgApi('sendMessage', { chat_id: chatId, text: 'جایزه (الماس) را ارسال کنید:' });
    return;
  }
  if (session.awaiting?.startsWith('mission_inv:reward:') && isAdmin(uid) && text) {
    const draft = JSON.parse(atob(session.awaiting.split(':')[2]));
    const reward = Number(text.trim());
    if (!Number.isFinite(reward) || reward <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد نامعتبر.' }); return; }
    draft.reward = reward;
    const created = await createMission(env, { title: `دعوت ${draft.needed} نفر در هفته`, reward: draft.reward, period: 'weekly', type: 'invite', config: { needed: draft.needed } });
    await setSession(env, uid, {});
    await tgApi('sendMessage', { chat_id: chatId, text: created.ok ? `✅ مأموریت دعوت ایجاد شد (id=${created.id})` : 'خطا در ایجاد مأموریت دعوت' });
    return;
  }

    // Lottery config (step-by-step)
    if (isAdmin(uid) && session.awaiting === 'lottery_cfg:winners' && text) {
      const winners = Math.floor(Number((text || '').trim()));
      if (!Number.isFinite(winners) || winners <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد برندگان نامعتبر است. یک عدد صحیح مثبت وارد کنید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      const base = { winners };
      await setSession(env, uid, { awaiting: `lottery_cfg:reward:${btoa(encodeURIComponent(JSON.stringify(base)))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'جایزه برای هر برنده (تعداد الماس) را وارد کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (isAdmin(uid) && session.awaiting?.startsWith('lottery_cfg:reward:') && text) {
      const base64 = session.awaiting.split(':')[2];
      const base = JSON.parse(decodeURIComponent(atob(base64)));
      const reward = Math.floor(Number((text || '').trim()));
      if (!Number.isFinite(reward) || reward <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد جایزه نامعتبر است. یک عدد صحیح مثبت وارد کنید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      base.reward_diamonds = reward;
      await setSession(env, uid, { awaiting: `lottery_cfg:hours:${btoa(encodeURIComponent(JSON.stringify(base)))}` });
      await tgApi('sendMessage', { chat_id: chatId, text: 'پس از چند ساعت قرعه‌کشی اجرا شود؟ (عدد صحیح، مثلا 24)', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
      return;
    }
    if (isAdmin(uid) && session.awaiting?.startsWith('lottery_cfg:hours:') && text) {
      const base64 = session.awaiting.split(':')[2];
      const base = JSON.parse(decodeURIComponent(atob(base64)));
      const hours = Math.floor(Number((text || '').trim()));
      if (!Number.isFinite(hours) || hours <= 0) { await tgApi('sendMessage', { chat_id: chatId, text: 'عدد ساعات نامعتبر است. یک عدد صحیح مثبت وارد کنید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } }); return; }
      const cfg = await getLotteryConfig(env);
      cfg.winners = Number(base.winners || cfg.winners || 0);
      cfg.reward_diamonds = Number(base.reward_diamonds || cfg.reward_diamonds || 0);
      cfg.run_every_hours = hours;
      cfg.next_run_at = now() + (hours * 60 * 60 * 1000);
      await setLotteryConfig(env, cfg);
      await setSession(env, uid, {});
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پیکربندی قرعه‌کشی ذخیره شد.' });
      return;
    }
    if (session.awaiting === 'bulk_meta' && isAdmin(uid) && text) {
      try {
        const arr = JSON.parse(text);
        if (!Array.isArray(arr)) throw new Error('bad array');
        let updated = 0;
        for (const item of arr) {
          const t = item && item.token;
          if (!t || !isValidTokenFormat(String(t))) continue;
          const f = await kvGetJson(env, `file:${t}`);
          if (!f) continue;
          if (typeof item.name === 'string' && item.name.trim()) f.name = item.name.trim();
          if (typeof item.category === 'string' && item.category.trim()) f.category = item.category.trim();
          await kvPutJson(env, `file:${t}`, f);
          updated++;
        }
        await setSession(env, uid, {});
        await tgApi('sendMessage', { chat_id: chatId, text: `✅ متادیتا اعمال شد. تعداد آیتم‌های به‌روزرسانی‌شده: ${updated}` });
      } catch (_) {
        await tgApi('sendMessage', { chat_id: chatId, text: 'JSON نامعتبر.' });
      }
      return;
    }
  }

  // Capture referral/pending intent from /start payload BEFORE enforcing join
  try {
    if (text.startsWith('/start')) {
      // Update mode: block regular users
      const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
      if (updateMode && !isAdmin(uid)) {
        await tgApi('sendMessage', { chat_id: chatId, text: '🔧 ربات در حال بروزرسانی است. لطفاً دقایقی دیگر مجدداً تلاش کنید.' });
        return;
      }
      const payload = text.replace('/start', '').trim();
      if (payload) {
        // Clone current session to avoid losing existing awaiting state
        const currentSession = { ...(session || {}) };
        if (payload.startsWith('d_')) {
          // deep link download: d_<token>_<ref?>
          const parts = payload.split('_');
          const token = parts[1] || '';
          const ref = parts[2] || '';
          currentSession.pending_download = { token, ref };
          // persist referrer on user profile if provided
          if (ref && !user.referred_by && Number(ref) !== Number(uid)) {
            user.referred_by = Number(ref);
            await kvPutJson(env, userKey, user);
          }
        } else {
          const refId = Number(payload);
          if (Number.isFinite(refId) && refId !== Number(uid)) {
            if (!user.referred_by) {
              user.referred_by = refId;
              await kvPutJson(env, userKey, user);
            }
            currentSession.pending_ref = refId;
          }
        }
        await setSession(env, uid, currentSession);
      }
    }
  } catch (_) {}

  // enforce join to required channels for non-admins (skip on /start to avoid delayed first response)
  const requireJoin = await getRequiredChannels(env);
  if (!text.startsWith('/start') && requireJoin.length && !isAdmin(uid)) {
    const joinedAll = await isUserJoinedAllRequiredChannels(env, uid);
    if (!joinedAll) {
      await presentJoinPrompt(env, chatId);
      return;
    }
  }

  // commands
  if (text.startsWith('/start')) {
    const payload = text.replace('/start', '').trim();
    if (payload) {
      if (payload.startsWith('d_')) {
        // deep link download: d_<token>_<ref?>
        const parts = payload.split('_');
        const token = parts[1] || '';
        const ref = parts[2] || '';
        await handleBotDownload(env, uid, chatId, token, ref);
        return;
      }
      // generic referral: payload is referrer id
      const refId = Number(payload);
      if (Number.isFinite(refId) && refId !== Number(uid)) {
        const refUser = (await kvGetJson(env, `user:${refId}`)) || null;
        if (refUser) {
          // persist referred_by if not already set
          user.referred_by = user.referred_by || refId;
          // credit only once per referred user
          if (!user.ref_credited) {
            refUser.diamonds = (refUser.diamonds || 0) + 1;
            refUser.referrals = (refUser.referrals || 0) + 1;
            await kvPutJson(env, `user:${refId}`, refUser);
            user.ref_credited = true;
            // track weekly referral for missions (credit to referrer)
            const wk = weekKey();
            const rk = `ref_week:${refId}:${wk}`;
            const rec = (await kvGetJson(env, rk)) || { count: 0 };
            rec.count = (rec.count || 0) + 1;
            await kvPutJson(env, rk, rec);
            // notify referrer
            await tgApi('sendMessage', { chat_id: refId, text: '🎉 یک الماس بابت معرفی کاربر جدید دریافت کردید.' });
          }
          await kvPutJson(env, userKey, user);
        }
      }
    }
    const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
    const settings = await getSettings(env);
    const welcomeText = settings.welcome_message && !updateMode
      ? settings.welcome_message
      : (updateMode && !isAdmin(uid)
        ? '🔧 ربات در حال بروزرسانی است. لطفاً دقایقی دیگر مجدداً تلاش کنید.'
        : `سلام ${from.first_name||''}! 🤖\nاز منو گزینه مورد نظر را انتخاب کنید.`);
    // send welcome immediately, then enforce join if needed
    await tgApi('sendMessage', { chat_id: chatId, text: welcomeText, reply_markup: await buildDynamicMainMenu(env, uid) });
    const requireJoin2 = await getRequiredChannels(env);
    if (requireJoin2.length && !isAdmin(uid)) {
      const joinedAll2 = await isUserJoinedAllRequiredChannels(env, uid);
      if (!joinedAll2) {
        await presentJoinPrompt(env, chatId);
      }
    }
    return;
  }

  // legacy /join kept for compatibility but routed to CHECK_JOIN
  if (text.startsWith('/join')) {
    const ok = await isUserJoinedAllRequiredChannels(env, uid);
    user.joined = ok; await kvPutJson(env, userKey, user);
    if (!ok) {
      await tgApi('sendMessage', { chat_id: chatId, text: '❌ هنوز عضو تمام کانال‌های الزامی نیستید.' });
      return;
    }
    // On success, credit referrer once and continue pending download
    const sess = await getSession(env, uid);
    const pendingRef = sess?.pending_download?.ref || sess?.pending_ref || user.referred_by;
    const refIdNum = Number(pendingRef);
    if (Number.isFinite(refIdNum) && refIdNum !== Number(uid) && !user.ref_credited) {
      const refUser = (await kvGetJson(env, `user:${refIdNum}`)) || null;
      if (refUser) {
        refUser.diamonds = (refUser.diamonds || 0) + 1;
        refUser.referrals = (refUser.referrals || 0) + 1;
        await kvPutJson(env, `user:${refIdNum}`, refUser);
        user.ref_credited = true;
        user.referred_by = user.referred_by || refIdNum;
        await kvPutJson(env, userKey, user);
        // track weekly referral for missions (credit to referrer)
        const wk = weekKey();
        const rk = `ref_week:${refIdNum}:${wk}`;
        const rec = (await kvGetJson(env, rk)) || { count: 0 };
        rec.count = (rec.count || 0) + 1;
        await kvPutJson(env, rk, rec);
        await tgApi('sendMessage', { chat_id: refIdNum, text: '🎉 یک الماس بابت معرفی کاربر جدید دریافت کردید.' });
      }
    }
    const pendingToken = sess?.pending_download?.token;
    const pendingDeepRef = sess?.pending_download?.ref || '';
    if (pendingToken) {
      const nextSession = { ...(sess || {}) };
      delete nextSession.pending_download;
      delete nextSession.pending_ref;
      await setSession(env, uid, nextSession);
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ عضویت شما تایید شد. ادامه عملیات...' });
      await handleBotDownload(env, uid, chatId, pendingToken, pendingDeepRef);
      return;
    }
    await tgApi('sendMessage', { chat_id: chatId, text: '✅ عضویت شما تایید شد.' });
    return;
  }

  if (text.startsWith('/profile')) {
    await tgApi('sendMessage', { 
      chat_id: chatId, 
      text: `📊 پروفایل شما:\n\n👤 آی‌دی: ${uid}\n🏷 یوزرنیم: ${user.username||'-'}\n💎 الماس: ${user.diamonds||0}\n📈 معرفی‌ها: ${user.referrals||0}\n📅 عضویت: ${formatDate(user.created_at||0)}` 
    });
    return;
  }

  // Admin command: give diamonds
  if (isAdmin(uid) && text.startsWith('/givediamonds')) {
    const parts = text.split(/\s+/);
    if (parts.length < 3) {
      await tgApi('sendMessage', { chat_id: chatId, text: 'استفاده: /givediamonds <uid> <amount>' });
      return;
    }
    const tid = Number(parts[1]);
    const amount = Number(parts[2]);
    if (!Number.isFinite(tid) || !Number.isFinite(amount)) {
      await tgApi('sendMessage', { chat_id: chatId, text: 'پارامتر نامعتبر است.' });
      return;
    }
    const tKey = `user:${tid}`;
  const target = (await kvGetJson(env, tKey)) || { id: tid, diamonds: 0 };
  target.diamonds = (target.diamonds || 0) + amount;
    await kvPutJson(env, tKey, target);
  await tgApi('sendMessage', { chat_id: chatId, text: `✅ ${amount} الماس به کاربر ${tid} اضافه شد. موجودی جدید: ${target.diamonds}` });
  try { await tgApi('sendMessage', { chat_id: tid, text: `🎯 ${amount} الماس به حساب شما اضافه شد.` }); } catch (_) {}
    return;
  }

  if (text.startsWith('/myfiles')) {
    const built = await buildMyFilesKeyboard(env, uid, 0);
    return tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
  }

  // command: missions view shortcut
  if (text.startsWith('/missions')) {
    const view = await buildMissionsView(env, uid);
    return tgApi('sendMessage', { chat_id: chatId, text: view.text, reply_markup: view.reply_markup });
  }

  // admin commands
  if (isAdmin(uid) && text.startsWith('/broadcast ')) {
    const message = text.replace('/broadcast ', '').trim();
    await broadcast(env, message);
    return tgApi('sendMessage', { chat_id: chatId, text: '📢 پیام به همه کاربران ارسال شد.' });
  }

  // set cost: /setcost <token> <diamonds>
  if (text.startsWith('/setcost')) {
    if (!isAdmin(uid)) return tgApi('sendMessage', { chat_id: chatId, text: 'این دستور فقط برای ادمین مجاز است.' });
    const parts = text.split(/\s+/);
  if (parts.length < 3) return tgApi('sendMessage', { chat_id: chatId, text: 'استفاده: /setcost <token> <diamonds>' });
    const token = parts[1]; const pts = parseInt(parts[2],10) || 0;
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) return tgApi('sendMessage', { chat_id: chatId, text: 'توکن پیدا نشد.' });
    file.cost_points = pts; await kvPutJson(env, `file:${token}`, file);
  return tgApi('sendMessage', { chat_id: chatId, text: `💰 هزینه فایل تنظیم شد: ${pts} الماس` });
  }

  // enable/disable a file: /disable <token> , /enable <token>
  if (isAdmin(uid) && text.startsWith('/disable ')) {
    const token = text.split(/\s+/)[1]; const file = await kvGetJson(env, `file:${token}`);
    if (!file) return tgApi('sendMessage', { chat_id: chatId, text: 'توکن پیدا نشد' });
    file.disabled = true; await kvPutJson(env, `file:${token}`, file);
    return tgApi('sendMessage', { chat_id: chatId, text: '🔴 فایل غیرفعال شد' });
  }
  if (isAdmin(uid) && text.startsWith('/enable ')) {
    const token = text.split(/\s+/)[1]; const file = await kvGetJson(env, `file:${token}`);
    if (!file) return tgApi('sendMessage', { chat_id: chatId, text: 'توکن پیدا نشد' });
    file.disabled = false; await kvPutJson(env, `file:${token}`, file);
    return tgApi('sendMessage', { chat_id: chatId, text: '🟢 فایل فعال شد' });
  }

  // document upload handled separately (legacy) -> now routed by upload flow
  if (msg.document && isAdmin(uid) && !session.awaiting) {
    const created = await handleAnyUpload(msg, env, { ownerId: uid });
    if (created) {
      const m = `✅ فایل آپلود شد:\nنام: ${created.name}\nحجم: ${formatFileSize(created.size||0)}\nتوکن: ${created.token}`;
      await tgApi('sendMessage', { chat_id: chatId, text: m, reply_markup: buildFileManageKeyboard(created.token, created, true) });
      return;
    }
  }

  // fallback -> show menu
  await sendMainMenu(env, chatId, uid);
}

async function onCallback(cb, env) {
  const data = cb.data; const chatId = cb.message.chat.id; const from = cb.from;
  const uid = from.id;
  // Ack immediately to stop Telegram UI spinner
  try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); } catch (_) {}
  // enforce block also for callbacks
  if (!isAdmin(uid)) {
    try {
      const blocked = await isUserBlocked(env, uid);
      if (blocked) {
        await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
        await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ دسترسی شما توسط مدیر محدود شده است.' });
        return;
      }
    } catch (_) {}
  }
  if (data === 'MENU') {
    await tgApi('sendMessage', { chat_id: chatId, text: 'منوی اصلی:', reply_markup: await buildDynamicMainMenu(env, uid) });
    return;
  }
  if (data === 'NOOP') {
    return;
  }
  if (data === 'CANCEL') {
    await setSession(env, uid, {});
    await tgApi('sendMessage', { chat_id: chatId, text: 'فرآیند لغو شد.', reply_markup: await buildDynamicMainMenu(env, uid) });
    return;
  }
  if (data === 'ADMIN:PANEL' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: '🛠 پنل مدیریت', reply_markup: buildAdminPanelKeyboard() });
    return;
  }
  if (data === 'ADMIN:BACKUP' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'در حال تهیه پشتیبان...' });
    try {
      const backup = await createKvBackup(env);
      const adminIds = await getAdminIds(env);
      const mainAdmin = adminIds && adminIds.length ? adminIds[0] : (MAIN_ADMIN_ID || uid);
      const content = JSON.stringify(backup, null, 2);
      await tgApi('sendMessage', { chat_id: mainAdmin, text: '📦 پشتیبان دیتابیس آماده شد. در حال ارسال...' });
      const filename = `backup_${new Date().toISOString().slice(0,10)}.json`;
      const form = new FormData();
      form.append('chat_id', String(mainAdmin));
      form.append('caption', filename);
      // Use Blob with filename for Telegram file upload
      form.append('document', new Blob([content], { type: 'application/json' }), filename);
      const res = await tgUpload('sendDocument', form);
      if (!res || !res.ok) {
        throw new Error('telegram_upload_failed');
      }
    } catch (e) {
      await tgApi('sendMessage', { chat_id: chatId, text: '❌ خطا در تهیه پشتیبان.' });
    }
    return;
  }
  if (data === 'ADMIN:PAYMENTS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const idx = (await kvGetJson(env, 'index:purchases')) || [];
    const first20 = idx.slice(0, 20);
    const entries = [];
    for (const pid of first20) {
      const p = await kvGetJson(env, `purchase:${pid}`);
      if (!p) continue;
      entries.push(p);
    }
    const lines = entries.map(p => `#${String(p.id).padStart(8,'0')} | ${p.user_id} | ${p.diamonds}💎 | ${(p.price_toman||0).toLocaleString('fa-IR')}ت | ${p.status}`);
    const text = lines.length ? `آخرین تراکنش‌ها (۲۰ مورد):
${lines.join('\n')}

برای مشاهده رسید یا تصمیم، از دکمه‌های زیر استفاده کنید.` : 'هیچ تراکنشی یافت نشد.';
    const kb = { inline_keyboard: [
      ...entries.map(p => ([
        { text: `🧾 ${String(p.id).padStart(8,'0')} (${p.status==='pending_review'?'در انتظار':'بررسی‌شده'})`, callback_data: `ADMIN:PAY:VIEW:${p.id}` },
      ])),
      [{ text: '⬅️ بازگشت به پنل', callback_data: 'ADMIN:PANEL' }]
    ] };
    await tgApi('sendMessage', { chat_id: chatId, text, reply_markup: kb });
    return;
  }
  if (data.startsWith('ADMIN:PAY:VIEW:') && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const id = data.split(':')[3];
    const p = await kvGetJson(env, `purchase:${id}`);
    if (!p) { await tgApi('sendMessage', { chat_id: chatId, text: 'سفارش یافت نشد.' }); return; }
    const hdr = `خرید #${String(p.id).padStart(8,'0')}
کاربر: ${p.user_id}
بسته: ${p.diamonds} الماس
مبلغ: ${(p.price_toman||0).toLocaleString('fa-IR')} تومان
وضعیت: ${p.status}`;
    const actions = [];
    if (p.status === 'pending_review') {
      actions.push(
        [{ text: '✅ تایید و افزودن الماس', callback_data: `PAYAPP:${p.id}` }, { text: '❌ رد', callback_data: `PAYREJ:${p.id}` }]
      );
    }
    actions.push([{ text: '⬅️ بازگشت', callback_data: 'ADMIN:PAYMENTS' }]);
    const kb = { inline_keyboard: actions };
    if (p.receipt_file_id) {
      try {
        await tgApi('sendPhoto', { chat_id: chatId, photo: p.receipt_file_id, caption: hdr, reply_markup: kb });
      } catch (_) {
        await tgApi('sendDocument', { chat_id: chatId, document: p.receipt_file_id, caption: hdr, reply_markup: kb });
      }
    } else {
      await tgApi('sendMessage', { chat_id: chatId, text: hdr, reply_markup: kb });
    }
    return;
  }
  if (data === 'HELP') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'راهنما' });
    const isAdminUser = isAdmin(uid);
    const userGuide = `📚 راهنمای استفاده\n\nمنوی اصلی:\n- 👤 پروفایل: نمایش آی‌دی، یوزرنیم، موجودی الماس، تعداد معرفی‌ها و تاریخ عضویت. در صورت فعال بودن مأموریت‌ها، پیشرفت شما نیز نمایش داده می‌شود.\n- دریافت لینک اختصاصی: لینک دعوت اختصاصی شما را می‌دهد. با هر کاربر فعال که از لینک شما وارد ربات شود، 1 الماس دریافت می‌کنید.\n- 🔑 دریافت با توکن: توکن فایل را ارسال کنید تا فایل داخل ربات برای شما ارسال شود. اگر فایل هزینه داشته باشد، پیش از ارسال از موجودی الماس شما کسر می‌شود. ممکن است سقف روزانه دریافت نیز فعال باشد.\n- 🎁 کد هدیه: کد گیفت را وارد کنید تا در صورت معتبر بودن، الماس به موجودی شما اضافه شود. (برخی کدها محدودیت تعداد استفاده دارند.)\n- 🆘 پشتیبانی: پیام یا تصویر خود را ارسال کنید تا برای مدیر ارسال شود؛ پاسخ از همین ربات به شما برمی‌گردد.\n- 💳 خرید الماس: بسته را انتخاب کنید، مبلغ را کارت‌به‌کارت کنید و تصویر رسید را بفرستید. پس از تأیید مدیر، الماس به حساب شما اضافه می‌شود.\n- 📆 مأموریت‌ها: فهرست مأموریت‌های فعال + دکمه «دریافت پاداش هفتگی (هر ۷ روز)» برای دریافت الماس رایگان.\n- 🎟 قرعه‌کشی: در صورت فعال بودن، می‌توانید وضعیت را ببینید و روزانه ثبت‌نام کنید.\n\nدریافت و اشتراک‌گذاری فایل:\n- اگر عضویت در کانال‌های اجباری فعال باشد، ابتدا باید عضو شوید و سپس «بررسی عضویت» را بزنید.\n- برای اشتراک‌گذاری فایل‌ها، از دکمه «لینک» استفاده کنید تا لینک ورود مستقیم به ربات ساخته شود.\n\nمدیریت فایل‌های شخصی:\n- با دستور /myfiles، فهرست فایل‌های خود را می‌بینید.\n- برای هر فایل: «دریافت»، «کپی لینک» و «تغییر نام» (برای مالک) در دسترس است. اگر فایل هزینه داشته باشد، پیش از دریافت الماس کسر می‌شود.`;
    const adminGuide = `📚 راهنمای مدیر\n\nمنوی اصلی کاربر:\n- همان گزینه‌های کاربری نمایش داده می‌شود؛ شما علاوه‌بر آن به «پنل مدیریت» دسترسی دارید.\n\nپنل مدیریت (دکمه‌های اصلی):\n- 📊 آمار: نمایش تعداد کاربران، فایل‌ها، دانلودها و شاخص‌های کلیدی.\n- 🛑 تغییر وضعیت سرویس: خاموش/روشن کردن پاسخ‌دهی ربات.\n- 🛠 حالت آپدیت: محدودسازی ربات به ادمین‌ها برای بروزرسانی/نگه‌داری موقت.\n- 📢 ارسال اعلان: ارسال پیام همگانی به همه کاربران (با رعایت محدودیت‌های تلگرام).\n- ⚙️ تنظیمات سرویس: ویرایش پیام خوش‌آمد، سقف روزانه دریافت، و عناوین دکمه‌های منوی اصلی.\n- 📂 مدیریت فایل‌ها: فهرست فایل‌های شما + جزئیات هر آیتم:\n  • 📥 دریافت: ارسال فایل در چت.\n  • 🔗 لینک: ساخت لینک اشتراک‌گذاری داخل ربات.\n  • 💰 هزینه: تعیین/تغییر تعداد الماس موردنیاز (0 = رایگان).\n  • 🔴/🟢 غیرفعال/فعال: قطع/وصل دسترسی به فایل.\n  • 🗑 حذف: حذف کامل آیتم.\n  • ♻️ جایگزینی محتوا: آپلود محتوای جدید روی همان توکن.\n- 📤 آپلود فایل: ارسال متن/سند/عکس/ویدیو/صدا/ویس و ساخت توکن و لینک اختصاصی.\n- 📤 آپلود گروهی: چندین آیتم را پشت‌سرهم بفرستید؛ سپس با «تنظیم نام/دسته» متادیتا را گروهی اعمال کرده و در پایان «پایان» را بزنید.\n- 📣 کانال‌های اجباری: افزودن/حذف کانال‌هایی که عضویت آنها برای استفاده از ربات لازم است.\n- 👑 مدیریت ادمین‌ها: افزودن/حذف ادمین‌ها (بر اساس آی‌دی عددی).\n- 🎟 مدیریت گیفت‌کد: ایجاد، فعال/غیرفعال و حذف گیفت‌کد؛ همراه با شمارش تعداد استفاده.\n- 🎯 افزودن الماس: شارژ دستی موجودی الماس یک کاربر با وارد کردن آی‌دی و مقدار.\n- 📆 مأموریت‌ها: ایجاد/حذف مأموریت با تعیین جایزه (الماس) و دوره (یک‌بار، روزانه، هفتگی).\n- 🎟 قرعه‌کشی: فعال/غیرفعال، تنظیم تعداد برندگان و جایزه، مشاهده تاریخچه؛ کاربران می‌توانند روزانه ثبت‌نام کنند.\n\nنکات عملی:\n- برای دقت آمار دانلود و ارجاع، لینک داخل ربات را برای کاربران ارسال کنید.\n- اگر هزینه فایل 0 باشد، دریافت آن رایگان است.\n- بسته‌های الماس قابل تغییر در تنظیمات/کد هستند و بررسی پرداخت به‌صورت دستی توسط ادمین انجام می‌شود.`;
    await tgApi('sendMessage', { chat_id: chatId, text: isAdminUser ? adminGuide : userGuide });
    return;
  }
  if (data === 'SUPPORT') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    // Show structured support options instead of free-form prompt
    const kb = { inline_keyboard: [
      [{ text: '🧾 ثبت تیکت جدید', callback_data: 'TICKET:NEW' }],
      [{ text: '✉️ ارسال پیام به پشتیبانی', callback_data: 'SUPPORT:MSG' }],
      [{ text: '📨 تیکت‌های من', callback_data: 'TICKET:MY' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] };
    await setSession(env, uid, {});
    await tgApi('sendMessage', { chat_id: chatId, text: '🆘 پشتیبانی — لطفاً یکی از گزینه‌ها را انتخاب کنید:', reply_markup: kb });
    return;
  }
  if (data === 'SUPPORT:MSG') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await setSession(env, uid, { awaiting: 'support_wait' });
    await tgApi('sendMessage', { chat_id: chatId, text: 'پیام خود را برای پشتیبانی ارسال کنید. می‌توانید متن، عکس یا فایل ارسال کنید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data.startsWith('SUPREPLY:') && isAdmin(uid)) {
    const target = Number(data.split(':')[1]);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await setSession(env, uid, { awaiting: `admin_reply:${target}` });
    await tgApi('sendMessage', { chat_id: chatId, text: `لطفاً پاسخ خود به کاربر ${target} را ارسال کنید.` });
    return;
  }
  if (data === 'BUY_DIAMONDS') {
    if (await isButtonDisabled(env, 'BUY_DIAMONDS')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const info = `💳 خرید الماس
یک بسته را انتخاب کنید:`;
    const rows = DIAMOND_PACKAGES.map(p => ([{ text: `${p.diamonds} الماس — ${p.price_toman.toLocaleString('fa-IR')} تومان`, callback_data: `DPKG:${p.id}` }]));
    rows.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
    await tgApi('sendMessage', { chat_id: chatId, text: info, reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data.startsWith('DPKG:')) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const pkgId = data.split(':')[1];
    const pkg = getDiamondPackageById(pkgId);
    const id = await generatePurchaseId(env);
    const purchase = { id, user_id: uid, diamonds: pkg.diamonds, price_toman: pkg.price_toman, pkg_id: pkg.id, status: 'awaiting_receipt', created_at: now() };
    await kvPutJson(env, `purchase:${id}`, purchase);
    // update purchases index (prepend newest)
    try {
      const idxKey = 'index:purchases';
      const idx = (await kvGetJson(env, idxKey)) || [];
      idx.unshift(id);
      // keep at most 1000 entries
      if (idx.length > 1000) idx.length = 1000;
      await kvPutJson(env, idxKey, idx);
    } catch (_) {}
    const txt = `✅ بسته انتخاب شد: ${pkg.diamonds} الماس (${pkg.price_toman.toLocaleString('fa-IR')} تومان)
شناسه خرید شما: \`${id}\`
لطفاً مبلغ را به کارت زیر واریز کنید و سپس روی «پرداخت کردم» بزنید:

کارت:
\`${BANK_CARD_NUMBER}\`
نام: **${BANK_CARD_NAME}**`;
    await tgApi('sendMessage', { chat_id: chatId, text: txt, parse_mode: 'Markdown', reply_markup: { inline_keyboard: [
      [{ text: '✅ پرداخت کردم', callback_data: `PAID_CONFIRM:${id}` }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] } });
    return;
  }
  if (data.startsWith('PAID_CONFIRM:')) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const purchaseId = data.split(':')[1];
    const pKey = `purchase:${purchaseId}`;
    const purchase = await kvGetJson(env, pKey);
    if (!purchase || purchase.user_id !== uid || purchase.status !== 'awaiting_receipt') {
      await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ درخواست خرید نامعتبر یا منقضی است.' });
      return;
    }
    await setSession(env, uid, { awaiting: `payment_receipt:${purchaseId}` });
    await tgApi('sendMessage', { chat_id: chatId, text: `شناسه خرید شما: \`${purchaseId}\`\nلطفاً عکس رسید پرداخت را ارسال کنید.`, parse_mode: 'Markdown' });
    return;
  }
  if (data === 'PAID_CONFIRM') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    // legacy path disabled; now user must select a package first
    await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً ابتدا یک بسته را انتخاب کنید.' });
    return;
  }
  if (data === 'UPLOAD_HELP') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'راهنمای آپلود' });
    await tgApi('sendMessage', { chat_id: chatId, text: 'برای آپلود، می‌توانید متن، سند، عکس، ویدیو، صدا یا ویس ارسال کنید. پس از آپلود، لینک اختصاصی دریافت می‌کنید.' });
    return;
  }
  if (data === 'ADMIN:UPLOAD' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'upload_wait' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'لطفاً متن یا یکی از انواع رسانه (document/photo/video/audio/voice) را ارسال کنید.' });
    return;
  }
  if (data === 'CHECK_JOIN') {
    const ok = await isUserJoinedAllRequiredChannels(env, uid);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    if (!ok) {
      await tgApi('sendMessage', { chat_id: chatId, text: '❌ هنوز عضو تمام کانال‌های الزامی نیستید.' });
      return;
    }
    // mark user joined
    const uKey = `user:${uid}`;
    const uMeta = (await kvGetJson(env, uKey)) || { id: uid };
    uMeta.joined = true;
    await kvPutJson(env, uKey, uMeta);

    // credit referrer (once) if pending or recorded
    const s = await getSession(env, uid);
    const pendingRef = s?.pending_download?.ref || s?.pending_ref || uMeta.referred_by;
    const refIdNum = Number(pendingRef);
    if (Number.isFinite(refIdNum) && refIdNum !== Number(uid) && !uMeta.ref_credited) {
      const refUser = (await kvGetJson(env, `user:${refIdNum}`)) || null;
      if (refUser) {
          refUser.diamonds = (refUser.diamonds || 0) + 1;
          refUser.referrals = (refUser.referrals || 0) + 1;
        await kvPutJson(env, `user:${refIdNum}`, refUser);
        uMeta.ref_credited = true;
        uMeta.referred_by = uMeta.referred_by || refIdNum;
        await kvPutJson(env, uKey, uMeta);
        // track weekly referral for missions (credit to referrer)
        const wk = weekKey();
        const rk = `ref_week:${refIdNum}:${wk}`;
        const rec = (await kvGetJson(env, rk)) || { count: 0 };
        rec.count = (rec.count || 0) + 1;
        await kvPutJson(env, rk, rec);
        await tgApi('sendMessage', { chat_id: refIdNum, text: '🎉 یک الماس بابت معرفی کاربر جدید دریافت کردید.' });
      }
    }

    // continue any pending download automatically
    const pendingToken = s?.pending_download?.token;
    const pendingDeepRef = s?.pending_download?.ref || '';
    if (pendingToken) {
      // clear pending markers but keep other session state
      const nextSession = { ...(s || {}) };
      delete nextSession.pending_download;
      delete nextSession.pending_ref;
      await setSession(env, uid, nextSession);
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ عضویت شما تایید شد. ادامه عملیات...' });
      await handleBotDownload(env, uid, chatId, pendingToken, pendingDeepRef);
      return;
    }

    await tgApi('sendMessage', { chat_id: chatId, text: '✅ عضویت شما تایید شد.' });
    return;
  }
  if (data === 'PROFILE') {
    const user = (await kvGetJson(env, `user:${uid}`)) || {};
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    if (isAdmin(uid)) {
      const users = (await kvGetJson(env, 'index:users')) || [];
      const userCount = users.length;
      let totalDownloads = 0; let fileCount = 0;
      for (const u of users.slice(0, 100)) {
        const list = (await kvGetJson(env, `uploader:${u}`)) || [];
        fileCount += list.length;
        for (const t of list) {
          const f = await kvGetJson(env, `file:${t}`);
          if (f && f.downloads) totalDownloads += f.downloads;
        }
      }
      await tgApi('sendMessage', { chat_id: chatId, text: `📊 پروفایل شما:\n\n👤 آی‌دی: ${uid}\n🏷 یوزرنیم: ${user.username||'-'}\n💎 الماس: ${user.diamonds||0}\n📈 معرفی‌ها: ${user.referrals||0}\n📅 عضویت: ${formatDate(user.created_at||0)}\n\n📈 آمار ربات:\n👥 کاربران: ${userCount}\n📁 فایل‌ها: ${fileCount}\n📥 دانلودها: ${totalDownloads}` });
    } else {
      // Show missions progress if available and present support/ticket actions
      const progress = await getUserMissionProgress(env, uid);
      const missionsActive = (await kvGetJson(env, 'missions:index')) || [];
      const mText = missionsActive.length ? `\n\n📆 ماموریت‌های فعال: ${missionsActive.length}\n✅ پیشرفت شما: ${progress.completed||0}/${missionsActive.length}` : '';
      const text = `📊 پروفایل شما:\n\n👤 آی‌دی: ${uid}\n🏷 یوزرنیم: ${user.username||'-'}\n💎 الماس: ${user.diamonds||0}\n📈 معرفی‌ها: ${user.referrals||0}\n📅 عضویت: ${formatDate(user.created_at||0)}${mText}`;
      const reply_markup = { inline_keyboard: [
        [{ text: '🧾 ثبت تیکت جدید', callback_data: 'TICKET:NEW' }],
        [{ text: '📨 تیکت‌های من', callback_data: 'TICKET:MY' }],
        [{ text: '💸 انتقال موجودی', callback_data: 'BAL:START' }],
        [{ text: '🆘 پشتیبانی', callback_data: 'SUPPORT' }],
        [{ text: '🏠 منو', callback_data: 'MENU' }]
      ] };
      await tgApi('sendMessage', { chat_id: chatId, text, reply_markup });
    }
    return;
  }
  if (data === 'GET_BY_TOKEN') {
    if (await isButtonDisabled(env, 'GET_BY_TOKEN')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await setSession(env, uid, { awaiting: 'get_by_token' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'توکن فایل را ارسال کنید:' });
    return;
  }
  if (data === 'SUB:REFERRAL') {
    if (await isButtonDisabled(env, 'SUB:REFERRAL')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const rows = [
      [{ text: 'دریافت لینک اختصاصی', callback_data: 'REFERRAL' }],
      [{ text: '🎁 کد هدیه', callback_data: 'REDEEM_GIFT' }],
      [{ text: '⬅️ بازگشت', callback_data: 'MENU' }]
    ];
    await tgApi('sendMessage', { chat_id: chatId, text: '👥 زیرمجموعه گیری:', reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data === 'SUB:ACCOUNT') {
    if (await isButtonDisabled(env, 'SUB:ACCOUNT')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    // Show profile directly without extra buttons
    const user = (await kvGetJson(env, `user:${uid}`)) || {};
    const progress = await getUserMissionProgress(env, uid);
    const missionsActive = (await kvGetJson(env, 'missions:index')) || [];
    const mText = missionsActive.length ? `\n\n📆 ماموریت‌های فعال: ${missionsActive.length}\n✅ پیشرفت شما: ${progress.completed||0}/${missionsActive.length}` : '';
    const text = `📊 پروفایل شما:\n\n👤 آی‌دی: ${uid}\n🏷 یوزرنیم: ${user.username||'-'}\n💎 الماس: ${user.diamonds||0}\n📈 معرفی‌ها: ${user.referrals||0}\n📅 عضویت: ${formatDate(user.created_at||0)}${mText}`;
    const reply_markup = { inline_keyboard: [
      [
        { text: '🧾 ثبت تیکت جدید', callback_data: 'TICKET:NEW' },
        { text: '📨 تیکت‌های من', callback_data: 'TICKET:MY' }
      ],
      [{ text: '💸 انتقال موجودی', callback_data: 'BAL:START' }],
      [{ text: '🆘 پشتیبانی', callback_data: 'SUPPORT' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] };
    await tgApi('sendMessage', { chat_id: chatId, text, reply_markup });
    return;
  }
  
  // ===== Balance transfer flow (callbacks)
  if (data === 'BAL:START') {
    await setSession(env, uid, { awaiting: 'bal:to' });
    try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); } catch (_) {}
    await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی عددی گیرنده را وارد کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data.startsWith('BAL:CONFIRM:')) {
    const [, , toIdStr, amountStr] = data.split(':');
    const toId = Number(toIdStr);
    const amount = Math.floor(Number(amountStr));
    if (!Number.isFinite(toId) || !Number.isFinite(amount)) { try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); } catch (_) {} return; }
    if (amount < 2 || amount > 50) { try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'بازه انتقال 2 تا 50 الماس است' }); } catch (_) {} return; }
    const fromKey = `user:${uid}`;
    const toKey = `user:${toId}`;
    const fromUser = (await kvGetJson(env, fromKey)) || { id: uid, diamonds: 0 };
    const toUser = (await kvGetJson(env, toKey)) || { id: toId, diamonds: 0 };
    if (toId === uid) { try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); } catch (_) {} return; }
    if ((fromUser.diamonds || 0) < amount) { try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'الماس کافی نیست' }); } catch (_) {} return; }
    // ensure destination exists in index; if not, create entry lazily
    const usersIndex = (await kvGetJson(env, 'index:users')) || [];
    if (!usersIndex.includes(toId)) {
      usersIndex.push(toId);
      await kvPutJson(env, 'index:users', usersIndex);
      const existing = await kvGetJson(env, toKey);
      if (!existing) { await kvPutJson(env, toKey, toUser); }
    }
    fromUser.diamonds = (fromUser.diamonds || 0) - amount;
    toUser.diamonds = (toUser.diamonds || 0) + amount;
    await kvPutJson(env, fromKey, fromUser);
    await kvPutJson(env, toKey, toUser);
    try { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'انجام شد' }); } catch (_) {}
    await tgApi('sendMessage', { chat_id: chatId, text: `✅ انتقال انجام شد. ${amount} الماس به کاربر ${toId} منتقل شد.` });
    try { await tgApi('sendMessage', { chat_id: toId, text: `💸 ${amount} الماس از سوی کاربر ${uid} به حساب شما واریز شد.` }); } catch(_) {}
    return;
  }
  // ===== Tickets: User actions
  if (data === 'TICKET:NEW') {
    await setSession(env, uid, { awaiting: 'ticket:new:category' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const categories = ['عمومی', 'پرداخت', 'فنی'];
    await tgApi('sendMessage', { chat_id: chatId, text: 'دسته تیکت را انتخاب یا تایپ کنید:', reply_markup: { inline_keyboard: [
      ...categories.map(c => ([{ text: c, callback_data: `TKT:CAT:${encodeURIComponent(c)}` }])),
      [{ text: '❌ انصراف', callback_data: 'CANCEL' }]
    ] } });
    return;
  }
  if (data.startsWith('TKT:CAT:')) {
    const cat = decodeURIComponent(data.split(':')[2]);
    await setSession(env, uid, { awaiting: `ticket:new:desc:${btoa(encodeURIComponent(JSON.stringify({ category: cat })) )}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `دسته انتخاب شد: ${cat}\nاکنون شرح تیکت را وارد کنید:` });
    return;
  }
  if (data.startsWith('TKT:SUBMIT:')) {
    const payload = data.split(':')[2];
    let obj = null;
    try { obj = JSON.parse(decodeURIComponent(atob(payload))); } catch (_) {}
    if (!obj || !obj.category || !obj.desc) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); return; }
    const userKey = `user:${uid}`;
    const u = (await kvGetJson(env, userKey)) || { id: uid };
    const created = await createTicket(env, {
      user_id: uid,
      username: u.username || null,
      category: obj.category,
      subject: '-',
      desc: obj.desc
    });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `✅ تیکت شما ثبت شد. شناسه: #${created.id}` });
    try {
      const admins = await getAdminIds(env);
      const notice = `🧾 تیکت جدید #${created.id} از ${uid}${u.username ? ` (@${u.username})` : ''}\nدسته: ${created.category}`;
      for (const aid of admins) { try { await tgApi('sendMessage', { chat_id: aid, text: notice }); } catch (_) {} }
    } catch (_) {}
    return;
  }
  if (data === 'TICKET:MY') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const list = await listUserTickets(env, uid, { limit: 10 });
    const lines = list.length ? list.map(t => `#${t.id} | ${t.status || 'open'} | ${escapeHtml(t.subject || '-')}`).join('\n') : '—';
    const rows = [
      ...list.map(t => ([{ text: `#${t.id}`, callback_data: `TKT:VIEW:${t.id}` }])),
      [{ text: '🧾 ثبت تیکت جدید', callback_data: 'TICKET:NEW' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ];
    await tgApi('sendMessage', { chat_id: chatId, text: `📨 تیکت‌های من\n\n${lines}` , reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data.startsWith('TKT:VIEW:')) {
    const id = data.split(':')[2];
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const t = await getTicket(env, id);
    if (!t || String(t.user_id) !== String(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: 'تیکت یافت نشد.' }); return; }
    const msgs = await getTicketMessages(env, id, 20);
    const history = msgs.map(m => `${m.from === 'admin' ? 'ادمین' : 'شما'} (${formatDate(m.at)}):\n${m.text}`).join('\n\n') || '—';
    const kb = { inline_keyboard: [
      ...(t.status !== 'closed' ? [[{ text: '✍️ ارسال پیام در این تیکت', callback_data: `TKT:REPLY:${t.id}` }]] : []),
      [{ text: '⬅️ بازگشت', callback_data: 'TICKET:MY' }]
    ] };
    await tgApi('sendMessage', { chat_id: chatId, text: `#${t.id} | ${t.status || 'open'}\nموضوع: ${t.subject || '-'}\nدسته: ${t.category || '-'}\n\nگفت‌وگو:\n${history}`, reply_markup: kb });
    return;
  }
  if (data.startsWith('TKT:REPLY:')) {
    const id = data.split(':')[2];
    const t = await getTicket(env, id);
    if (!t || String(t.user_id) !== String(uid) || t.status === 'closed') { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); return; }
    await setSession(env, uid, { awaiting: `tkt_user_reply:${id}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'پیام خود را برای افزودن به این تیکت ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'MISSIONS') {
    if (await isButtonDisabled(env, 'MISSIONS')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const { text, reply_markup } = await buildMissionsView(env, uid);
    await tgApi('sendMessage', { chat_id: chatId, text, reply_markup });
    // If there is an active weekly invite mission, show progress shortcut
    const missions = await listMissions(env);
    const inviteM = missions.find(m => m.enabled && m.type === 'invite');
    if (inviteM) {
      const wk = weekKey();
      const rk = `ref_week:${uid}:${wk}`;
      const rec = (await kvGetJson(env, rk)) || { count: 0 };
      const needed = Number(inviteM.config?.needed || 0);
      const left = Math.max(0, needed - (rec.count || 0));
      await tgApi('sendMessage', { chat_id: chatId, text: `👥 ماموریت دعوت این هفته: دعوت ${needed} نفر\nپیشرفت شما: ${rec.count||0}/${needed}${left>0 ? `\nبرای تکمیل ماموریت ${left} نفر دیگر باقی مانده است.` : ''}` });
      if (left <= 0) {
        // auto-complete if not done
        await completeMissionIfEligible(env, uid, inviteM);
      }
    }
    return;
  }
  // Mission interactions for quiz and weekly question
  if (data.startsWith('MIS:QUIZ:')) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const id = data.split(':')[2];
    const m = await kvGetJson(env, `mission:${id}`);
    if (!m || !m.enabled || m.type !== 'quiz') { await tgApi('sendMessage', { chat_id: chatId, text: 'ماموریت یافت نشد.' }); return; }
    const prog = await getUserMissionProgress(env, uid);
    const markKey = `${m.id}:${weekKey()}`; // weekly quiz default
    if ((prog.map||{})[markKey]) { await tgApi('sendMessage', { chat_id: chatId, text: 'شما قبلاً به این کوییز پاسخ داده‌اید.' }); return; }
    const q = m.config?.question || '-';
    const options = Array.isArray(m.config?.options) ? m.config.options : [];
    const note = 'توجه: هر کاربر فقط یک بار می‌تواند پاسخ دهد.';
    if (options.length >= 2) {
      const rows = options.map((opt, idx) => ([{ text: opt, callback_data: `MIS:QUIZ_ANS:${m.id}:${idx}` }]));
      rows.push([{ text: '❌ انصراف', callback_data: 'CANCEL' }]);
      await tgApi('sendMessage', { chat_id: chatId, text: `🎮 کوییز هفتگی:\n${q}\n\n${note}`, reply_markup: { inline_keyboard: rows } });
    } else {
      await setSession(env, uid, { awaiting: `mis_quiz_answer:${id}` });
      await tgApi('sendMessage', { chat_id: chatId, text: `🎮 کوییز هفتگی:\n${q}\n\n${note}\nپاسخ خود را ارسال کنید:`, reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    }
    return;
  }
  if (data.startsWith('MIS:QUIZ_ANS:')) {
    const [, , id, idxStr] = data.split(':');
    const m = await kvGetJson(env, `mission:${id}`);
    if (!m || !m.enabled || m.type !== 'quiz') { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); return; }
    const idx = Number(idxStr);
    const options = Array.isArray(m.config?.options) ? m.config.options : [];
    if (!Number.isFinite(idx) || idx < 0 || idx >= options.length) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'نامعتبر' }); return; }
    const prog = await getUserMissionProgress(env, uid);
    const markKey = `${m.id}:${weekKey()}`; // weekly quiz
    if ((prog.map||{})[markKey]) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'قبلاً پاسخ داده‌اید' }); return; }
    const ok = idx === Number(m.config?.correctIndex || -1);
    if (ok) {
      await completeMissionIfEligible(env, uid, m);
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: '✅ صحیح' });
      await tgApi('sendMessage', { chat_id: chatId, text: '✅ پاسخ صحیح بود و جایزه برای شما منظور شد.' });
    } else {
      // mark attempt without reward
      prog.map = prog.map || {};
      prog.map[markKey] = now();
      await setUserMissionProgress(env, uid, prog);
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: '❌ نادرست' });
      await tgApi('sendMessage', { chat_id: chatId, text: '❌ پاسخ شما نادرست بود. امکان پاسخ مجدد وجود ندارد.' });
    }
    return;
  }
  if (data.startsWith('MIS:Q:')) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const id = data.split(':')[2];
    const m = await kvGetJson(env, `mission:${id}`);
    if (!m || !m.enabled || m.type !== 'question') { await tgApi('sendMessage', { chat_id: chatId, text: 'ماموریت یافت نشد.' }); return; }
    const q = m.config?.question || '-';
    await setSession(env, uid, { awaiting: `mis_question_answer:${id}` });
    await tgApi('sendMessage', { chat_id: chatId, text: `❓ سوال هفتگی:\n${q}\n\nتوجه: هر کاربر فقط یک بار می‌تواند پاسخ دهد.\nپاسخ خود را ارسال کنید:`, reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'WEEKLY_CHECKIN') {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    // strict 7-day cooldown per user
    const WEEK_MS = 7 * 24 * 60 * 60 * 1000;
    const nowTs = now();
    const prog = await getUserMissionProgress(env, uid);
    const lastTs = Number(prog.weekly_last_ts || 0);
    const formatDurationFull = (ms) => {
      const totalSeconds = Math.max(0, Math.ceil(ms / 1000));
      const days = Math.floor(totalSeconds / 86400);
      const hours = Math.floor((totalSeconds % 86400) / 3600);
      const minutes = Math.floor((totalSeconds % 3600) / 60);
      const seconds = totalSeconds % 60;
      return `${days} روز و ${hours} ساعت و ${minutes} دقیقه و ${seconds} ثانیه`;
    };
    if (lastTs && (nowTs - lastTs) < WEEK_MS) {
      const remain = WEEK_MS - (nowTs - lastTs);
      const human = formatDurationFull(remain);
      await tgApi('sendMessage', { chat_id: chatId, text: `⏳ هنوز زود است. زمان باقی‌مانده تا دریافت بعدی: ${human}` });
      return;
    }
    // mark last claim and keep a week marker for compatibility
    prog.map = prog.map || {};
    prog.map[`checkin:${weekKey()}`] = nowTs;
    prog.weekly_last_ts = nowTs;
    await setUserMissionProgress(env, uid, prog);

    const uKey = `user:${uid}`;
    const user = (await kvGetJson(env, uKey)) || { id: uid, diamonds: 0 };
    const reward = 2; // weekly reward amount
    user.diamonds = (user.diamonds || 0) + reward;
    await kvPutJson(env, uKey, user);
    const humanNext = formatDurationFull(WEEK_MS);
    await tgApi('sendMessage', { chat_id: chatId, text: `✅ پاداش هفتگی دریافت شد. ${reward} الماس دریافت کردید.\n⏱ زمان تا دریافت بعدی: ${humanNext}` });
    return;
  }
  if (data === 'LOTTERY') {
    if (await isButtonDisabled(env, 'LOTTERY')) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش موقتاً غیرفعال است.' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const cfg = await getLotteryConfig(env);
    const enrolled = await isUserEnrolledToday(env, uid);
    const pool = (await kvGetJson(env, `lottery:pool:${dayKey()}`)) || [];
    const poolCount = pool.length;
    const kbd = { inline_keyboard: [
      ...(cfg.enabled && !enrolled ? [[{ text: '✨ ثبت‌نام در قرعه‌کشی امروز', callback_data: 'LOTTERY:ENROLL' }]] : []),
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] };
    const txt = cfg.enabled
    ? `🎟 قرعه‌کشی فعال است. برندگان: ${cfg.winners||0} | جایزه: ${cfg.reward_diamonds||0} الماس\n👥 ثبت‌نام امروز: ${poolCount} نفر${enrolled ? '\nشما برای امروز ثبت‌نام کرده‌اید.' : ''}`
      : `🎟 قرعه‌کشی در حال حاضر غیرفعال است.\n👥 ثبت‌نام امروز: ${poolCount} نفر`;
    await tgApi('sendMessage', { chat_id: chatId, text: txt, reply_markup: kbd });
    return;
  }
  if (data === 'LOTTERY:ENROLL') {
    const cfg = await getLotteryConfig(env);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    if (!cfg.enabled) { await tgApi('sendMessage', { chat_id: chatId, text: 'در حال حاضر فعال نیست.' }); return; }
    const ok = await userEnrollToday(env, uid);
    await tgApi('sendMessage', { chat_id: chatId, text: ok ? '✅ ثبت‌نام شما برای امروز انجام شد.' : 'قبلا ثبت‌نام کرده‌اید.' });
    return;
  }
  if (data === 'REDEEM_GIFT') {
    await setSession(env, uid, { awaiting: 'redeem_gift' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: '🎁 کد هدیه را وارد کنید:' });
    return;
  }
  if (data === 'REFERRAL') {
    const botUsername = await getBotUsername(env);
    const refLink = botUsername ? `https://t.me/${botUsername}?start=${uid}` : '—';
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `👥 زیرمجموعه گیری:\n\nبا اشتراک‌گذاری لینک زیر به ازای هر کاربر فعال، الماس دریافت می‌کنید.\n\n${refLink}` });
    return;
  }
  if (data.startsWith('MYFILES:')) {
    const page = parseInt(data.split(':')[1] || '0', 10) || 0;
    const built = await buildMyFilesKeyboard(env, uid, page);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    return;
  }
  if (data.startsWith('DETAILS:')) {
    const parts = data.split(':');
    const token = parts[1];
    const page = parseInt(parts[2] || '0', 10) || 0;
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const f = await kvGetJson(env, `file:${token}`);
    if (!f) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    const link = await getShareLink(env, token);
    const details = `📄 جزئیات فایل:
نام: ${f.name || '-'}
توکن: \`${token}\`
حجم: ${formatFileSize(f.size||0)}
هزینه (الماس): ${f.cost_points||0}
دانلود: ${f.downloads||0}
آخرین دانلود: ${f.last_download ? formatDate(f.last_download) : '-'}
وضعیت: ${f.disabled ? '🔴 غیرفعال' : '🟢 فعال'}
 محدودیت دانلود: ${(f.max_downloads||0) > 0 ? f.max_downloads : 'نامحدود'}
 حذف پس از اتمام: ${f.delete_on_limit ? 'بله' : 'خیر'}
لینک اشتراک: \`${link}\``;
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const baseRow = [{ text: '📥 دریافت', callback_data: `SEND:${token}` }, { text: '🔗 کپی لینک', callback_data: `LINK:${token}` }];
    const adminExtras = isAdmin(uid) ? [
      { text: '✏️ ویرایش نام', callback_data: `RENAME:${token}` },
      { text: '♻️ جایگزینی', callback_data: `REPLACE:${token}` },
      { text: '🔒 محدودیت دانلود', callback_data: `LIMIT:${token}` }
    ] : [{ text: '✏️ تغییر نام', callback_data: `RENAME:${token}` }];
    const rows = [baseRow, adminExtras, [{ text: '⬅️ بازگشت', callback_data: `MYFILES:${page}` }]];
    await tgApi('sendMessage', { chat_id: chatId, text: details, parse_mode: 'Markdown', reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data.startsWith('LINK:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    const botUsername = await getBotUsername(env);
    const link = botUsername ? `https://t.me/${botUsername}?start=d_${token}` : `${domainFromWebhook()}/f/${token}`;
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'لینک ارسال شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `🔗 لینک دانلود:\n${link}\n\nبرای کسب الماس لینک را از داخل ربات برای دیگران ارسال کنید.` });
    return;
  }
  if (data.startsWith('RENAME:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    await setSession(env, uid, { awaiting: `rename:${token}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'نام جدید را ارسال کنید:' });
    return;
  }
  if (data.startsWith('SEND:')) {
    const token = data.split(':')[1];
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    // Only allow owner or admin to fetch directly inside the bot
    // For regular users, go through handleBotDownload flow (membership + cost)
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id }); await handleBotDownload(env, uid, chatId, token, ''); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await deliverStoredContent(chatId, file);
    // update download stats
    file.downloads = (file.downloads || 0) + 1; file.last_download = now();
    await kvPutJson(env, `file:${token}`, file);
    return;
  }
  if (data.startsWith('CONFIRM_SPEND:')) {
    const [, token, neededStr, ref = ''] = data.split(':');
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const needed = parseInt(neededStr, 10) || 0;
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    const ok = await checkRateLimit(env, uid, 'confirm_spend_click', 5, 60_000);
    if (!ok) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'سریع‌تر از حد مجاز' }); return; }
    // daily limit enforcement
    const settings = await getSettings(env);
    const limit = settings.daily_limit || 0;
    if (limit > 0 && !isAdmin(uid)) {
      const dk = `usage:${uid}:${dayKey()}`;
      const used = (await kvGetJson(env, dk)) || { count: 0 };
      if ((used.count || 0) >= limit) {
        await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'سقف روزانه پر شده' });
        return;
      }
    }
    const user = (await kvGetJson(env, `user:${uid}`)) || { diamonds: 0 };
    if ((user.diamonds || 0) < needed) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'الماس کافی نیست' }); return; }
    user.diamonds = (user.diamonds || 0) - needed; await kvPutJson(env, `user:${uid}`, user);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'پرداخت شد' });
    // per-file limit enforcement before delivery
    if ((file.max_downloads || 0) > 0 && (file.downloads || 0) >= file.max_downloads) {
      await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ ظرفیت دانلود این فایل به پایان رسیده است.' });
      if (file.delete_on_limit) {
        try {
          const upKey = `uploader:${file.owner}`;
          const upList = (await kvGetJson(env, upKey)) || [];
          await kvPutJson(env, upKey, upList.filter(t => t !== token));
          await kvDelete(env, `file:${token}`);
        } catch (_) {}
      }
      return;
    }
    // deliver file
    await deliverStoredContent(chatId, file);
    file.downloads = (file.downloads || 0) + 1; file.last_download = now();
    await kvPutJson(env, `file:${token}`, file);
    if ((file.max_downloads || 0) > 0 && (file.downloads || 0) >= file.max_downloads && file.delete_on_limit) {
      try {
        const upKey = `uploader:${file.owner}`;
        const upList = (await kvGetJson(env, upKey)) || [];
        await kvPutJson(env, upKey, upList.filter(t => t !== token));
        await kvDelete(env, `file:${token}`);
      } catch (_) {}
    }
    if ((limit > 0) && !isAdmin(uid)) {
      const dk = `usage:${uid}:${dayKey()}`;
      const used = (await kvGetJson(env, dk)) || { count: 0 };
      used.count = (used.count || 0) + 1;
      await kvPutJson(env, dk, used);
    }
    // referral credit if any
    if (ref && String(ref) !== String(file.owner)) {
      if (!user.ref_credited) {
        const refUser = (await kvGetJson(env, `user:${ref}`)) || null;
        if (refUser) {
          refUser.diamonds = (refUser.diamonds || 0) + 1;
          refUser.referrals = (refUser.referrals || 0) + 1;
          await kvPutJson(env, `user:${ref}`, refUser);
          user.ref_credited = true;
          user.referred_by = user.referred_by || Number(ref);
          await kvPutJson(env, `user:${uid}`, user);
        }
      }
    }
    return;
  }
  if (data.startsWith('COST:')) {
    const token = data.split(':')[1];
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'هزینه دلخواه را انتخاب کنید:', reply_markup: buildCostKeyboard(token) });
    return;
  }
  if (data.startsWith('COST_SET:')) {
    const [, token, amountStr] = data.split(':');
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const amount = parseInt(amountStr, 10) || 0;
    const file = await kvGetJson(env, `file:${token}`);
    if (file && isAdmin(uid)) {
      file.cost_points = amount; await kvPutJson(env, `file:${token}`, file);
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: `هزینه ${amount}` });
      const built = await buildMyFilesKeyboard(env, uid, 0);
      await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    } else {
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'اجازه ندارید' });
    }
    return;
  }
  if (data.startsWith('COST_CUSTOM:')) {
    const token = data.split(':')[1];
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    await setSession(env, uid, { awaiting: `setcost:${token}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'عدد هزینه دلخواه را ارسال کنید:' });
    return;
  }
  if (data.startsWith('LIMIT:')) {
    const token = data.split(':')[1];
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'محدودیت دانلود را انتخاب کنید:', reply_markup: buildLimitKeyboard(token) });
    return;
  }
  if (data.startsWith('LIMIT_SET:')) {
    const [, token, amountStr] = data.split(':');
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const amount = parseInt(amountStr, 10) || 0;
    const file = await kvGetJson(env, `file:${token}`);
    if (file && isAdmin(uid)) {
      file.max_downloads = Math.max(0, amount);
      await kvPutJson(env, `file:${token}`, file);
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: `حد: ${amount || '∞'}` });
      const built = await buildMyFilesKeyboard(env, uid, 0);
      await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    } else {
      await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'اجازه ندارید' });
    }
    return;
  }
  if (data.startsWith('LIMIT_CUSTOM:')) {
    const token = data.split(':')[1];
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    await setSession(env, uid, { awaiting: `setlimit:${token}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'عدد محدودیت دانلود را ارسال کنید (0 = نامحدود):' });
    return;
  }
  if (data.startsWith('DELAFTER:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    file.delete_on_limit = !file.delete_on_limit;
    await kvPutJson(env, `file:${token}`, file);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: file.delete_on_limit ? 'حذف پس از اتمام: روشن' : 'خاموش' });
    const built = await buildMyFilesKeyboard(env, uid, 0);
    await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    return;
  }
  if (data.startsWith('TOGGLE:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    file.disabled = !file.disabled; await kvPutJson(env, `file:${token}`, file);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: file.disabled ? 'غیرفعال شد' : 'فعال شد' });
    const built = await buildMyFilesKeyboard(env, uid, 0);
    await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    return;
  }
  if (data.startsWith('DEL:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    const file = await kvGetJson(env, `file:${token}`);
    if (!file) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    // remove from uploader index
    const upKey = `uploader:${file.owner}`;
    const upList = (await kvGetJson(env, upKey)) || [];
    const newList = upList.filter(t => t !== token);
    await kvPutJson(env, upKey, newList);
    // delete file meta
    await kvDelete(env, `file:${token}`);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'حذف شد' });
    const built = await buildMyFilesKeyboard(env, uid, 0);
    await tgApi('sendMessage', { chat_id: chatId, text: built.text, reply_markup: built.reply_markup });
    return;
  }
  if (data.startsWith('REPLACE:')) {
    const token = data.split(':')[1];
    if (!isValidTokenFormat(token)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'توکن نامعتبر' }); return; }
    if (!isAdmin(uid)) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'فقط ادمین' }); return; }
    const f = await kvGetJson(env, `file:${token}`);
    if (!f) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    await setSession(env, uid, { awaiting: `replace:${token}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `لطفاً محتوای جدید برای جایگزینی توکن ${token} را ارسال کنید (متن/رسانه).` });
    return;
  }

  if (data === 'ADMIN:GIVEPOINTS' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'givepoints_uid' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی عددی کاربر را وارد کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }

  if (data === 'ADMIN:TOGGLE_UPDATE' && isAdmin(uid)) {
    const current = (await kvGetJson(env, 'bot:update_mode')) || false;
    await kvPutJson(env, 'bot:update_mode', !current);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: !current ? 'حالت آپدیت فعال شد' : 'حالت آپدیت غیرفعال شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `حالت آپدیت: ${!current ? 'فعال' : 'غیرفعال'}` });
    return;
  }
  // Removed ADMIN:TOGGLE_SERVICE per request
  if (data === 'ADMIN:BROADCAST' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'broadcast' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'متن پیام عمومی را ارسال کنید:' });
    return;
  }
  if (data.startsWith('PAYAPP:') && isAdmin(uid)) {
    const id = data.split(':')[1];
    const key = `purchase:${id}`;
    const purchase = await kvGetJson(env, key);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    if (!purchase || purchase.status !== 'pending_review') {
      await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ درخواست نامعتبر است.' });
      return;
    }
    const userKey = `user:${purchase.user_id}`;
    const user = (await kvGetJson(env, userKey)) || { id: purchase.user_id, diamonds: 0 };
    user.diamonds = (user.diamonds || 0) + (purchase.diamonds || 0);
    await kvPutJson(env, userKey, user);
    purchase.status = 'approved'; purchase.processed_by = uid; purchase.processed_at = now();
    await kvPutJson(env, key, purchase);
    await tgApi('sendMessage', { chat_id: purchase.user_id, text: `✅ پرداخت شما تایید شد. ${purchase.diamonds} الماس به حساب شما اضافه شد.` });
    await tgApi('sendMessage', { chat_id: chatId, text: `انجام شد. ${purchase.diamonds} الماس به کاربر ${purchase.user_id} اضافه شد.` });
    return;
  }
  if (data.startsWith('PAYREJ:') && isAdmin(uid)) {
    const id = data.split(':')[1];
    const key = `purchase:${id}`;
    const purchase = await kvGetJson(env, key);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    if (!purchase || purchase.status !== 'pending_review') {
      await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ درخواست نامعتبر است.' });
      return;
    }
    purchase.status = 'rejected'; purchase.processed_by = uid; purchase.processed_at = now();
    await kvPutJson(env, key, purchase);
    await tgApi('sendMessage', { chat_id: purchase.user_id, text: '❌ پرداخت شما تایید نشد. لطفاً با پشتیبانی تماس بگیرید.' });
    await tgApi('sendMessage', { chat_id: chatId, text: `درخواست ${id} رد شد.` });
    return;
  }
  if (data === 'ADMIN:STATS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const users = (await kvGetJson(env, 'index:users')) || [];
    const userCount = users.length;
    const enabled = (await kvGetJson(env, 'bot:enabled')) ?? true;
    const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
    const lastWebhookAt = (await kvGetJson(env, 'bot:last_webhook')) || 0;
    const connected = typeof lastWebhookAt === 'number' && (now() - lastWebhookAt) < 5 * 60 * 1000;
    const admins = await getAdminIds(env);
    const joinReq = await getRequiredChannels(env);

    const LIMIT_USERS = 300;
    let totalFiles = 0;
    let totalDownloads = 0;
    let disabledFiles = 0;
    let filesCreated7d = 0;
    let usersCreated7d = 0;
    const sevenDaysAgo = now() - 7 * 24 * 60 * 60 * 1000;
    const topFiles = [];
    const uploaderStats = new Map();

    for (const uidIter of users.slice(0, LIMIT_USERS)) {
      const uMeta = (await kvGetJson(env, `user:${uidIter}`)) || {};
      if ((uMeta.created_at || 0) >= sevenDaysAgo) usersCreated7d++;
      const list = (await kvGetJson(env, `uploader:${uidIter}`)) || [];
      totalFiles += list.length;
      for (const t of list) {
        const f = await kvGetJson(env, `file:${t}`);
        if (!f) continue;
        totalDownloads += f.downloads || 0;
        if (f.disabled) disabledFiles++;
        if ((f.created_at || 0) >= sevenDaysAgo) filesCreated7d++;
        topFiles.push({ name: f.name || 'file', downloads: f.downloads || 0, token: f.token || t });
        const owner = f.owner;
        const s = uploaderStats.get(owner) || { files: 0, downloads: 0 };
        s.files += 1;
        s.downloads += f.downloads || 0;
        uploaderStats.set(owner, s);
      }
    }

    const topFilesText = topFiles
      .sort((a, b) => (b.downloads || 0) - (a.downloads || 0))
      .slice(0, 5)
      .map((f, i) => `${i + 1}. ${escapeHtml(f.name)} — ${f.downloads || 0} دانلود`)
      .join('\n') || '—';

    const topUploadersText = Array.from(uploaderStats.entries())
      .map(([owner, s]) => ({ owner, ...s }))
      .sort((a, b) => (b.downloads || 0) - (a.downloads || 0))
      .slice(0, 5)
      .map((u, i) => `${i + 1}. ${u.owner} — ${u.downloads} دانلود (${u.files} فایل)`) 
      .join('\n') || '—';

    const avgDownloads = totalFiles ? Math.round(totalDownloads / totalFiles) : 0;
    const statsText = `📊 آمار پیشرفته ربات\n\n` +
      `🔧 وضعیت سرویس: ${enabled ? '🟢 فعال' : '🔴 غیرفعال'}\n` +
      `🛠 حالت آپدیت: ${updateMode ? 'فعال' : 'غیرفعال'}\n` +
      `🔌 اتصال وبهوک: ${connected ? 'آنلاین' : 'آفلاین'}${lastWebhookAt ? ' (' + formatDate(lastWebhookAt) + ')' : ''}\n` +
      `👑 ادمین‌ها: ${admins.length}\n` +
      `📣 کانال‌های اجباری: ${joinReq.length}${joinReq.length ? ' — ' + joinReq.join(', ') : ''}\n\n` +
      `👥 کاربران کل: ${userCount.toLocaleString('fa-IR')}\n` +
      `🆕 کاربران ۷ روز اخیر: ${usersCreated7d.toLocaleString('fa-IR')} (نمونه‌گیری از ${Math.min(LIMIT_USERS, userCount)} کاربر نخست)\n\n` +
      `📁 فایل‌ها: ${totalFiles.toLocaleString('fa-IR')} (غیرفعال: ${disabledFiles.toLocaleString('fa-IR')})\n` +
      `📥 کل دانلودها: ${totalDownloads.toLocaleString('fa-IR')}\n` +
      `📈 میانگین دانلود به ازای هر فایل: ${avgDownloads.toLocaleString('fa-IR')}\n` +
      `🆕 فایل‌های ۷ روز اخیر: ${filesCreated7d.toLocaleString('fa-IR')}\n\n` +
      `🏆 برترین فایل‌ها (براساس دانلود):\n${topFilesText}\n\n` +
      `👤 برترین آپلودرها: \n${topUploadersText}`;

    await tgApi('sendMessage', { 
      chat_id: chatId, 
      text: statsText, 
      reply_markup: { inline_keyboard: [
        [{ text: '📊 جزئیات بیشتر', callback_data: 'ADMIN:STATS:DETAILS' }],
        [{ text: '🔄 تازه‌سازی', callback_data: 'ADMIN:STATS' }],
        [{ text: '🏠 منو', callback_data: 'MENU' }]
      ] }
    });
    return;
  }
  if (data === 'ADMIN:STATS:DETAILS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    // Compute highest weekly points among users (sample limited)
    const users = (await kvGetJson(env, 'index:users')) || [];
    const wk = weekKey();
    let topUser = null; let topPts = -1;
    for (const u of users.slice(0, 500)) {
      const rec = (await kvGetJson(env, `points_week:${u}:${wk}`)) || { points: 0 };
      if ((rec.points || 0) > topPts) { topPts = rec.points || 0; topUser = u; }
    }
    const highestWeekly = topUser ? `${topUser} — ${topPts} الماس` : '—';
    const text = `📊 آمار جزئی\n\n🏆 بیشترین امتیاز کسب‌شده در این هفته: ${highestWeekly}\n\nدکمه‌های بیشتر:`;
    const rows = [
      [{ text: '🏆 بیشترین امتیاز هفته (تازه‌سازی)', callback_data: 'ADMIN:STATS:DETAILS' }],
      [{ text: '⬅️ بازگشت', callback_data: 'ADMIN:STATS' }]
    ];
    await tgApi('sendMessage', { chat_id: chatId, text, reply_markup: { inline_keyboard: rows } });
    return;
  }
  // ===== Tickets: Admin management panel
  if (data === 'ADMIN:TICKETS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const list = await listTickets(env, { limit: 10 });
    const lines = list.length ? list.map(t => `#${t.id} | ${t.status || 'open'} | از ${t.user_id} | ${escapeHtml(t.subject || '-')}`).join('\n') : '—';
    const rows = [
      ...list.map(t => ([{ text: `🗂 ${t.id}`, callback_data: `ATK:VIEW:${t.id}` }])),
      [{ text: '🔄 تازه‌سازی', callback_data: 'ADMIN:TICKETS' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ];
    await tgApi('sendMessage', { chat_id: chatId, text: `🧾 مدیریت تیکت‌ها\n\n${lines}`, reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data.startsWith('ATK:VIEW:') && isAdmin(uid)) {
    const id = data.split(':')[2];
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const t = await getTicket(env, id);
    if (!t) { await tgApi('sendMessage', { chat_id: chatId, text: 'تیکت یافت نشد.' }); return; }
    const userBlocked = await isUserBlocked(env, t.user_id);
    const msgs = await getTicketMessages(env, id, 20);
    const history = msgs.map(m => `${m.from === 'admin' ? 'ادمین' : 'کاربر'} (${formatDate(m.at)}):\n${m.text}`).join('\n\n') || '—';
    const txt = `#${t.id} | ${t.status || 'open'}\nاز: ${t.user_id}${t.username ? ` (@${t.username})` : ''}\nدسته: ${t.category || '-'}\nموضوع: ${t.subject || '-'}\n${t.desc ? `\nشرح:\n${t.desc}\n` : ''}\nگفت‌وگو (آخرین ۲۰ پیام):\n${history}`;
    const kb = { inline_keyboard: [
      [{ text: '✉️ پاسخ', callback_data: `ATK:REPLY:${t.id}` }, { text: t.status === 'closed' ? '🔓 باز کردن' : '🔒 بستن', callback_data: `ATK:TOGGLE:${t.id}` }],
      [{ text: userBlocked ? '🟢 آنبلاک کاربر' : '⛔️ Block کاربر', callback_data: `ATK:BLK:${t.user_id}:${userBlocked ? 'UN' : 'BL'}` }],
      [{ text: '🗑 حذف تیکت', callback_data: `ATK:DEL:${t.id}` }],
      [{ text: '⬅️ بازگشت', callback_data: 'ADMIN:TICKETS' }]
    ] };
    await tgApi('sendMessage', { chat_id: chatId, text: txt, reply_markup: kb });
    return;
  }
  if (data.startsWith('ATK:REPLY:') && isAdmin(uid)) {
    const id = data.split(':')[2];
    await setSession(env, uid, { awaiting: `admin_ticket_reply:${id}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'متن پاسخ را ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data.startsWith('ATK:TOGGLE:') && isAdmin(uid)) {
    const id = data.split(':')[2];
    const t = await getTicket(env, id);
    if (t) { t.status = t.status === 'closed' ? 'open' : 'closed'; await putTicket(env, t); }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'بروزرسانی شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `وضعیت تیکت #${id}: ${t?.status || '-'}` });
    // notify the ticket owner if closed
    try {
      if (t && t.status === 'closed') {
        await tgApi('sendMessage', { chat_id: t.user_id, text: `📪 تیکت شما (#${t.id}) بسته شد. اگر نیاز به ادامه دارید، می‌توانید تیکت جدیدی ثبت کنید.` });
      }
    } catch (_) {}
    return;
  }
  if (data.startsWith('ATK:BLK:') && isAdmin(uid)) {
    const [, , userIdStr, op] = data.split(':');
    const targetId = Number(userIdStr);
    if (op === 'BL') await blockUser(env, targetId); else await unblockUser(env, targetId);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: op === 'BL' ? 'مسدود شد' : 'آنبلاک شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `کاربر ${targetId} ${op === 'BL' ? 'مسدود' : 'آنبلاک'} شد.` });
    return;
  }
  if (data.startsWith('ATK:DEL:') && isAdmin(uid)) {
    const id = data.split(':')[2];
    await deleteTicket(env, id);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'حذف شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `تیکت #${id} حذف شد.` });
    return;
  }
  if (data === 'ADMIN:MANAGE_JOIN' && isAdmin(uid)) {
    const channels = await getRequiredChannels(env);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const lines = channels.map((c, i) => `${i+1}. ${c}`).join('\n') || '—';
    await tgApi('sendMessage', { chat_id: chatId, text: `📣 کانال‌های اجباری فعلی:\n${lines}\n\nبرای افزودن/حذف، از دکمه‌ها استفاده کنید.`, reply_markup: { inline_keyboard: [
      [{ text: '➕ افزودن کانال', callback_data: 'ADMIN:JOIN_ADD' }],
      ...(channels.map((c, idx) => ([{ text: `❌ حذف ${c}`, callback_data: `ADMIN:JOIN_DEL:${idx}` }]))),
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:JOIN_ADD' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'join_add' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'شناسه کانال را ارسال کنید (با @ یا آی‌دی عددی):' });
    return;
  }
  if (data.startsWith('ADMIN:JOIN_DEL:') && isAdmin(uid)) {
    const idx = parseInt(data.split(':')[2], 10);
    const channels = await getRequiredChannels(env);
    if (idx >= 0 && idx < channels.length) {
      channels.splice(idx, 1);
      await setRequiredChannels(env, channels);
    }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'به‌روزرسانی شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: 'به‌روزرسانی انجام شد.', reply_markup: await buildDynamicMainMenu(env, uid) });
    return;
  }
  if (data === 'ADMIN:MANAGE_ADMINS' && isAdmin(uid)) {
    const admins = await getAdminIds(env);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `👑 ادمین‌ها:\n${admins.join(', ') || '—'}`, reply_markup: { inline_keyboard: [
      [{ text: '➕ افزودن ادمین', callback_data: 'ADMIN:ADD_ADMIN' }],
      ...(admins.filter(id => id !== Number(uid)).map(id => ([{ text: `❌ حذف ${id}`, callback_data: `ADMIN:DEL_ADMIN:${id}` }]))),
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:GIFTS' && isAdmin(uid)) {
    const list = await listGiftCodes(env, 20);
    const lines = list.map(g => `${g.code} | ${g.amount} الماس | ${g.disabled ? 'غیرفعال' : 'فعال'} | ${g.used||0}/${g.max_uses||'∞'}`).join('\n') || '—';
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `🎁 گیفت‌کدها:\n${lines}`, reply_markup: { inline_keyboard: [
      [{ text: '➕ ایجاد گیفت‌کد', callback_data: 'ADMIN:GIFT_CREATE' }],
      ...list.map(g => ([
        { text: `${g.disabled ? '🟢 فعال‌سازی' : '🔴 غیرفعال'}`, callback_data: `ADMIN:GIFT_TOGGLE:${g.code}` },
        { text: '🗑 حذف', callback_data: `ADMIN:GIFT_DELETE:${g.code}` }
      ])),
      [{ text: '🔄 تازه‌سازی', callback_data: 'ADMIN:GIFTS' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] } });
    return;
  }
  if (data.startsWith('ADMIN:GIFT_TOGGLE:') && isAdmin(uid)) {
    const code = data.split(':')[2];
    const key = await giftCodeKey(code);
    const meta = await kvGetJson(env, key);
    if (meta) { meta.disabled = !meta.disabled; await kvPutJson(env, key, meta); }
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'بروزرسانی شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `کد ${code} ${meta?.disabled ? 'غیرفعال' : 'فعال'} شد.` });
    return;
  }
  if (data.startsWith('ADMIN:GIFT_DELETE:') && isAdmin(uid)) {
    const code = data.split(':')[2];
    const key = await giftCodeKey(code);
    await kvDelete(env, key);
    // remove from index
    const idx = (await kvGetJson(env, 'gift:index')) || [];
    const c = String(code).trim().toUpperCase();
    const next = idx.filter(x => x !== c);
    await kvPutJson(env, 'gift:index', next);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'حذف شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `کد ${code} حذف شد.` });
    return;
  }
  if (data === 'ADMIN:GIFT_CREATE' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: `admin_create_gift:code:` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'کد دلخواه را وارد کنید (حروف/اعداد):' });
    return;
  }
  if (data === 'ADMIN:SETTINGS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const s = await getSettings(env);
    await tgApi('sendMessage', { chat_id: chatId, text: `⚙️ تنظیمات سرویس:\n- محدودیت روزانه دانلود: ${s.daily_limit}\n- پیام خوش‌آمد: ${s.welcome_message ? 'تعریف شده' : '—'}`, reply_markup: { inline_keyboard: [
      [{ text: '✏️ ویرایش پیام خوش‌آمد', callback_data: 'ADMIN:SET:WELCOME' }, { text: '🔢 تغییر سقف روزانه', callback_data: 'ADMIN:SET:DAILY' }],
      [{ text: '📝 ویرایش عنوان دکمه‌ها', callback_data: 'ADMIN:SET:BUTTONS' }],
      [{ text: '🚫 مدیریت دکمه‌های غیرفعال', callback_data: 'ADMIN:DISABLE_BTNS' }],
      [{ text: '⬅️ بازگشت به پنل', callback_data: 'ADMIN:PANEL' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:DISABLE_BTNS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const s = await getSettings(env);
    const map = s.disabled_buttons || {};
    const items = [
      { key: 'GET_BY_TOKEN', label: labelFor(s.button_labels, 'get_by_token', '🔑 دریافت با توکن') },
      { key: 'MISSIONS', label: labelFor(s.button_labels, 'missions', '📆 مأموریت‌ها') },
      { key: 'LOTTERY', label: labelFor(s.button_labels, 'lottery', '🎟 قرعه‌کشی') },
      { key: 'SUB:REFERRAL', label: '👥 زیرمجموعه گیری' },
      { key: 'SUB:ACCOUNT', label: '👤 حساب کاربری' },
      { key: 'BUY_DIAMONDS', label: labelFor(s.button_labels, 'buy_points', '💳 خرید الماس') }
    ];
    const rows = items.map(it => ([{ text: `${map[it.key] ? '🟢 فعال‌سازی' : '🔴 غیرفعال'} ${it.label}` , callback_data: `ADMIN:BTN_TOGGLE:${encodeURIComponent(it.key)}` }]));
    rows.push([{ text: '⬅️ بازگشت', callback_data: 'ADMIN:SETTINGS' }]);
    await tgApi('sendMessage', { chat_id: chatId, text: '🚫 مدیریت دکمه‌ها:', reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data.startsWith('ADMIN:BTN_TOGGLE:') && isAdmin(uid)) {
    const key = decodeURIComponent(data.split(':')[2]);
    const s = await getSettings(env);
    const map = s.disabled_buttons || {};
    map[key] = !map[key];
    s.disabled_buttons = map;
    await setSettings(env, s);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'به‌روزرسانی شد' });
    // Refresh list view with human-friendly labels
    const items = [
      { key: 'GET_BY_TOKEN', label: labelFor(s.button_labels, 'get_by_token', '🔑 دریافت با توکن') },
      { key: 'MISSIONS', label: labelFor(s.button_labels, 'missions', '📆 مأموریت‌ها') },
      { key: 'LOTTERY', label: labelFor(s.button_labels, 'lottery', '🎟 قرعه‌کشی') },
      { key: 'SUB:REFERRAL', label: '👥 زیرمجموعه گیری' },
      { key: 'SUB:ACCOUNT', label: '👤 حساب کاربری' },
      { key: 'BUY_DIAMONDS', label: labelFor(s.button_labels, 'buy_points', '💳 خرید الماس') }
    ];
    const rows = items.map(it => ([{ text: `${map[it.key] ? '🟢 فعال‌سازی' : '🔴 غیرفعال'} ${it.label}` , callback_data: `ADMIN:BTN_TOGGLE:${encodeURIComponent(it.key)}` }]));
    rows.push([{ text: '⬅️ بازگشت', callback_data: 'ADMIN:SETTINGS' }]);
    await tgApi('sendMessage', { chat_id: chatId, text: '🚫 مدیریت دکمه‌ها:', reply_markup: { inline_keyboard: rows } });
    return;
  }
  if (data === 'ADMIN:SET:WELCOME' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'set_welcome' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'متن پیام خوش‌آمد جدید را ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:SET:DAILY' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'set_daily_limit' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'محدودیت روزانه (عدد) را ارسال کنید. 0 برای غیرفعال:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:SET:BUTTONS' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'set_buttons' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'JSON دکمه‌ها را ارسال کنید. مثال: {"profile":"پروفایل من"}', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:MISSIONS' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const v = await listMissions(env);
    const listText = v.length ? v.map(m => `- ${m.id}: ${m.title} (${m.period||'once'} | ${m.type||'generic'}) ${m.enabled ? '🟢' : '🔴'} +${m.reward}`).join('\n') : '—';
    await tgApi('sendMessage', { chat_id: chatId, text: `📆 مأموریت‌ها:\n${listText}`, reply_markup: { inline_keyboard: [
      [{ text: '➕ ایجاد', callback_data: 'ADMIN:MIS:CREATE' }, { text: '✏️ ویرایش', callback_data: 'ADMIN:MIS:EDIT' }],
      [{ text: '🧩 کوییز هفتگی', callback_data: 'ADMIN:MIS:CREATE:QUIZ' }, { text: '❓ سوال هفتگی', callback_data: 'ADMIN:MIS:CREATE:QUESTION' }, { text: '👥 دعوت هفتگی', callback_data: 'ADMIN:MIS:CREATE:INVITE' }],
      ...v.map(m => ([
        { text: `${m.enabled ? '🔴 غیرفعال' : '🟢 فعال‌سازی'} ${m.id}` , callback_data: `ADMIN:MIS:TOGGLE:${m.id}` },
        { text: `🗑 حذف ${m.id}`, callback_data: `ADMIN:MIS:DEL:${m.id}` }
      ])),
      [{ text: '⬅️ بازگشت به پنل', callback_data: 'ADMIN:PANEL' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:MIS:EDIT' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'mission_edit:id' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'شناسه ماموریت برای ویرایش را ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:MIS:CREATE' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'mission_create:title' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'عنوان ماموریت را ارسال کنید:', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data.startsWith('ADMIN:MIS:TOGGLE:') && isAdmin(uid)) {
    const id = data.split(':')[3];
    const key = `mission:${id}`;
    const m = await kvGetJson(env, key);
    if (!m) { await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'یافت نشد' }); return; }
    m.enabled = !m.enabled;
    await kvPutJson(env, key, m);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: m.enabled ? 'فعال شد' : 'غیرفعال شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: `ماموریت ${id} اکنون ${m.enabled ? 'فعال' : 'غیرفعال'} است.` });
    return;
  }
  if (data === 'ADMIN:MIS:CREATE:QUIZ' && isAdmin(uid)) {
    const draft = { type: 'quiz' };
    await setSession(env, uid, { awaiting: `mission_quiz:q:${btoa(JSON.stringify(draft))}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'سوال کوتاه کوییز را ارسال کنید:' });
    return;
  }
  if (data === 'ADMIN:MIS:CREATE:QUESTION' && isAdmin(uid)) {
    const draft = { type: 'question' };
    await setSession(env, uid, { awaiting: `mission_q:question:${btoa(JSON.stringify(draft))}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'سوال مسابقه هفتگی را ارسال کنید:' });
    return;
  }
  if (data === 'ADMIN:MIS:CREATE:INVITE' && isAdmin(uid)) {
    const draft = { type: 'invite' };
    await setSession(env, uid, { awaiting: `mission_inv:count:${btoa(JSON.stringify(draft))}` });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'تعداد دعوت مورد نیاز این هفته را وارد کنید (مثلاً 3):' });
    return;
  }
  if (data.startsWith('ADMIN:MIS:DEL:') && isAdmin(uid)) {
    const id = data.split(':')[3];
    await deleteMission(env, id);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'حذف شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: 'ماموریت حذف شد.' });
    return;
  }
  if (data === 'ADMIN:BULK_UPLOAD' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'bulk_upload', tokens: [] });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'برای آپلود گروهی، فایل‌ها را یکی یکی ارسال کنید. سپس می‌توانید نام/دسته هر مورد را تنظیم کنید.', reply_markup: { inline_keyboard: [
      [{ text: '🏷 تنظیم نام/دسته', callback_data: 'ADMIN:BULK_META' }],
      [{ text: '✅ پایان', callback_data: 'ADMIN:BULK_FINISH' }],
      [{ text: '❌ انصراف', callback_data: 'CANCEL' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:BULK_META' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'bulk_meta' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'JSON شامل آرایه‌ای از { token, name, category } ارسال کنید.', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:BULK_FINISH' && isAdmin(uid)) {
    const sess = await getSession(env, uid);
    const count = Array.isArray(sess.tokens) ? sess.tokens.length : 0;
    await setSession(env, uid, {});
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: `پایان آپلود گروهی. تعداد آیتم‌ها: ${count}` });
    return;
  }
  if (data === 'ADMIN:LOTTERY' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const cfg = await getLotteryConfig(env);
    const enabled = cfg.enabled ? 'فعال' : 'غیرفعال';
    const scheduleInfo = cfg.run_every_hours ? `\nبازه اجرا: هر ${cfg.run_every_hours} ساعت` : '';
    await tgApi('sendMessage', { chat_id: chatId, text: `🎟 قرعه‌کشی: ${enabled}\nبرندگان هر دوره: ${cfg.winners||0}\nجایزه: ${cfg.reward_diamonds||0} الماس${scheduleInfo}`, reply_markup: { inline_keyboard: [
      [{ text: cfg.enabled ? '🔴 غیرفعال‌سازی' : '🟢 فعال‌سازی', callback_data: 'ADMIN:LOT:TOGGLE' }],
      [{ text: '✏️ تنظیم مقادیر', callback_data: 'ADMIN:LOT:CONFIG' }],
      [{ text: '▶️ Start', callback_data: 'ADMIN:LOT:RUN_NOW' }],
      [{ text: '📜 تاریخچه', callback_data: 'ADMIN:LOT:HISTORY' }],
      [{ text: '🏠 منو', callback_data: 'MENU' }]
    ] } });
    return;
  }
  if (data === 'ADMIN:LOT:TOGGLE' && isAdmin(uid)) {
    const cfg = await getLotteryConfig(env);
    cfg.enabled = !cfg.enabled;
    await setLotteryConfig(env, cfg);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: cfg.enabled ? 'فعال شد' : 'غیرفعال شد' });
    return;
  }
  if (data === 'ADMIN:LOT:CONFIG' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'lottery_cfg:winners' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'تعداد برندگان را وارد کنید (عدد صحیح مثبت):', reply_markup: { inline_keyboard: [[{ text: '❌ انصراف', callback_data: 'CANCEL' }]] } });
    return;
  }
  if (data === 'ADMIN:LOT:RUN_NOW' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const dateKey = dayKey();
    const res = await runLotteryPickAndReward(env, dateKey);
    if (res.ok && res.winners && res.winners.length) {
      await tgApi('sendMessage', { chat_id: chatId, text: `✅ قرعه‌کشی اجرا شد. برندگان امروز (${dateKey}):\n${res.winners.map(w => `• ${w}`).join('\n')}` });
    } else {
      await tgApi('sendMessage', { chat_id: chatId, text: '⚠️ قرعه‌کشی اجرا نشد (احتمالاً غیرفعال است یا شرکت‌کننده‌ای وجود ندارد).' });
    }
    return;
  }
  if (data === 'ADMIN:LOT:HISTORY' && isAdmin(uid)) {
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    const hist = await getLotteryHistory(env, 10);
    const txt = hist.length ? hist.map(h => `${formatDate(h.at)} → winners: ${h.winners.join(', ')} (+${h.reward_diamonds})`).join('\n') : '—';
    await tgApi('sendMessage', { chat_id: chatId, text: `📜 تاریخچه قرعه‌کشی:\n${txt}` });
    return;
  }
  if (data === 'ADMIN:ADD_ADMIN' && isAdmin(uid)) {
    await setSession(env, uid, { awaiting: 'add_admin' });
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id });
    await tgApi('sendMessage', { chat_id: chatId, text: 'آی‌دی عددی کاربر را ارسال کنید تا به عنوان ادمین اضافه شود:' });
    return;
  }
  if (data.startsWith('ADMIN:DEL_ADMIN:') && isAdmin(uid)) {
    const removeId = Number(data.split(':')[2]);
    const admins = await getAdminIds(env);
    const next = admins.filter(id => id !== removeId);
    await setAdminIds(env, next);
    await tgApi('answerCallbackQuery', { callback_query_id: cb.id, text: 'حذف شد' });
    await tgApi('sendMessage', { chat_id: chatId, text: 'ادمین حذف شد.', reply_markup: await buildDynamicMainMenu(env, uid) });
    DYNAMIC_ADMIN_IDS = next.slice();
    return;
  }
}

/* -------------------- Document upload flow -------------------- */
async function onDocumentUpload(msg, env) {
  const doc = msg.document; const from = msg.from; const chatId = msg.chat.id;
  const fileId = doc.file_id; const fname = doc.file_name || 'config';

  const token = makeToken(18);
  const meta = {
    token,
    file_id: fileId,
    owner: from.id,
    name: fname,
    size: doc.file_size || 0,
    created_at: now(),
    downloads: 0,
    cost_points: 0,
    disabled: false
  };
  await kvPutJson(env, `file:${token}`, meta);

  // add to uploader index
  const upKey = `uploader:${from.id}`;
  const upList = (await kvGetJson(env, upKey)) || [];
  upList.push(token);
  await kvPutJson(env, upKey, upList);

  const botUsername = await getBotUsername(env);
  const link = botUsername 
    ? `https://t.me/${botUsername}?start=d_${token}`
    : `${domainFromWebhook()}/f/${token}`;
  await tgApi('sendMessage', { 
    chat_id: chatId, 
    text: `✅ فایل با موفقیت آپلود شد!\n\n📁 نام: ${fname}\n📏 حجم: ${formatFileSize(doc.file_size || 0)}\n\n🔐 توکن:\n\`${token}\`\n\n🔗 لینک اشتراک‌گذاری (باز می‌شود در ربات):\n\`${link}\`` ,
    parse_mode: 'Markdown'
  });
}

/* -------------------- Unified upload/store + delivery helpers -------------------- */
async function handleAnyUpload(msg, env, { ownerId, replaceToken, original } = {}) {
  // Supports: text, document, photo, video, audio, voice
  let meta = null;
  const base = {
    owner: ownerId,
    downloads: original?.downloads || 0,
    cost_points: original?.cost_points || 0,
    disabled: original?.disabled || false,
    created_at: original?.created_at || now()
  };

  if (msg.text) {
    const token = replaceToken || makeToken(18);
    meta = {
      token,
      type: 'text',
      text: msg.text,
      name: 'متن',
      size: (msg.text || '').length,
      ...base
    };
  } else if (msg.document) {
    const token = replaceToken || makeToken(18);
    meta = {
      token,
      type: 'document',
      file_id: msg.document.file_id,
      name: msg.document.file_name || 'document',
      size: msg.document.file_size || 0,
      ...base
    };
  } else if (msg.photo && msg.photo.length) {
    const token = replaceToken || makeToken(18);
    const p = msg.photo[msg.photo.length - 1];
    meta = {
      token,
      type: 'photo',
      file_id: p.file_id,
      name: 'photo',
      size: p.file_size || 0,
      ...base
    };
  } else if (msg.video) {
    const token = replaceToken || makeToken(18);
    meta = {
      token,
      type: 'video',
      file_id: msg.video.file_id,
      name: msg.video.file_name || 'video',
      size: msg.video.file_size || 0,
      ...base
    };
  } else if (msg.audio) {
    const token = replaceToken || makeToken(18);
    meta = {
      token,
      type: 'audio',
      file_id: msg.audio.file_id,
      name: msg.audio.title || 'audio',
      size: msg.audio.file_size || 0,
      ...base
    };
  } else if (msg.voice) {
    const token = replaceToken || makeToken(18);
    meta = {
      token,
      type: 'voice',
      file_id: msg.voice.file_id,
      name: 'voice',
      size: msg.voice.file_size || 0,
      ...base
    };
  }

  if (!meta) return null;

  // persist
  await kvPutJson(env, `file:${meta.token}`, meta);

  // If new item (not replace), add to uploader index
  if (!replaceToken) {
    const upKey = `uploader:${ownerId}`;
    const upList = (await kvGetJson(env, upKey)) || [];
    upList.push(meta.token);
    await kvPutJson(env, upKey, upList);
  }
  return meta;
}

async function deliverStoredContent(chatId, fileMeta) {
  const caption = `${fileMeta.name || fileMeta.type || 'item'}${fileMeta.size ? ' | ' + formatFileSize(fileMeta.size) : ''}`;
  switch (fileMeta.type) {
    case 'text':
      await tgApi('sendMessage', { chat_id: chatId, text: fileMeta.text || '' });
      break;
    case 'photo':
      await tgApi('sendPhoto', { chat_id: chatId, photo: fileMeta.file_id, caption });
      break;
    case 'video':
      await tgApi('sendVideo', { chat_id: chatId, video: fileMeta.file_id, caption });
      break;
    case 'audio':
      await tgApi('sendAudio', { chat_id: chatId, audio: fileMeta.file_id, caption });
      break;
    case 'voice':
      await tgApi('sendVoice', { chat_id: chatId, voice: fileMeta.file_id, caption });
      break;
    case 'document':
    default:
      await tgApi('sendDocument', { chat_id: chatId, document: fileMeta.file_id, caption });
      break;
  }
}

/* -------------------- File download handler -------------------- */
async function handleFileDownload(req, env, url) {
  const token = url.pathname.split('/f/')[1];
  if (!token) return new Response('Not Found', { status: 404 });
  const file = await kvGetJson(env, `file:${token}`);
  if (!file) return new Response('File Not Found', { status: 404 });

  // check service enabled
  const enabled = (await kvGetJson(env, 'bot:enabled')) ?? true;
  const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
  if (!enabled) return new Response('Service temporarily disabled', { status: 503 });
  if (updateMode) return new Response('Bot is currently in update mode. Please try again later.', { status: 503 });

  if (file.disabled) return new Response('File disabled by owner/admin', { status: 403 });

  // For web link, instead of redirect, provide inline bot deep-link to receive inside bot
  const botUsername = await getBotUsername(env);
  const deepLink = botUsername ? `https://t.me/${botUsername}?start=d_${token}` : '';
  const html = `<!doctype html><meta charset="utf-8"><body style="font-family:sans-serif;padding:2rem;">
  <h2>دریافت فایل داخل ربات</h2>
  <p>برای دریافت مستقیم فایل داخل تلگرام روی لینک زیر بزنید:</p>
  ${deepLink ? `<p><a href="${deepLink}">باز کردن در تلگرام</a></p>` : '<p>نام کاربری ربات در دسترس نیست.</p>'}
  </body>`;
  return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8' } });
}

/* -------------------- Main Page with Admin Panel -------------------- */
async function handleMainPage(req, env, url, ctx) {
  const key = url.searchParams.get('key');
  const adminKey = (RUNTIME.adminKey || ADMIN_KEY || '').trim();
  const isAuthenticated = key === adminKey;
  const action = url.searchParams.get('action');
  const op = url.searchParams.get('op');
  const targetId = url.searchParams.get('uid');

  // Handle toggle action via GET for convenience from admin panel button
  if (isAuthenticated && action === 'toggle') {
    const current = (await kvGetJson(env, 'bot:enabled')) ?? true;
    await kvPutJson(env, 'bot:enabled', !current);
    return Response.redirect(`/?key=${adminKey}`, 302);
  }

  // Admin action: setup webhook
  if (isAuthenticated && action === 'setup-webhook') {
    const setRes = await tgSetWebhook(RUNTIME.webhookUrl || WEBHOOK_URL);
    if (setRes && setRes.ok) {
      await kvPutJson(env, 'bot:webhook_set_at', now());
    }
    return Response.redirect(`/?key=${adminKey}`, 302);
  }
  // Admin action: toggle update mode
  if (isAuthenticated && action === 'toggle-update') {
    const current = (await kvGetJson(env, 'bot:update_mode')) || false;
    await kvPutJson(env, 'bot:update_mode', !current);
    return Response.redirect(`/?key=${adminKey}`, 302);
  }
  // Admin action: broadcast via GET (simple)
  if (isAuthenticated && action === 'broadcast') {
    const msg = url.searchParams.get('message') || '';
    if (msg.trim()) {
      // run broadcast in background to keep page responsive
      if (ctx) ctx.waitUntil(broadcast(env, msg.trim())); else await broadcast(env, msg.trim());
    }
    return Response.redirect(`/?key=${adminKey}`, 302);
  }
  // Admin view: full users list
  if (isAuthenticated && action === 'users') {
    const users = (await kvGetJson(env, 'index:users')) || [];
    const entries = await Promise.all(users.map(async (uid, idx) => {
      const u = (await kvGetJson(env, `user:${uid}`)) || { id: uid };
      const blocked = await isUserBlocked(env, uid);
      return {
        idx: idx + 1,
        id: uid,
        first_name: u.first_name || '-',
        username: u.username || '-',
        diamonds: u.diamonds || 0,
        referrals: u.referrals || 0,
        joined: u.joined ? '✅' : '—',
        created_at: u.created_at || 0,
        last_seen: u.last_seen || 0,
        referred_by: u.referred_by || '-',
        ref_credited: u.ref_credited ? '✅' : '—',
        blocked
      };
    }));

    const html = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>فهرست کامل کاربران</title>
  <style>
    :root { --glass-bg: rgba(255,255,255,.05); --glass-border: rgba(255,255,255,.1); --accent:#60a5fa; }
    body { margin:0; font-family: Segoe UI, Tahoma, Geneva, Verdana, sans-serif; background: linear-gradient(135deg,#0f172a,#1e293b,#334155); color:#f1f5f9; }
    .container{ max-width:1400px; margin:0 auto; padding:20px; }
    .header{ background:var(--glass-bg); border:1px solid var(--glass-border); border-radius:16px; padding:24px; margin:20px 0; backdrop-filter: blur(10px); box-shadow:0 10px 24px rgba(0,0,0,.25); }
    .header h2{ margin:0 0 8px 0; font-weight:700; font-size:1.8rem; color:var(--accent); }
    .actions{ display:flex; gap:10px; flex-wrap:wrap; margin-top:10px; }
    .btn{ display:inline-block; padding:10px 16px; border-radius:10px; background: linear-gradient(135deg,#3b82f6,#1d4ed8); color:white; text-decoration:none; border:none; cursor:pointer; box-shadow:0 8px 18px rgba(29,78,216,.35); }
    .btn:hover{ box-shadow:0 12px 24px rgba(59,130,246,.5); transform: translateY(-1px); }
    .search{ display:flex; gap:10px; margin-top:12px; }
    .search input{ flex:1; padding:12px; border-radius:10px; border:1px solid var(--glass-border); background: rgba(255,255,255,.08); color:white; }
    .table-wrap{ background:rgba(255,255,255,.03); border:1px solid var(--glass-border); border-radius:14px; overflow:hidden; box-shadow:0 10px 28px rgba(0,0,0,.25); }
    table{ width:100%; border-collapse:collapse; }
    thead{ position:sticky; top:0; background: rgba(255,255,255,.08); }
    th,td{ text-align:right; padding:12px 10px; border-bottom:1px solid rgba(255,255,255,.06); font-size:.95rem; }
    tbody tr:nth-child(even) td{ background: rgba(255,255,255,.03); }
    tbody tr:hover td{ background: rgba(255,255,255,.07); }
    .muted{ opacity:.8; }
    .status-badge{ padding:4px 10px; border-radius:12px; font-size:.85rem; border:1px solid var(--glass-border); }
    .ok{ background: rgba(34,197,94,.2); color:#22c55e; border-color: rgba(34,197,94,.3); }
    .bad{ background: rgba(239,68,68,.2); color:#ef4444; border-color: rgba(239,68,68,.3); }
    .table-scroller{ max-height:70vh; overflow:auto; }
    code{ opacity:.8; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h2>👥 فهرست کامل کاربران</h2>
      <div class="muted">تعداد کاربران: ${entries.length.toLocaleString('fa-IR')}</div>
      <div class="actions">
        <a class="btn" href="/?key=${adminKey}">⬅️ بازگشت به پنل</a>
      </div>
      <div class="search">
        <input id="q" type="text" placeholder="جستجو بر اساس آی‌دی، نام یا یوزرنیم..." />
        <button class="btn" id="clearBtn">پاک کردن</button>
      </div>
    </div>
    <div class="table-wrap">
      <div class="table-scroller">
        <table id="usersTbl">
          <thead>
            <tr>
              <th>#</th>
              <th>آی‌دی</th>
              <th>نام</th>
              <th>یوزرنیم</th>
              <th>الماس</th>
              <th>معرفی‌ها</th>
              <th>عضویت</th>
              <th>ارجاع از</th>
              <th>ارجاع ثبت شده</th>
              <th>آخرین فعالیت</th>
              <th>تاریخ عضویت</th>
              <th>وضعیت</th>
              <th>اقدام</th>
            </tr>
          </thead>
          <tbody>
            ${entries.map(e => `
              <tr>
                <td>${e.idx}</td>
                <td><code>${e.id}</code></td>
                <td>${escapeHtml(e.first_name)}</td>
                <td>${escapeHtml(e.username)}</td>
                <td>${(e.diamonds||0).toLocaleString('fa-IR')}</td>
                <td>${(e.referrals||0).toLocaleString('fa-IR')}</td>
                <td>${e.joined}</td>
                <td>${e.referred_by}</td>
                <td>${e.ref_credited}</td>
                <td data-ts="${e.last_seen}">-</td>
                <td data-tsc="${e.created_at}">-</td>
                <td><span class="status-badge ${e.blocked ? 'bad' : 'ok'}">${e.blocked ? '⛔️ مسدود' : '🟢 فعال'}</span></td>
                <td>
                  ${e.blocked 
                    ? `<a class="btn" href="/?key=${adminKey}&op=unblock&uid=${e.id}">آنبلاک</a>`
                    : `<a class="btn" href="/?key=${adminKey}&op=block&uid=${e.id}">Block</a>`}
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <script>
    // Humanize timestamps
    function human(ts){ try{ const n = Number(ts)||0; if(!n) return '-'; return new Date(n).toLocaleString('fa-IR'); }catch(_){ return '-'; } }
    // Fill timestamps from data attributes
    document.querySelectorAll('#usersTbl tbody tr').forEach(tr => {
      const lastTd = tr.querySelector('td[data-ts]');
      const createdTd = tr.querySelector('td[data-tsc]');
      if (lastTd) lastTd.textContent = human(lastTd.getAttribute('data-ts'));
      if (createdTd) createdTd.textContent = human(createdTd.getAttribute('data-tsc'));
    });
    // Filter
    const q = document.getElementById('q');
    const clearBtn = document.getElementById('clearBtn');
    function filter(){
      const term = (q.value||'').toLowerCase();
      document.querySelectorAll('#usersTbl tbody tr').forEach(tr => {
        const text = tr.textContent.toLowerCase();
        tr.style.display = text.includes(term) ? '' : 'none';
      });
    }
    q.addEventListener('input', filter);
    clearBtn.addEventListener('click', ()=>{ q.value=''; filter(); });
  </script>
</body>
</html>`;

    return new Response(html, { headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' } });
  }
  // Admin actions: block/unblock via GET
  if (isAuthenticated && op && targetId) {
    const tid = Number(targetId);
    if (Number.isFinite(tid)) {
      if (op === 'block') { await blockUser(env, tid); }
      if (op === 'unblock') { await unblockUser(env, tid); }
    }
    return Response.redirect(`/?key=${adminKey}`, 302);
  }
  
  // Get basic stats for public view
  const users = (await kvGetJson(env, 'index:users')) || [];
  const userCount = users.length;
  
  let files = [];
  let totalDownloads = 0;
  
  // Collect files from all uploaders
  for (const uid of users.slice(0, 100)) { // Limit for performance
    const list = (await kvGetJson(env, `uploader:${uid}`)) || [];
    for (const t of list) {
      const f = await kvGetJson(env, `file:${t}`);
      if (f) {
        files.push(f);
        totalDownloads += f.downloads || 0;
      }
    }
  }
  
  const fileCount = files.length;
  const enabled = (await kvGetJson(env, 'bot:enabled')) ?? true;
  const lastWebhookAt = (await kvGetJson(env, 'bot:last_webhook')) || 0;
  const connected = typeof lastWebhookAt === 'number' && (now() - lastWebhookAt) < 5 * 60 * 1000;

  const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
  const html = `<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${isAuthenticated ? 'پنل مدیریت' : 'WireGuard Bot'}</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        :root {
            --bg-start: #0f172a;
            --bg-mid: #1e293b;
            --bg-end: #334155;
            --glass-bg: rgba(255, 255, 255, 0.05);
            --glass-border: rgba(255, 255, 255, 0.1);
            --text-muted: #cbd5e1;
            --accent: #60a5fa;
            --accent2: #34d399;
            --accent3: #fbbf24;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, var(--bg-start) 0%, var(--bg-mid) 50%, var(--bg-end) 100%);
            background-size: 200% 200%;
            animation: gradientShift 12s ease infinite;
            min-height: 100vh;
            color: #f1f5f9;
            overflow-x: hidden;
        }

        @keyframes gradientShift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            text-align: center;
            margin-bottom: 40px;
            padding: 40px 20px;
            background: var(--glass-bg);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            border: 1px solid var(--glass-border);
            position: relative;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
        }

        .header::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: linear-gradient(45deg, transparent 49%, rgba(255, 255, 255, 0.03) 50%, transparent 51%);
            pointer-events: none;
        }

        .header h1 {
            font-size: 3rem;
            font-weight: 700;
            background: linear-gradient(135deg, var(--accent), var(--accent2), var(--accent3));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 10px;
            text-shadow: 0 2px 20px rgba(96, 165, 250, 0.15);
        }

        .header p {
            font-size: 1.2rem;
            opacity: 0.8;
            max-width: 600px;
            margin: 0 auto;
        }

        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }

        .stat-card {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(12px);
            padding: 30px;
            border-radius: 15px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            text-align: center;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
        }

        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255, 255, 255, 0.1), transparent);
            transition: left 0.5s;
        }

        .stat-card:hover::before {
            left: 100%;
        }

        .stat-card:hover {
            transform: translateY(-5px);
            border-color: rgba(96, 165, 250, 0.35);
            box-shadow: 0 25px 50px rgba(0, 0, 0, 0.35), 0 0 0 1px rgba(96,165,250,0.2) inset;
        }

        .stat-icon {
            font-size: 3rem;
            margin-bottom: 15px;
            display: block;
        }

        .stat-value {
            font-size: 2.5rem;
            font-weight: 700;
            margin-bottom: 10px;
            color: #60a5fa;
        }

        .stat-label {
            font-size: 1.1rem;
            opacity: 0.8;
        }

        ${isAuthenticated ? `
        .admin-panel {
            background: var(--glass-bg);
            backdrop-filter: blur(15px);
            border-radius: 20px;
            border: 1px solid var(--glass-border);
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 12px 30px rgba(0,0,0,0.28);
        }

        .admin-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            flex-wrap: wrap;
            gap: 20px;
        }

        .admin-title {
            font-size: 1.8rem;
            font-weight: 600;
            color: #fbbf24;
        }

        .service-status {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 20px;
            border-radius: 10px;
            background: rgba(255, 255, 255, 0.1);
            font-weight: 500;
        }

        .status-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            background: ${enabled ? '#22c55e' : '#ef4444'};
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }

        .btn {
            display: inline-block;
            padding: 12px 24px;
            border-radius: 10px;
            background: linear-gradient(135deg, #3b82f6, #1d4ed8);
            color: white;
            text-decoration: none;
            font-weight: 500;
            transition: all 0.3s ease;
            border: none;
            cursor: pointer;
            box-shadow: 0 8px 18px rgba(29, 78, 216, 0.35);
            letter-spacing: 0.2px;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 12px 24px rgba(59, 130, 246, 0.5);
        }

        .btn-danger {
            background: linear-gradient(135deg, #ef4444, #dc2626);
        }

        .btn-success {
            background: linear-gradient(135deg, #22c55e, #16a34a);
        }

        .data-table {
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            overflow: hidden;
            margin-bottom: 30px;
            box-shadow: 0 10px 28px rgba(0,0,0,0.25);
        }

        .table-header {
            background: rgba(255, 255, 255, 0.1);
            padding: 20px;
            font-size: 1.3rem;
            font-weight: 700;
            color: var(--accent);
            position: relative;
        }

        .table-header::after {
            content: '';
            position: absolute;
            left: 20px;
            right: 20px;
            bottom: 8px;
            height: 2px;
            background: linear-gradient(90deg, rgba(96,165,250,.6), rgba(52,211,153,.6), rgba(251,191,36,.6));
            border-radius: 2px;
        }

        .table-content {
            max-height: 400px;
            overflow-y: auto;
        }

        .table-content::-webkit-scrollbar {
            width: 8px;
        }

        .table-content::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.1);
        }

        .table-content::-webkit-scrollbar-thumb {
            background: rgba(96, 165, 250, 0.5);
            border-radius: 4px;
        }

        table {
            width: 100%;
            border-collapse: collapse;
        }

        th, td {
            text-align: right;
            padding: 15px;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
        }

        th {
            background: rgba(255, 255, 255, 0.05);
            font-weight: 600;
            color: #cbd5e1;
            position: sticky;
            top: 0;
        }

        td {
            transition: all 0.3s ease;
        }

        tbody tr:nth-child(even) td { background: rgba(255, 255, 255, 0.03); }
        tbody tr:hover td { background: rgba(255, 255, 255, 0.07); }

        .file-name {
            max-width: 200px;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }

        .status-badge {
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 0.9rem;
            font-weight: 500;
        }

        .status-active {
            background: rgba(34, 197, 94, 0.2);
            color: #22c55e;
            border: 1px solid rgba(34, 197, 94, 0.3);
        }

        .status-disabled {
            background: rgba(239, 68, 68, 0.2);
            color: #ef4444;
            border: 1px solid rgba(239, 68, 68, 0.3);
        }
        ` : ''}

        .auth-form {
            max-width: 400px;
            margin: 40px auto;
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(15px);
            padding: 40px;
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
        }

        .auth-form h2 {
            text-align: center;
            margin-bottom: 30px;
            color: #fbbf24;
        }

        .form-group {
            margin-bottom: 20px;
        }

        .form-group label {
            display: block;
            margin-bottom: 8px;
            color: #cbd5e1;
        }

        .form-group input {
            width: 100%;
            padding: 15px;
            border-radius: 10px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            background: rgba(255, 255, 255, 0.1);
            color: white;
            font-size: 1rem;
        }

        .form-group input:focus {
            outline: none;
            border-color: #60a5fa;
            box-shadow: 0 0 0 3px rgba(96, 165, 250, 0.2);
        }

        @media (max-width: 768px) {
            .header h1 { font-size: 2rem; }
            .container { padding: 15px; }
            .stats-grid { grid-template-columns: 1fr; }
            .admin-header { flex-direction: column; text-align: center; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🤖 WireGuard Bot</h1>
            <p>سیستم مدیریت و اشتراک‌گذاری فایل‌های WireGuard با امکانات پیشرفته</p>
        </div>

        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-icon">👥</span>
                <div class="stat-value">${userCount.toLocaleString('fa-IR')}</div>
                <div class="stat-label">کاربران ثبت شده</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">📁</span>
                <div class="stat-value">${fileCount.toLocaleString('fa-IR')}</div>
                <div class="stat-label">فایل‌های آپلود شده</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">📥</span>
                <div class="stat-value">${totalDownloads.toLocaleString('fa-IR')}</div>
                <div class="stat-label">کل دانلودها</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">${enabled ? '🟢' : '🔴'}</span>
                <div class="stat-value">${enabled ? 'فعال' : 'غیرفعال'}</div>
                <div class="stat-label">وضعیت سرویس</div>
            </div>
            <div class="stat-card">
                <span class="stat-icon">${connected ? '🔌' : '⚠️'}</span>
                <div class="stat-value">${connected ? 'آنلاین' : 'آفلاین'}</div>
                <div class="stat-label">اتصال وبهوک ${lastWebhookAt ? '(' + formatDate(lastWebhookAt) + ')' : ''}</div>
            </div>
        </div>

        ${!isAuthenticated ? `
        <div class="auth-form">
            <h2>🔐 ورود به پنل مدیریت</h2>
            <form method="GET">
                <div class="form-group">
                    <label for="key">کلید دسترسی:</label>
                    <input type="password" id="key" name="key" placeholder="کلید مدیریت را وارد کنید" required>
                </div>
                <button type="submit" class="btn" style="width: 100%;">ورود به پنل</button>
            </form>
        </div>
        ` : `
        <div class="admin-panel">
            <div class="admin-header">
                <div class="admin-title">🛠 پنل مدیریت</div>
                <div class="service-status">
                    <div class="status-dot"></div>
                    <span>سرویس ${enabled ? 'فعال' : 'غیرفعال'}</span>
                    <a href="/?key=${adminKey}&action=toggle" class="btn ${enabled ? 'btn-danger' : 'btn-success'}" style="margin-right: 15px;">
                        ${enabled ? 'غیرفعال کردن' : 'فعال کردن'}
                    </a>
                    <a href="/?key=${adminKey}&action=setup-webhook" class="btn" style="margin-right: 10px;">
                        راه‌اندازی وبهوک
                    </a>
                    <a href="/?key=${adminKey}&action=toggle-update" class="btn" style="margin-right: 10px;">
                        ${updateMode ? 'خاموش کردن حالت آپدیت' : 'روشن کردن حالت آپدیت'}
                    </a>
                </div>
            </div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px;">
              <a class="btn" href="/?key=${adminKey}&action=users">👥 فهرست کامل کاربران</a>
            </div>
            <div style="display:flex; gap:12px; flex-wrap:wrap; margin-bottom:16px;">
              <a class="btn" href="https://t.me/${await getBotUsername(env) || ''}" target="_blank">باز کردن ربات در تلگرام</a>
              <a class="btn" href="/?key=${adminKey}&action=toggle">${enabled ? '⛔️ توقف سرویس' : '▶️ شروع سرویس'}</a>
            </div>
            <div class="data-table" style="margin-top:10px;">
              <div class="table-header">🔧 مدیریت کاربران (Block/آنبلاک)</div>
              <div class="table-content">
                <table>
                  <thead>
                    <tr>
                      <th>آی‌دی</th>
                      <th>یوزرنیم</th>
                     <th>الماس</th>
                      <th>وضعیت</th>
                      <th>اقدام</th>
                    </tr>
                  </thead>
                  <tbody>
                    ${(await Promise.all(users.slice(0, 30).map(async uid => {
                      const u = await kvGetJson(env, `user:${uid}`) || {}; const blocked = await isUserBlocked(env, uid);
                      return `
                        <tr>
                          <td>${uid}</td>
                          <td>${escapeHtml(u.username || '-')}</td>
                          <td>${(u.diamonds||0).toLocaleString('fa-IR')}</td>
                          <td>${blocked ? '⛔️ مسدود' : '🟢 فعال'}</td>
                          <td>
                            ${blocked 
                              ? `<a class="btn btn-success" href="/?key=${adminKey}&op=unblock&uid=${uid}">آنبلاک</a>`
                              : `<a class="btn btn-danger" href="/?key=${adminKey}&op=block&uid=${uid}">Block</a>`}
                          </td>
                        </tr>
                      `;
                    }))).join('')}
                  </tbody>
                </table>
              </div>
            </div>
            <div class="data-table" style="margin-top:10px;">
              <div class="table-header">📢 ارسال اعلان به همه کاربران</div>
              <div style="padding:16px;">
                <form method="GET" action="/?">
                  <input type="hidden" name="key" value="${adminKey}" />
                  <input type="hidden" name="action" value="broadcast" />
                  <div style="display:flex; gap:8px;">
                    <input type="text" name="message" placeholder="متن پیام" style="flex:1; padding:12px; border-radius:10px; border:1px solid rgba(255,255,255,.2); background:rgba(255,255,255,.08); color:white;" />
                    <button class="btn" type="submit">ارسال</button>
                  </div>
                </form>
              </div>
            </div>
        </div>

        <div class="data-table">
            <div class="table-header">📂 فایل‌های اخیر (${Math.min(files.length, 50)} از ${fileCount})</div>
            <div class="table-content">
                <table>
                    <thead>
                        <tr>
                            <th>نام فایل</th>
                            <th>مالک</th>
                            <th>حجم</th>
                            <th>دانلود</th>
                            <th>هزینه</th>
                            <th>تاریخ ایجاد</th>
                            <th>وضعیت</th>
                            <th>توکن</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${files.slice(0, 50).sort((a, b) => (b.created_at || 0) - (a.created_at || 0)).map(f => `
                        <tr>
                            <td class="file-name" title="${escapeHtml(f.name || 'file')}">${escapeHtml(f.name || 'file')}</td>
                            <td>${f.owner}</td>
                            <td>${formatFileSize(f.size || 0)}</td>
                            <td>${(f.downloads || 0).toLocaleString('fa-IR')}</td>
                            <td>${(f.cost_points || 0).toLocaleString('fa-IR')}</td>
                            <td>${formatDate(f.created_at || 0)}</td>
                            <td>
                                <span class="status-badge ${f.disabled ? 'status-disabled' : 'status-active'}">
                                    ${f.disabled ? '🔴 غیرفعال' : '🟢 فعال'}
                                </span>
                            </td>
                            <td><code style="font-size: 0.8rem; opacity: 0.7;">${f.token}</code></td>
                        </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </div>

        <div class="data-table">
            <div class="table-header">👥 کاربران اخیر (${Math.min(users.length, 30)} از ${userCount})</div>
            <div class="table-content">
                <table>
                    <thead>
                        <tr>
                            <th>آی‌دی</th>
                            <th>نام</th>
                            <th>یوزرنیم</th>
                           <th>الماس</th>
                            <th>معرفی‌ها</th>
                            <th>آخرین فعالیت</th>
                            <th>تاریخ عضویت</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${(await Promise.all(users.slice(0, 30).map(async uid => {
                            const user = await kvGetJson(env, `user:${uid}`) || {};
                            return `
                            <tr>
                                <td>${uid}</td>
                                <td>${escapeHtml(user.first_name || '-')}</td>
                                <td>${escapeHtml(user.username || '-')}</td>
                                <td>${(user.diamonds || 0).toLocaleString('fa-IR')}</td>
                                <td>${(user.referrals || 0).toLocaleString('fa-IR')}</td>
                                <td>${user.last_seen ? formatDate(user.last_seen) : '-'}</td>
                                <td>${formatDate(user.created_at || 0)}</td>
                            </tr>
                            `;
                        }))).join('')}
                    </tbody>
                </table>
            </div>
        </div>
        `}

        <div style="text-align: center; margin-top: 40px; opacity: 0.7;">
            <p>🤖 Telegram WireGuard Bot - نسخه پیشرفته</p>
            <p>ساخته شده با ❤️ برای مدیریت بهتر فایل‌ها</p>
        </div>
    </div>

    <script>
        // Auto refresh every 30 seconds for admin panel
        ${isAuthenticated ? `
        let refreshInterval;
        function startAutoRefresh() {
            refreshInterval = setInterval(() => {
                window.location.reload();
            }, 30000);
        }
        
        // Stop refresh when page is not visible
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                clearInterval(refreshInterval);
            } else {
                startAutoRefresh();
            }
        });
        
        startAutoRefresh();
        ` : ''}

        // Add loading animation to buttons
        document.querySelectorAll('.btn').forEach(btn => {
            btn.addEventListener('click', function() {
                if (this.type !== 'submit') return;
                this.style.opacity = '0.7';
                this.innerHTML = 'در حال پردازش...';
            });
        });

        // Add smooth scroll animation
        document.documentElement.style.scrollBehavior = 'smooth';
    </script>
</body>
</html>`;

  return new Response(html, { 
    headers: { 
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-cache, no-store, must-revalidate'
    } 
  });
}

/* -------------------- API handlers for admin operations -------------------- */
async function handleApiRequest(req, env, url, ctx) {
  const key = url.searchParams.get('key');
  const adminKey = RUNTIME.adminKey || ADMIN_KEY;
  if (!key || key !== adminKey) {
    return new Response(JSON.stringify({ error: 'Unauthorized' }), { 
      status: 401, 
      headers: { 'Content-Type': 'application/json' } 
    });
  }

  const path = url.pathname.replace('/api/', '');
  
  if (path === 'toggle-service' && req.method === 'POST') {
    const current = (await kvGetJson(env, 'bot:enabled')) ?? true;
    await kvPutJson(env, 'bot:enabled', !current);
    return new Response(JSON.stringify({ enabled: !current }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  if (path === 'stats') {
    const users = (await kvGetJson(env, 'index:users')) || [];
    const userCount = users.length;
    
    let files = [];
    let totalDownloads = 0;
    
    for (const uid of users.slice(0, 100)) {
      const list = (await kvGetJson(env, `uploader:${uid}`)) || [];
      for (const t of list) {
        const f = await kvGetJson(env, `file:${t}`);
        if (f) {
          files.push(f);
          totalDownloads += f.downloads || 0;
        }
      }
    }
    
    return new Response(JSON.stringify({
      users: userCount,
      files: files.length,
      downloads: totalDownloads,
      enabled: (await kvGetJson(env, 'bot:enabled')) ?? true
    }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }

  if (path === 'backup' && req.method === 'GET') {
    const backup = await createKvBackup(env);
    return new Response(JSON.stringify(backup), { headers: { 'Content-Type': 'application/json' } });
  }

  // Block/unblock via API
  if (path === 'block' && req.method === 'POST') {
    const { uid } = await req.json().catch(() => ({ uid: null }));
    if (!Number.isFinite(Number(uid))) return new Response(JSON.stringify({ ok: false, error: 'bad uid' }), { headers: { 'Content-Type': 'application/json' }, status: 400 });
    await blockUser(env, Number(uid));
    return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
  }
  if (path === 'unblock' && req.method === 'POST') {
    const { uid } = await req.json().catch(() => ({ uid: null }));
    if (!Number.isFinite(Number(uid))) return new Response(JSON.stringify({ ok: false, error: 'bad uid' }), { headers: { 'Content-Type': 'application/json' }, status: 400 });
    await unblockUser(env, Number(uid));
    return new Response(JSON.stringify({ ok: true }), { headers: { 'Content-Type': 'application/json' } });
  }

  return new Response(JSON.stringify({ error: 'Not Found' }), { 
    status: 404, 
    headers: { 'Content-Type': 'application/json' } 
  });
}

/* -------------------- Admin utilities -------------------- */
async function createKvBackup(env) {
  const users = (await kvGetJson(env, 'index:users')) || [];
  const admins = await getAdminIds(env);
  const botSettings = await getSettings(env);
  const missionsIndex = (await kvGetJson(env, 'missions:index')) || [];
  const giftsIndex = (await kvGetJson(env, 'gift:index')) || [];
  const lotteryCfg = await getLotteryConfig(env);
  const ticketsIdx = (await kvGetJson(env, await ticketsIndexKey())) || [];
  const data = {
    meta: { created_at: now() },
    admins,
    settings: botSettings,
    users: [],
    files: [],
    missions: [],
    gifts: [],
    lottery: lotteryCfg,
    tickets: []
  };
  for (const uid of users) {
    const user = (await kvGetJson(env, `user:${uid}`)) || { id: uid };
    data.users.push(user);
    const list = (await kvGetJson(env, `uploader:${uid}`)) || [];
    for (const t of list) {
      const f = await kvGetJson(env, `file:${t}`);
      if (f) data.files.push(f);
    }
  }
  for (const mid of missionsIndex) {
    const m = await kvGetJson(env, `mission:${mid}`);
    if (m) data.missions.push(m);
  }
  for (const code of giftsIndex) {
    const g = await kvGetJson(env, `gift:${code}`);
    if (g) data.gifts.push(g);
  }
  for (const tid of ticketsIdx) {
    const t = await getTicket(env, tid);
    const msgs = await getTicketMessages(env, tid, 200);
    if (t) data.tickets.push({ ...t, messages: msgs });
  }
  return data;
}
function isAdmin(uid) { 
  const id = Number(uid);
  if (DYNAMIC_ADMIN_IDS && DYNAMIC_ADMIN_IDS.length) return DYNAMIC_ADMIN_IDS.includes(id);
  if (RUNTIME.adminIds && RUNTIME.adminIds.length) return RUNTIME.adminIds.includes(id);
  return (Array.isArray(ADMIN_IDS) ? ADMIN_IDS : []).includes(id);
}

async function isUserBlocked(env, uid) {
  const v = await kvGetJson(env, `block:${uid}`);
  return !!(v && v.blocked);
}
async function blockUser(env, uid) {
  return kvPutJson(env, `block:${uid}`, { blocked: true, at: now() });
}
async function unblockUser(env, uid) {
  return kvPutJson(env, `block:${uid}`, { blocked: false, at: now() });
}

async function broadcast(env, message) {
  const users = (await kvGetJson(env, 'index:users')) || [];
  let successful = 0;
  let failed = 0;
  
  for (const u of users) {
    try { 
      await tgApi('sendMessage', { chat_id: u, text: message }); 
      successful++;
      // Add small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 50));
    } catch (e) { 
      failed++;
    }
  }
  
  return { successful, failed };
}

function escapeHtml(s) { 
  if (!s) return ''; 
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

/* -------------------- Tickets storage & helpers -------------------- */
async function ticketsIndexKey() { return 'tickets:index'; }
function newTicketId() {
  // Simple, readable ticket id: one letter + digits, e.g., p123456789
  const prefix = 'p';
  const digits = String(Math.floor(100000000 + Math.random() * 900000000)); // 9 digits
  return `${prefix}${digits}`;
}
async function listTickets(env, { limit = 20 } = {}) {
  const idx = (await kvGetJson(env, await ticketsIndexKey())) || [];
  const res = [];
  for (const id of idx.slice(0, limit)) {
    const t = await kvGetJson(env, `ticket:${id}`);
    if (t) res.push(t);
  }
  return res;
}
async function listUserTickets(env, uid, { limit = 20 } = {}) {
  const idx = (await kvGetJson(env, `tickets:user:${uid}`)) || [];
  const res = [];
  for (const id of idx.slice(0, limit)) {
    const t = await kvGetJson(env, `ticket:${id}`);
    if (t) res.push(t);
  }
  return res;
}
async function getTicket(env, id) { return await kvGetJson(env, `ticket:${id}`); }
async function putTicket(env, meta) {
  const next = { ...meta, updated_at: now() };
  return await kvPutJson(env, `ticket:${meta.id}`, next);
}
async function deleteTicket(env, id) {
  const t = await getTicket(env, id);
  await kvDelete(env, `ticket:${id}`);
  await kvDelete(env, `ticket:${id}:messages`);
  // remove from indexes
  const idx = (await kvGetJson(env, await ticketsIndexKey())) || [];
  await kvPutJson(env, await ticketsIndexKey(), idx.filter(x => x !== id));
  if (t) {
    const uidx = (await kvGetJson(env, `tickets:user:${t.user_id}`)) || [];
    await kvPutJson(env, `tickets:user:${t.user_id}`, uidx.filter(x => x !== id));
  }
}
async function getTicketMessages(env, id, limit = 50) {
  const list = (await kvGetJson(env, `ticket:${id}:messages`)) || [];
  return list.slice(-limit);
}
async function appendTicketMessage(env, id, message) {
  const list = (await kvGetJson(env, `ticket:${id}:messages`)) || [];
  list.push({ ...message });
  await kvPutJson(env, `ticket:${id}:messages`, list.slice(-200)); // keep last 200 messages
  // also update ticket meta timestamps and last message info
  const meta = await getTicket(env, id);
  if (meta) {
    meta.updated_at = now();
    meta.last_message_from = message.from;
    await kvPutJson(env, `ticket:${id}`, meta);
  }
}
async function createTicket(env, { user_id, username, category, subject, desc }) {
  const id = newTicketId();
  const meta = { id, user_id, username: username || null, category: category || '-', subject: subject || '-', desc: desc || '', status: 'open', created_at: now(), updated_at: now() };
  // indexes
  const idx = (await kvGetJson(env, await ticketsIndexKey())) || [];
  idx.unshift(id);
  await kvPutJson(env, await ticketsIndexKey(), idx);
  const uidx = (await kvGetJson(env, `tickets:user:${user_id}`)) || [];
  uidx.unshift(id);
  await kvPutJson(env, `tickets:user:${user_id}`, uidx);
  await kvPutJson(env, `ticket:${id}`, meta);
  await appendTicketMessage(env, id, { from: 'user', by: user_id, at: now(), text: desc });
  return meta;
}

/* -------------------- In-bot download helper -------------------- */
async function handleBotDownload(env, uid, chatId, token, ref) {
  if (!isValidTokenFormat(token)) { await tgApi('sendMessage', { chat_id: chatId, text: 'توکن نامعتبر' }); return; }
  const file = await kvGetJson(env, `file:${token}`);
  if (!file) { await tgApi('sendMessage', { chat_id: chatId, text: 'فایل یافت نشد' }); return; }

  // service and disabled checks
  const enabled = (await kvGetJson(env, 'bot:enabled')) ?? true;
  const updateMode = (await kvGetJson(env, 'bot:update_mode')) || false;
  if (!enabled) { await tgApi('sendMessage', { chat_id: chatId, text: 'سرویس موقتا غیرفعال است' }); return; }
  if (updateMode && !isAdmin(uid)) { await tgApi('sendMessage', { chat_id: chatId, text: '🔧 ربات در حال بروزرسانی است. لطفاً دقایقی دیگر مجدداً تلاش کنید.' }); return; }
  if (file.disabled) { await tgApi('sendMessage', { chat_id: chatId, text: 'فایل توسط مالک/ادمین غیرفعال شده است' }); return; }

  // per-file download limit enforcement
  if ((file.max_downloads || 0) > 0 && (file.downloads || 0) >= file.max_downloads) {
    await tgApi('sendMessage', { chat_id: chatId, text: '⛔️ ظرفیت دانلود این فایل به پایان رسیده است.' });
    // delete if flagged
    if (file.delete_on_limit) {
      try {
        const upKey = `uploader:${file.owner}`;
        const upList = (await kvGetJson(env, upKey)) || [];
        await kvPutJson(env, upKey, upList.filter(t => t !== token));
        await kvDelete(env, `file:${token}`);
      } catch (_) {}
    }
    return;
  }

  // enforce required channel membership for non-admins
  const req = await getRequiredChannels(env);
  if (req.length && !(await isUserJoinedAllRequiredChannels(env, uid))) {
    await presentJoinPrompt(env, chatId);
    return;
  }

  // cost handling
  if ((file.cost_points || 0) > 0) {
    // daily limit enforcement (if set)
    const settings = await getSettings(env);
    const limit = settings.daily_limit || 0;
    if (limit > 0 && !isAdmin(uid)) {
      const dk = `usage:${uid}:${dayKey()}`;
      const used = (await kvGetJson(env, dk)) || { count: 0 };
      if ((used.count || 0) >= limit) {
        await tgApi('sendMessage', { chat_id: chatId, text: `به سقف روزانه استفاده (${limit}) رسیده‌اید.` });
        return;
      }
    }
  const user = (await kvGetJson(env, `user:${uid}`)) || { diamonds: 0 };
  const needed = file.cost_points || 0;
  if ((user.diamonds || 0) < needed) {
  const botUsername = await getBotUsername(env);
  const refLink = botUsername ? `https://t.me/${botUsername}?start=${uid}` : '';
    await tgApi('sendMessage', { chat_id: chatId, text: `⚠️ الماس کافی ندارید. نیاز: ${needed} | الماس شما: ${user.diamonds||0}${refLink ? `\nبرای کسب الماس لینک معرفی شما:\n${refLink}` : ''}` });
      return;
    }
    const ok = await checkRateLimit(env, uid, 'confirm_spend', 3, 60_000);
    if (!ok) { await tgApi('sendMessage', { chat_id: chatId, text: 'تعداد درخواست بیش از حد. لطفاً بعداً تلاش کنید.' }); return; }
    await setSession(env, uid, { awaiting: `confirm_spend:${token}:${needed}:${ref||''}` });
  await tgApi('sendMessage', { chat_id: chatId, text: `این فایل ${needed} الماس هزینه دارد. مایل به پرداخت هستید؟`, reply_markup: { inline_keyboard: [
      [{ text: '✅ بله، پرداخت و دریافت', callback_data: `CONFIRM_SPEND:${token}:${needed}:${ref||''}` }],
      [{ text: '❌ خیر، بازگشت به منو', callback_data: 'MENU' }]
    ] } });
    return;
  }

  // referral credit
  if (ref && String(ref) !== String(file.owner)) {
    const currentUser = (await kvGetJson(env, `user:${uid}`)) || { id: uid };
    if (!currentUser.ref_credited) {
      const refUser = (await kvGetJson(env, `user:${ref}`)) || null;
      if (refUser) {
        refUser.diamonds = (refUser.diamonds || 0) + 1;
        refUser.referrals = (refUser.referrals || 0) + 1;
        await kvPutJson(env, `user:${ref}`, refUser);
        currentUser.ref_credited = true;
        currentUser.referred_by = currentUser.referred_by || Number(ref);
        await kvPutJson(env, `user:${uid}`, currentUser);
         await tgApi('sendMessage', { chat_id: Number(ref), text: '🎉 یک الماس بابت معرفی دریافت کردید.' });
      }
    }
  }

  // deliver based on type
  await deliverStoredContent(chatId, file);

  // stats + usage increment
  file.downloads = (file.downloads || 0) + 1; file.last_download = now();
  await kvPutJson(env, `file:${token}`, file);

  // if reached limit after increment, optionally delete
  if ((file.max_downloads || 0) > 0 && (file.downloads || 0) >= file.max_downloads && file.delete_on_limit) {
    try {
      const upKey = `uploader:${file.owner}`;
      const upList = (await kvGetJson(env, upKey)) || [];
      await kvPutJson(env, upKey, upList.filter(t => t !== token));
      await kvDelete(env, `file:${token}`);
    } catch (_) {}
  }
  // increase usage counter if daily limit is set
  const settings = await getSettings(env);
  if ((settings.daily_limit || 0) > 0 && !isAdmin(uid)) {
    const dk = `usage:${uid}:${dayKey()}`;
    const used = (await kvGetJson(env, dk)) || { count: 0 };
    used.count = (used.count || 0) + 1;
    await kvPutJson(env, dk, used);
  }
}

/* -------------------- Missions: storage, view, progress -------------------- */
async function listMissions(env) {
  const idx = (await kvGetJson(env, 'missions:index')) || [];
  const res = [];
  for (const id of idx) {
    const m = await kvGetJson(env, `mission:${id}`);
    if (m) res.push(m);
  }
  return res;
}
async function createMission(env, { title, reward, period, type = 'generic', config = {} }) {
  const id = `m_${makeToken(8)}`;
  const m = { id, title, reward: Number(reward)||0, period: period||'once', created_at: now(), enabled: true, type, config };
  const idx = (await kvGetJson(env, 'missions:index')) || [];
  idx.unshift(id);
  await kvPutJson(env, 'missions:index', idx);
  await kvPutJson(env, `mission:${id}`, m);
  return { ok: true, id };
}
async function deleteMission(env, id) {
  const idx = (await kvGetJson(env, 'missions:index')) || [];
  const next = idx.filter(x => x !== id);
  await kvPutJson(env, 'missions:index', next);
  await kvDelete(env, `mission:${id}`);
}
async function getUserMissionProgress(env, uid) {
  const key = `missionprog:${uid}`;
  return (await kvGetJson(env, key)) || { completed: 0, map: {} };
}
async function setUserMissionProgress(env, uid, prog) {
  await kvPutJson(env, `missionprog:${uid}`, prog || { completed: 0, map: {} });
}
async function completeMissionIfEligible(env, uid, mission) {
  const prog = await getUserMissionProgress(env, uid);
  const map = prog.map || {};
  const markKey = mission.period === 'once' ? 'once' : mission.period === 'daily' ? dayKey() : weekKey();
  const doneKey = `${mission.id}:${markKey}`;
  if (map[doneKey]) return false;
  // mark completed
  map[doneKey] = now();
  prog.map = map;
  prog.completed = (prog.completed || 0) + 1;
  await setUserMissionProgress(env, uid, prog);
  // reward diamonds
  const uKey = `user:${uid}`;
  const user = (await kvGetJson(env, uKey)) || { id: uid, diamonds: 0 };
  user.diamonds = (user.diamonds || 0) + (mission.reward || 0);
  await kvPutJson(env, uKey, user);
  // track weekly earned points for stats
  const wk = weekKey();
  const psKey = `points_week:${uid}:${wk}`;
  const ps = (await kvGetJson(env, psKey)) || { points: 0 };
  ps.points = (ps.points || 0) + (mission.reward || 0);
  await kvPutJson(env, psKey, ps);
  return true;
}
async function buildMissionsView(env, uid) {
  const missions = await listMissions(env);
  const prog = await getUserMissionProgress(env, uid);
  const nowWeek = weekKey();
  const list = missions.map(m => {
    const markKey = m.period === 'weekly' ? `${m.id}:${nowWeek}` : m.period === 'daily' ? `${m.id}:${dayKey()}` : `${m.id}:once`;
    const done = Boolean((prog.map||{})[markKey]);
    const periodLabel = m.period === 'weekly' ? 'هفتگی' : (m.period === 'daily' ? 'روزانه' : 'یکبار');
    const typeLabel = m.type === 'quiz' ? 'کوییز' : (m.type === 'question' ? 'مسابقه' : (m.type === 'invite' ? 'دعوت' : 'عمومی'));
    return `${done ? '✅' : '⬜️'} ${m.title} (${periodLabel} | ${typeLabel}) +${m.reward} الماس`;
  }).join('\n');
  const actions = [];
  actions.push([{ text: '✅ دریافت پاداش هفتگی (هر ۷ روز)', callback_data: 'WEEKLY_CHECKIN' }]);
  // dynamic actions for special weekly missions
  const quiz = missions.find(m => m.enabled && m.period === 'weekly' && m.type === 'quiz');
  const question = missions.find(m => m.enabled && m.period === 'weekly' && m.type === 'question');
  if (quiz) actions.push([{ text: '🎮 شرکت در کوییز هفتگی', callback_data: `MIS:QUIZ:${quiz.id}` }]);
  if (question) actions.push([{ text: '❓ پاسخ سوال هفتگی', callback_data: `MIS:Q:${question.id}` }]);
  actions.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
  return { text: `📆 مأموریت‌ها:\n${list}\n\nبا انجام فعالیت‌ها و چک‌این هفتگی الماس بگیرید.`, reply_markup: { inline_keyboard: actions } };
}

/* -------------------- Lottery helpers -------------------- */
async function getLotteryConfig(env) {
  return (await kvGetJson(env, 'lottery:cfg')) || { enabled: false, winners: 0, reward_diamonds: 0, run_every_hours: 0, next_run_at: 0 };
}
async function setLotteryConfig(env, cfg) { await kvPutJson(env, 'lottery:cfg', cfg || {}); }
async function lotteryAutoEnroll(env, uid) {
  const cfg = await getLotteryConfig(env);
  if (!cfg.enabled) return;
  const key = `lottery:pool:${dayKey()}`; // daily pool
  const pool = (await kvGetJson(env, key)) || [];
  if (!pool.includes(uid)) { pool.push(uid); await kvPutJson(env, key, pool); }
  // auto draw if threshold? We draw end-of-day; here we do nothing further
}
async function isUserEnrolledToday(env, uid) {
  const key = `lottery:pool:${dayKey()}`;
  const pool = (await kvGetJson(env, key)) || [];
  return pool.includes(uid);
}
async function userEnrollToday(env, uid) {
  const key = `lottery:pool:${dayKey()}`;
  const pool = (await kvGetJson(env, key)) || [];
  if (pool.includes(uid)) return false;
  pool.push(uid);
  await kvPutJson(env, key, pool);
  return true;
}
async function runLotteryPickAndReward(env, dateKey) {
  const cfg = await getLotteryConfig(env);
  if (!cfg.enabled || !(cfg.winners > 0) || !(cfg.reward_diamonds > 0)) return { ok: false };
  const key = `lottery:pool:${dateKey}`;
  const pool = (await kvGetJson(env, key)) || [];
  if (!pool.length) return { ok: false };
  const shuffled = pool.slice().sort(() => Math.random() - 0.5);
  const winners = shuffled.slice(0, Math.min(cfg.winners, shuffled.length));
  for (const w of winners) {
    const uKey = `user:${w}`;
    const u = (await kvGetJson(env, uKey)) || { id: w, diamonds: 0 };
    u.diamonds = (u.diamonds || 0) + cfg.reward_diamonds;
    await kvPutJson(env, uKey, u);
  }
  const hist = (await kvGetJson(env, 'lottery:hist')) || [];
  hist.unshift({ at: now(), dateKey, winners, reward_diamonds: cfg.reward_diamonds });
  await kvPutJson(env, 'lottery:hist', hist.slice(0, 100));
  return { ok: true, winners };
}
async function getLotteryHistory(env, limit = 20) {
  const hist = (await kvGetJson(env, 'lottery:hist')) || [];
  return hist.slice(0, limit);
}

/* -------------------- Daily tasks (cron) -------------------- */
async function runDailyTasks(env) {
  try {
    // 1) Automatic KV backup to main admin
    const adminIds = await getAdminIds(env);
    const mainAdmin = adminIds && adminIds.length ? adminIds[0] : null;
    if (mainAdmin) {
      const backup = await createKvBackup(env);
      const content = JSON.stringify(backup, null, 2);
      const filename = `backup_${new Date().toISOString().slice(0,10)}.json`;
      const form = new FormData();
      form.append('chat_id', String(mainAdmin));
      form.append('caption', filename);
      form.append('document', new Blob([content], { type: 'application/json' }), filename);
      await tgUpload('sendDocument', form);
    }
  } catch (_) {}

  try {
    // 2) Lottery: pick and reward winners for yesterday's pool (end-of-day draw)
    const nowTs = now();
    const yesterday = new Date(nowTs - 24*60*60*1000);
    const yKey = dayKey(yesterday.getTime());
    await runLotteryPickAndReward(env, yKey);
  } catch (_) {}
}

/* -------------------- Gift codes -------------------- */
async function giftCodeKey(code) { return `gift:${String(code).trim().toUpperCase()}`; }
async function createGiftCode(env, { code, amount, max_uses }) {
  if (!code || !Number.isFinite(amount) || amount <= 0) return { ok: false, error: 'پارامتر نامعتبر' };
  const key = await giftCodeKey(code);
  const exists = await kvGetJson(env, key);
  if (exists) return { ok: false, error: 'کد تکراری است' };
  const meta = { code: String(code).trim().toUpperCase(), amount: Number(amount), max_uses: Number(max_uses)||0, used: 0, disabled: false, created_at: now() };
  await kvPutJson(env, key, meta);
  return { ok: true };
}
async function listGiftCodes(env, limit = 50) {
  // KV list not available; we keep an index
  const idx = (await kvGetJson(env, 'gift:index')) || [];
  const codes = [];
  for (const c of idx.slice(0, limit)) {
    const g = await kvGetJson(env, `gift:${c}`);
    if (g) codes.push(g);
  }
  return codes;
}
async function addGiftToIndex(env, code) {
  const idx = (await kvGetJson(env, 'gift:index')) || [];
  const c = String(code).trim().toUpperCase();
  if (!idx.includes(c)) { idx.unshift(c); await kvPutJson(env, 'gift:index', idx); }
}
async function redeemGiftCode(env, uid, code) {
  const key = await giftCodeKey(code);
  const meta = await kvGetJson(env, key);
  if (!meta) return { ok: false, message: 'کد نامعتبر است.' };
  if (meta.disabled) return { ok: false, message: 'این کد غیرفعال است.' };
  if (meta.max_uses && (meta.used || 0) >= meta.max_uses) return { ok: false, message: 'ظرفیت این کد تکمیل شده است.' };
  const usedKey = `giftused:${meta.code}:${uid}`;
  const already = await kvGetJson(env, usedKey);
  if (already) return { ok: false, message: 'شما قبلا از این کد استفاده کرده‌اید.' };
  // credit diamonds
  const user = (await kvGetJson(env, `user:${uid}`)) || { id: uid, diamonds: 0 };
  user.diamonds = (user.diamonds || 0) + (meta.amount || 0);
  await kvPutJson(env, `user:${uid}`, user);
  // mark used
  await kvPutJson(env, usedKey, { used_at: now() });
  meta.used = (meta.used || 0) + 1;
  await kvPutJson(env, key, meta);
  return { ok: true, message: `🎁 ${meta.amount} الماس به حساب شما اضافه شد.` };
}

/* -------------------- Join/Admins management (KV backed) -------------------- */
async function getAdminIds(env) {
  const list = (await kvGetJson(env, 'bot:admins')) || null;
  if (Array.isArray(list) && list.length) return list.map(Number);
  return ADMIN_IDS.map(Number);
}
async function setAdminIds(env, list) {
  await kvPutJson(env, 'bot:admins', list.map(Number));
}
async function getRequiredChannels(env) {
  const list = (await kvGetJson(env, 'bot:join_channels')) || [];
  const defaultJoin = (RUNTIME.joinChat || JOIN_CHAT || '').trim();
  if (!list.length && defaultJoin) {
    const initial = [normalizeChannelIdentifier(defaultJoin)];
    await kvPutJson(env, 'bot:join_channels', initial);
    return initial;
  }
  return list;
}
async function setRequiredChannels(env, list) {
  await kvPutJson(env, 'bot:join_channels', list);
}
function normalizeChannelIdentifier(ch) {
  const s = String(ch).trim();
  if (/^-?\d+$/.test(s)) return s; // numeric id
  if (s.startsWith('@')) return s;
  return `@${s}`;
}
async function isUserJoinedAllRequiredChannels(env, userId) {
  const channels = await getRequiredChannels(env);
  if (!channels.length) return true;
  for (const ch of channels) {
    try {
      const ans = await tgGet(`getChatMember?chat_id=${encodeURIComponent(ch)}&user_id=${userId}`);
    if (!ans || !ans.ok) return false;
    const status = ans.result.status;
      if (!['member', 'creator', 'administrator'].includes(status)) return false;
    } catch (_) { return false; }
  }
  return true;
}
async function presentJoinPrompt(env, chatId) {
  const channels = await getRequiredChannels(env);
  const buttons = channels.map(ch => {
    const username = ch.startsWith('@') ? ch.slice(1) : '';
    const url = username ? `https://t.me/${username}` : undefined;
    return url ? [{ text: `عضویت در ${ch}`, url }] : [{ text: `${ch}`, callback_data: 'NOOP' }];
  });
  buttons.push([{ text: '✅ بررسی عضویت', callback_data: 'CHECK_JOIN' }]);
  await tgApi('sendMessage', { chat_id: chatId, text: 'برای استفاده از ربات، ابتدا در کانال‌های زیر عضو شوید، سپس روی «بررسی عضویت» بزنید.', reply_markup: { inline_keyboard: buttons } });
}

/* -------------------- Webhook ensure helper -------------------- */
async function ensureWebhookForRequest(env, req) {
  try {
    // Throttle to at most once per 10 minutes
    const last = await kvGetJson(env, 'bot:webhook_check_at');
    const nowTs = now();
    if (last && (nowTs - last) < 10 * 60 * 1000) return;
    const info = await tgGet('getWebhookInfo');
    const want = RUNTIME.webhookUrl || WEBHOOK_URL || '';
    const current = info && info.result && info.result.url || '';
    if (!current || current !== want) {
      if (want) {
        await tgSetWebhook(want);
        await kvPutJson(env, 'bot:webhook_set_at', now());
      }
    }
    await kvPutJson(env, 'bot:webhook_check_at', nowTs);
  } catch (_) { /* ignore */ }
}

// Removed module-level network call to comply with Worker cold-start best practices

/* End of enhanced worker */
