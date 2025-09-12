// Cloudflare Pages Function to handle Telegram webhook
// Exposes onRequestPost and delegates to handleUpdate from ../main.js (ESM)

import { handleUpdate } from '../main.js';

export async function onRequestPost({ request, env, waitUntil }) {
  try {
    const update = await request.json();
    const p = handleUpdate(update, env, { waitUntil });
    if (waitUntil && p && typeof p.then === 'function') {
      waitUntil(p);
    }
    // Respond immediately; processing continues in background
    return new Response('ok');
  } catch (err) {
    return new Response('bad request', { status: 400 });
  }
}

// Optional: allow GET to /webhook to return a helpful message when opened in browser
export async function onRequestGet() {
  return new Response('Telegram webhook endpoint. Use POST from Telegram servers.');
}


