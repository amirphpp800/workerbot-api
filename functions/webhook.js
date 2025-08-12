// Cloudflare Pages Function to handle Telegram webhook
// Exposes onRequestPost and delegates to handleUpdate from ../main.js (ESM)

import { handleUpdate } from '../main.js';

export async function onRequestPost({ request, env, waitUntil }) {
  try {
    const update = await request.json();
    // If handleUpdate returns a promise, ensure it's awaited
    const p = handleUpdate(update, env, { waitUntil });
    if (p && typeof p.then === 'function') await p;
    return new Response('ok');
  } catch (err) {
    return new Response('bad request', { status: 400 });
  }
}


