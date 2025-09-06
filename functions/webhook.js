// Cloudflare Pages Function to handle Telegram webhook
// Delegate to the default export from main.js (Worker-style app.fetch)

import app from '../main.js';

export async function onRequestPost(context) {
  const { request, env, waitUntil } = context;
  if (!app || typeof app.fetch !== 'function') {
    return new Response('Application not initialized', { status: 500 });
  }
  return app.fetch(request, env, { waitUntil });
}


