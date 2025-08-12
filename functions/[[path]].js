// Catch-all Pages Function to delegate to main.js default export (Worker-style)
// This keeps your existing routes (/, /api/*, /f/*, etc.) working on Pages

import app from '../main.js';

export async function onRequest(context) {
  const { request, env, waitUntil } = context;
  if (!app || typeof app.fetch !== 'function') {
    return new Response('Application not initialized', { status: 500 });
  }
  return app.fetch(request, env, { waitUntil });
}


