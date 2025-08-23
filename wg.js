// WireGuard module: handlers and utilities extracted from main.js

// Local utility used only in this module
function base64UrlToBase64(u) {
  const s = u.replace(/-/g, '+').replace(/_/g, '/');
  return s + '='.repeat((4 - (s.length % 4)) % 4);
}

function randomIntInclusive(min, max) {
  const mn = Math.ceil(min);
  const mx = Math.floor(max);
  return Math.floor(Math.random() * (mx - mn + 1)) + mn;
}

async function generateWgKeypairBase64() {
  const kp = await crypto.subtle.generateKey({ name: 'X25519' }, true, ['deriveBits']);
  const jwkPriv = await crypto.subtle.exportKey('jwk', kp.privateKey);
  const jwkPub = await crypto.subtle.exportKey('jwk', kp.publicKey);
  const privB64 = base64UrlToBase64(jwkPriv.d || '');
  const pubB64 = base64UrlToBase64(jwkPub.x || '');
  return { privateKey: privB64, publicKey: pubB64 };
}

// Countries list used for WG menu paging
const WG_COUNTRIES = ['ES','DE','FR','PH','JP','TR','SE','NL','DK','BE','CH','CN'];

export async function handleWireguardMyConfig(data, ctx) {
  const { uid, chatId, env, tgApi, tgUpload, kvGetJson, countryFlag, dnsCountryLabel } = ctx;
  const id = data.split(':')[2];
  await tgApi('answerCallbackQuery', { callback_query_id: ctx.cbId });
  const listKey = `user:${uid}:servers`;
  const list = (await kvGetJson(env, listKey)) || [];
  const item = list.find(s => String(s.id) === String(id) && (s.type||'dns') === 'wg');
  if (!item) { await tgApi('sendMessage', { chat_id: chatId, text: 'مورد یافت نشد.' }); return; }
  if (item.conf) {
    const form = new FormData();
    form.append('chat_id', String(chatId));
    form.append('document', new Blob([item.conf], { type: 'text/plain' }), `${(item.name||'WG')}.conf`);
    form.append('caption', `${countryFlag(item.country)} وایرگارد (${dnsCountryLabel(item.country)})${item.name ? `\nنام: ${item.name}` : ''}`);
    const res = await tgUpload('sendDocument', form);
    if (!res || !res.ok) { await tgApi('sendMessage', { chat_id: chatId, text: 'ارسال فایل با خطا مواجه شد.' }); }
    return;
  }
  if (item.endpoint && item.name) {
    const mtu = 1410;
    const address = `10.66.66.${randomIntInclusive(2, 254)}/24`;
    const allowed = '43.152.0.0/16, 45.40.0.0/16, 150.109.0.0/16, 161.117.0.0/16, 18.141.0.0/16, 34.87.0.0/16, 52.76.0.0/16, 52.220.0.0/16, 170.106.0.0/16, 125.209.222.0/24, 203.205.0.0/16';
    const conf = `[Interface]\nPrivateKey = (در زمان ساخت ذخیره نشده)\nAddress = ${address}\nDNS = 10.202.10.10\nMTU = ${mtu}\n\n[Peer]\nPublicKey = (در زمان ساخت ذخیره نشده)\nEndpoint = ${item.endpoint}\nAllowedIPs = ${allowed}\nPersistentKeepalive = 25\n`;
    const form = new FormData();
    form.append('chat_id', String(chatId));
    form.append('document', new Blob([conf], { type: 'text/plain' }), `${(item.name||'WG')}.conf`);
    form.append('caption', `${countryFlag(item.country)} وایرگارد (${dnsCountryLabel(item.country)})${item.name?`\nنام: ${item.name}`:''}`);
    const res = await tgUpload('sendDocument', form);
    if (!res || !res.ok) { await tgApi('sendMessage', { chat_id: chatId, text: 'ارسال فایل با خطا مواجه شد.' }); }
    return;
  }
  await tgApi('sendMessage', { chat_id: chatId, text: 'متاسفانه اطلاعات کافی برای ارسال کانفیگ موجود نیست.' });
}

export async function handleWireguardCallback(data, ctx) {
  const {
    uid, chatId, env,
    tgApi, tgUpload,
    kvGetJson, kvPutJson,
    now, getSettings, getDnsCidrConfig,
    countryFlag, dnsCountryLabel,
    randomIp4FromCidr, randomIpv6FromCidr,
    isLocationDisabled
  } = ctx;

  if (data === 'PS:WG') {
    await tgApi('answerCallbackQuery', { callback_query_id: ctx.cbId });
    const page = 0;
    const perPage = 12;
    const totalPages = Math.ceil(WG_COUNTRIES.length / perPage);
    const rows = [];
    const slice = WG_COUNTRIES.slice(page*perPage, page*perPage + perPage);
    for (let i = 0; i < slice.length; i += 2) {
      const c1 = slice[i]; const c2 = slice[i+1];
      const r = [{ text: `${countryFlag(c1)} ${dnsCountryLabel(c1)}`, callback_data: `PS:WG:${c1}` }];
      if (c2) r.push({ text: `${countryFlag(c2)} ${dnsCountryLabel(c2)}`, callback_data: `PS:WG:${c2}` });
      rows.push(r);
    }
    rows.push([{ text: '⬅️ بازگشت', callback_data: 'PRIVATE_SERVER' }]);
    rows.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
    if (totalPages > 1) {
      const label = `${page+1}/${totalPages} صفحه ${page+1} از ${totalPages}`;
      const nav = [{ text: label, callback_data: 'NOOP' }];
      if (page < totalPages - 1) nav.push({ text: '▶️ صفحه بعد', callback_data: `PS:WG_PAGE:${page+1}` });
      rows.push(nav);
    }
    await tgApi('sendMessage', { chat_id: chatId, text: '🌐 کشور مورد نظر برای وایرگارد اختصاصی را انتخاب کنید:', reply_markup: { inline_keyboard: rows } });
    return;
  }

  if (data.startsWith('PS:WG_PAGE:')) {
    await tgApi('answerCallbackQuery', { callback_query_id: ctx.cbId });
    const perPage = 12;
    const totalPages = Math.ceil(WG_COUNTRIES.length / perPage);
    let page = parseInt(data.split(':')[2], 10) || 0;
    if (page < 0) page = 0;
    if (page >= totalPages) page = totalPages - 1;
    const start = page * perPage;
    const slice = WG_COUNTRIES.slice(start, start + perPage);
    const rows = [];
    for (let i = 0; i < slice.length; i += 2) {
      const c1 = slice[i]; const c2 = slice[i+1];
      const r = [{ text: `${countryFlag(c1)} ${dnsCountryLabel(c1)}`, callback_data: `PS:WG:${c1}` }];
      if (c2) r.push({ text: `${countryFlag(c2)} ${dnsCountryLabel(c2)}`, callback_data: `PS:WG:${c2}` });
      rows.push(r);
    }
    if (totalPages > 1) {
      const label = `${page+1}/${totalPages} صفحه ${page+1} از ${totalPages}`;
      const nav = [];
      if (page > 0) nav.push({ text: '◀️ صفحه قبل', callback_data: `PS:WG_PAGE:${page-1}` });
      nav.push({ text: label, callback_data: 'NOOP' });
      if (page < totalPages - 1) nav.push({ text: '▶️ صفحه بعد', callback_data: `PS:WG_PAGE:${page+1}` });
      rows.push(nav);
    }
    rows.push([{ text: '⬅️ بازگشت', callback_data: 'PRIVATE_SERVER' }]);
    rows.push([{ text: '🏠 منو', callback_data: 'MENU' }]);
    await tgApi('sendMessage', { chat_id: chatId, text: '🌐 کشور مورد نظر برای وایرگارد اختصاصی را انتخاب کنید:', reply_markup: { inline_keyboard: rows } });
    return;
  }

  if (data.startsWith('PS:WG:')) {
    const code = data.split(':')[2];
    await tgApi('answerCallbackQuery', { callback_query_id: ctx.cbId });
    if (await isLocationDisabled(env, 'wg', code)) {
      await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش درحال توسعه و بروزرسانی می‌باشد و موقتا غیر فعال است.' });
      return;
    }
    const userKey = `user:${uid}`;
    const user = (await kvGetJson(env, userKey)) || { id: uid, diamonds: 0 };
    const settings = await getSettings(env);
    const cost = settings.cost_wg || 2;
    const text = `🛰️ وایرگارد اختصاصی (${dnsCountryLabel(code)})\n\n💎 هزینه: ${cost} الماس\n💳 آیا پرداخت انجام شود؟\n\n👤 موجودی شما: ${user.diamonds || 0}`;
    const kb = { inline_keyboard: [
      [{ text: '✅ پرداخت و دریافت', callback_data: `PS:WGCONF:${code}` }],
      [{ text: '❌ انصراف', callback_data: 'PS:WG' }]
    ] };
    await tgApi('sendMessage', { chat_id: chatId, text, reply_markup: kb });
    return;
  }

  if (data.startsWith('PS:WGCONF:')) {
    const code = data.split(':')[2];
    await tgApi('answerCallbackQuery', { callback_query_id: ctx.cbId });
    if (await isLocationDisabled(env, 'wg', code)) {
      await tgApi('sendMessage', { chat_id: chatId, text: 'این بخش درحال توسعه و بروزرسانی می‌باشد و موقتا غیر فعال است.' });
      return;
    }
    const userKey = `user:${uid}`;
    const user = (await kvGetJson(env, userKey)) || { id: uid, diamonds: 0 };
    const settings = await getSettings(env);
    const cost = settings.cost_wg || 2;
    if ((user.diamonds || 0) < cost) { await tgApi('sendMessage', { chat_id: chatId, text: `⚠️ الماس کافی نیست. این سرویس ${cost} الماس هزینه دارد.` }); return; }
    user.diamonds = (user.diamonds || 0) - cost; await kvPutJson(env, userKey, user);

    const kp = await generateWgKeypairBase64();
    const cfg = await getDnsCidrConfig(env);
    const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
    const v4cidr = pick((cfg[code]||{}).v4 || []);
    const v6cidr = pick((cfg[code]||{}).v6 || []);
    const dnsV4 = v4cidr ? randomIp4FromCidr(v4cidr) : '1.1.1.1';
    const dnsFixedV4 = '10.202.10.10';
    const dnsV6 = v6cidr ? randomIpv6FromCidr(v6cidr) : '2001:4860:4860::8888';
    const epCidr = v4cidr || pick(((cfg[code]||{}).v4 || []));
    const endpointHost = epCidr ? randomIp4FromCidr(epCidr) : '8.8.8.8';
    const endpoint = `${endpointHost}:51820`;
    const nameId = String(Math.floor(100000 + Math.random() * 900000));
    const name = `NoiD${nameId}`;
    const mtu = 1410;
    const address = `10.66.66.${randomIntInclusive(2, 254)}/24`;
    const allowed = '43.152.0.0/16, 45.40.0.0/16, 150.109.0.0/16, 161.117.0.0/16, 18.141.0.0/16, 34.87.0.0/16, 52.76.0.0/16, 52.220.0.0/16, 170.106.0.0/16, 125.209.222.0/24, 203.205.0.0/16';
    const conf = `[Interface]\nPrivateKey = ${kp.privateKey}\nAddress = ${address}\nDNS = ${dnsV4}, ${dnsFixedV4}, ${dnsV6}\nMTU = ${mtu}\n\n[Peer]\nPublicKey = ${kp.publicKey}\nEndpoint = ${endpoint}\nAllowedIPs = ${allowed}\nPersistentKeepalive = 25\n`;

    try {
      const listKey = `user:${uid}:servers`;
      const list = (await kvGetJson(env, listKey)) || [];
      list.unshift({ id: `${now()}`, type: 'wg', country: code, name, endpoint, created_at: now() });
      if (list.length > 200) list.length = 200;
      await kvPutJson(env, listKey, list);
    } catch (_) {}

    const form = new FormData();
    form.append('chat_id', String(chatId));
    form.append('document', new Blob([conf], { type: 'text/plain' }), `${name}.conf`);
    form.append('caption', `${countryFlag(code)} وایرگارد اختصاصی (${dnsCountryLabel(code)})\nنام: ${name}`);
    const res = await tgUpload('sendDocument', form);
    if (!res || !res.ok) { await tgApi('sendMessage', { chat_id: chatId, text: 'ارسال فایل با خطا مواجه شد.' }); }
    return;
  }
}


