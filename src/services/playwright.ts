/*
 * File: playwright.ts
 * Project: qwenproxy
 * Author: Pedro Farias
 * Created: 2026-05-12
 * 
 * Sistema de rotação de sessões para evitar rate limit
 */

import { chromium, Browser, BrowserContext, Page } from 'playwright';

let browser: Browser | null = null;

// ✅ Pool de sessões ativas
interface QwenSession {
  context: BrowserContext;
  page: Page;
  chatSessionId: string;
  requestCount: number;
  createdAt: number;
}

const sessionPool: QwenSession[] = [];
const MAX_REQUESTS_PER_SESSION = 5; // ✅ Limite de requisições por chat
const MAX_SESSIONS = 10; // ✅ Máximo de sessões simultâneas
let currentSessionIndex = 0;

export async function initPlaywright() {
  if (!browser) {
    console.log('[Playwright] Launching browser...');
    browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-blink-features=AutomationControlled'
      ]
    });
    console.log('[Playwright] Browser launched.');
  }
}

// ✅ Criar nova sessão do Qwen
async function createNewSession(): Promise<QwenSession> {
  if (!browser) await initPlaywright();

  console.log('[Playwright] Creating new Qwen session...');

  const context = await browser!.newContext({
    viewport: { width: 1280, height: 720 },
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
  });

  const page = await context.newPage();

  // Navegar para Qwen
  console.log('[Playwright] Navigating to Qwen...');
  await page.goto('https://chat.qwen.ai', { waitUntil: 'domcontentloaded' });
  await page.waitForTimeout(2000);

  // Aguardar input estar pronto
  await page.waitForSelector('.message-input-area textarea, input[placeholder*="Message"]', { timeout: 10000 });

  // ✅ Disparar uma mensagem dummy para criar chat e capturar headers
  let headers: Record<string, string> = {};
  let chatSessionId = '';

  const interceptPromise = page.waitForResponse(
    (response) => response.url().includes('/api/v2/chat/completions'),
    { timeout: 15000 }
  ).then(async (response) => {
    const reqHeaders = response.request().headers();
    headers = {
      'cookie': reqHeaders['cookie'] || '',
      'user-agent': reqHeaders['user-agent'] || '',
      'bx-v': reqHeaders['bx-v'] || '',
      'bx-ua': reqHeaders['bx-ua'] || '',
      'bx-umidtoken': reqHeaders['bx-umidtoken'] || ''
    };

    // Extrair chat_id da URL
    const url = response.url();
    const match = url.match(/chat_id=([^&]+)/);
    if (match) chatSessionId = match[1];
  });

  // Disparar mensagem
  const inputSelector = '.message-input-area textarea, input[placeholder*="Message"]';
  await page.fill(inputSelector, '.');
  await page.waitForTimeout(500);
  await page.keyboard.press('Enter');

  await interceptPromise;

  console.log(`[Playwright] ✅ New session created | chat_id: ${chatSessionId}`);

  return {
    context,
    page,
    chatSessionId,
    requestCount: 0,
    createdAt: Date.now()
  };
}

// ✅ Obter sessão disponível (ou criar nova)
async function getAvailableSession(): Promise<QwenSession> {
  // Limpar sessões expiradas (>30min sem uso)
  const now = Date.now();
  for (let i = sessionPool.length - 1; i >= 0; i--) {
    const session = sessionPool[i];
    if (now - session.createdAt > 30 * 60 * 1000) {
      console.log(`[Playwright] Closing expired session: ${session.chatSessionId}`);
      await session.context.close();
      sessionPool.splice(i, 1);
    }
  }

  // Procurar sessão disponível (abaixo do limite)
  for (const session of sessionPool) {
    if (session.requestCount < MAX_REQUESTS_PER_SESSION) {
      console.log(`[Playlist] Reusing session ${session.chatSessionId} (${session.requestCount}/${MAX_REQUESTS_PER_SESSION} requests)`);
      return session;
    }
  }

  // Se todas estão no limite, limpar a mais antiga e criar nova
  if (sessionPool.length >= MAX_SESSIONS) {
    const oldest = sessionPool.shift()!;
    console.log(`[Playwright] Pool full, closing oldest session: ${oldest.chatSessionId}`);
    await oldest.context.close();
  }

  // Criar nova sessão
  const newSession = await createNewSession();
  sessionPool.push(newSession);
  return newSession;
}

// ✅ Função principal: obter headers de uma sessão válida
export async function getQwenHeaders(forceNew = false): Promise<{
  headers: Record<string, string>;
  chatSessionId: string;
  parentMessageId: string | null;
}> {
  let session: QwenSession;

  if (forceNew) {
    // Forçar nova sessão
    console.log('[Playwright] Force new session requested');
    session = await createNewSession();
    sessionPool.push(session);
  } else {
    // Obter sessão disponível
    session = await getAvailableSession();
  }

  // ✅ Incrementar contador de uso
  session.requestCount++;

  // Re-extrair headers atualizados
  const page = session.page;
  
  let headers: Record<string, string> = {};
  let parentMessageId: string | null = null;

  // Interceptar próxima request para pegar headers frescos
  const interceptPromise = page.waitForResponse(
    (response) => response.url().includes('/api/v2/chat/completions'),
    { timeout: 10000 }
  ).then(async (response) => {
    const reqHeaders = response.request().headers();
    headers = {
      'cookie': reqHeaders['cookie'] || '',
      'user-agent': reqHeaders['user-agent'] || '',
      'bx-v': reqHeaders['bx-v'] || '',
      'bx-ua': reqHeaders['bx-ua'] || '',
      'bx-umidtoken': reqHeaders['bx-umidtoken'] || ''
    };
  }).catch(() => {
    // Se timeout, usar headers em cache (melhor que falhar)
    console.warn('[Playwright] Could not refresh headers, using cached');
  });

  // Disparar mensagem dummy para refresh de headers
  const inputSelector = '.message-input-area textarea, input[placeholder*="Message"]';
  await page.fill(inputSelector, '.');
  await page.waitForTimeout(300);
  await page.keyboard.press('Enter');

  await interceptPromise.catch(() => {});

  console.log(`[Playwright] Session ${session.chatSessionId} | Requests: ${session.requestCount}/${MAX_REQUESTS_PER_SESSION}`);

  return {
    headers,
    chatSessionId: session.chatSessionId,
    parentMessageId
  };
}

// ✅ Obter headers básicos (sem criar sessão completa)
export async function getBasicHeaders(): Promise<{
  cookie: string;
  userAgent: string;
  bxV: string;
}> {
  const session = await getAvailableSession();
  const cookies = await session.context.cookies();
  const cookieString = cookies.map(c => `${c.name}=${c.value}`).join('; ');

  return {
    cookie: cookieString,
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    bxV: '2.5.4'
  };
}

// ✅ Cleanup ao encerrar
export async function closePlaywright() {
  console.log('[Playwright] Closing all sessions...');
  for (const session of sessionPool) {
    await session.context.close();
  }
  sessionPool.length = 0;

  if (browser) {
    await browser.close();
    browser = null;
    console.log('[Playwright] Browser closed.');
  }
}

process.on('SIGINT', async () => {
  await closePlaywright();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await closePlaywright();
  process.exit(0);
});
