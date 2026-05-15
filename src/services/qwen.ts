/*
 * File: qwen.ts
 * Project: qwenproxy
 * Author: Pedro Farias
 * Created: 2026-05-12
 */

import { getQwenHeaders, getBasicHeaders } from './playwright.ts';
import { v4 as uuidv4 } from 'uuid';

const sessionStates: Record<string, string | null> = (globalThis as any)._sessionStates || {};
(globalThis as any)._sessionStates = sessionStates;

export function updateSessionParent(sessionId: string, parentId: string | null) {
  if (sessionId) {
    sessionStates[sessionId] = parentId;
  }
}

export interface QwenMessage {
  fid: string;
  parentId: string | null;
  childrenIds: string[];
  role: 'user' | 'assistant';
  content: string;
  user_action: string;
  files: any[];
  timestamp: number;
  models: string[];
  chat_type: string;
  feature_config: {
    thinking_enabled: boolean;
    output_schema: string;
    research_mode: string;
    auto_thinking: boolean;
    thinking_mode: string;
    thinking_format: string;
    auto_search: boolean;
  };
  extra: {
    meta: {
      subChatType: string;
    };
  };
  sub_chat_type: string;
  parent_id: string | null;
}

export interface QwenPayload {
  stream: boolean;
  version: string;
  incremental_output: boolean;
  chat_id: string;
  chat_mode: string;
  model: string;
  parent_id: string | null;
  messages: QwenMessage[];
  timestamp: number;
}

let cachedModels: any[] | null = null;
let lastModelsFetch = 0;

export async function disableNativeTools(): Promise<void> {
  const { headers } = await getQwenHeaders();
  
  const payload = {
    tools_enabled: {
      web_extractor: false,
      web_search_image: false,
      web_search: false,
      image_gen_tool: false,
      code_interpreter: false,
      history_retriever: false,
      image_edit_tool: false,
      bio: false,
      image_zoom_in_tool: false
    }
  };

  console.log('[Qwen] Disabling native tools...');
  const response = await fetch('https://chat.qwen.ai/api/v2/users/user/settings/update', {
    method: 'POST',
    headers: {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'pt-BR,pt;q=0.9',
      'content-type': 'application/json',
      'cookie': headers['cookie'],
      'origin': 'https://chat.qwen.ai',
      'referer': 'https://chat.qwen.ai/',
      'user-agent': headers['user-agent'],
      'x-request-id': uuidv4(),
      'bx-ua': headers['bx-ua'],
      'bx-umidtoken': headers['bx-umidtoken'],
      'bx-v': headers['bx-v']
    },
    body: JSON.stringify(payload)
  });

  if (!response.ok) {
    const text = await response.text();
    console.error(`[Qwen] Failed to disable native tools: ${response.status} - ${text}`);
  } else {
    console.log('[Qwen] Native tools disabled successfully.');
  }
}

export async function fetchQwenModels(): Promise<any[]> {
  const now = Date.now();
  if (cachedModels && (now - lastModelsFetch < 3600000)) {
    return cachedModels;
  }

  const { cookie, userAgent, bxV } = await getBasicHeaders();
  
  const response = await fetch('https://chat.qwen.ai/api/models', {
    headers: {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'pt-BR,pt;q=0.9',
      'cookie': cookie,
      'referer': 'https://chat.qwen.ai/',
      'user-agent': userAgent,
      'x-request-id': uuidv4(),
      'bx-v': bxV,
      'timezone': new Date().toString(),
      'source': 'web'
    }
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch models from Qwen: ${response.status} ${response.statusText}`);
  }

  const json = await response.json();
  if (json.data && Array.isArray(json.data)) {
    const models = json.data.map((m: any) => ({
      id: m.id,
      object: 'model',
      created: m.info?.created_at || Math.floor(Date.now() / 1000),
      owned_by: m.owned_by || 'qwen'
    }));

    const extendedModels = [...models];
    for (const m of models) {
      extendedModels.push({
        ...m,
        id: `${m.id}-no-thinking`
      });
    }

    cachedModels = extendedModels;
    lastModelsFetch = now;
    return extendedModels;
  }

  return [];
}

// ✅ Criar novo chat no Qwen e retornar chat_id
async function createQwenChat(
  headers: Record<string, string>,
  model: string
): Promise<string> {
  const chatId = uuidv4();
  
  console.log(`[Qwen] Creating new chat | ID: ${chatId}`);
  
  const payload = {
    chat_id: chatId,
    model: model,
    chat_mode: 'normal',
    title: 'New Chat'
  };

  const response = await fetch('https://chat.qwen.ai/api/v2/chats/create', {
    method: 'POST',
    headers: {
      'accept': 'application/json, text/plain, */*',
      'accept-language': 'pt-BR,pt;q=0.9',
      'content-type': 'application/json',
      'cookie': headers['cookie'],
      'origin': 'https://chat.qwen.ai',
      'referer': 'https://chat.qwen.ai/',
      'user-agent': headers['user-agent'],
      'x-request-id': uuidv4(),
      'bx-ua': headers['bx-ua'],
      'bx-umidtoken': headers['bx-umidtoken'],
      'bx-v': headers['bx-v']
    },
    body: JSON.stringify(payload)
  });

  const responseText = await response.text();
  console.log(`[Qwen] Create chat response: ${response.status} | Body: ${responseText.substring(0, 200)}`);

  if (response.ok) {
    try {
      const json = JSON.parse(responseText);
      // ✅ Retornar chat_id da resposta ou o que geramos
      const returnedId = json?.data?.chat_id || json?.chat_id || chatId;
      console.log(`[Qwen] ✅ Chat created: ${returnedId}`);
      return returnedId;
    } catch {
      console.log(`[Qwen] Using generated chat_id: ${chatId}`);
      return chatId;
    }
  }

  // ✅ Se falhou criar, usar o UUID gerado mesmo (alguns casos aceita)
  console.warn(`[Qwen] Create chat failed (${response.status}), using generated ID: ${chatId}`);
  return chatId;
}

// ✅ Separar system prompt do user message
function splitPrompt(fullPrompt: string): { systemContent: string; userContent: string } {
  const userPrefix = 'User: ';
  const lastUserIndex = fullPrompt.lastIndexOf(userPrefix);
  
  if (lastUserIndex === -1) {
    return { systemContent: '', userContent: fullPrompt };
  }
  
  const systemContent = fullPrompt.substring(0, lastUserIndex).trim();
  const userContent = fullPrompt.substring(lastUserIndex + userPrefix.length).trim();
  
  return { systemContent, userContent };
}

// ✅ Truncar system preservando user message
function buildFinalContent(prompt: string, maxChars: number): string {
  if (prompt.length <= maxChars) return prompt;
  
  const { systemContent, userContent } = splitPrompt(prompt);
  
  if (userContent) {
    const maxSystemChars = Math.max(maxChars - userContent.length - 100, 1000);
    const truncatedSystem = systemContent.length > maxSystemChars
      ? systemContent.substring(0, maxSystemChars) + '\n\n[...]\n\n'
      : systemContent;
    
    const result = `${truncatedSystem}\n\nUser: ${userContent}`;
    console.log(`[Qwen] Prompt truncated: ${prompt.length} → ${result.length} chars`);
    return result;
  }
  
  return prompt.substring(0, maxChars);
}

export async function createQwenStream(
  prompt: string, 
  enableThinking: boolean, 
  modelId: string,
  forcedParentId?: string | null
): Promise<{ stream: ReadableStream, headers: Record<string, string>, uiSessionId: string }> {
  
  console.log(`[Qwen] Creating stream | Prompt: ${prompt.length} chars | Thinking: ${enableThinking} | forcedParentId: ${String(forcedParentId)}`);
  
  const isNewSession = forcedParentId === null;
  
  const { headers, chatSessionId, parentMessageId } = await getQwenHeaders(isNewSession);

  let actualParentId: string | null = parentMessageId;
  
  if (forcedParentId !== undefined) {
    actualParentId = forcedParentId;
  } else if (chatSessionId && sessionStates[chatSessionId] !== undefined) {
    actualParentId = sessionStates[chatSessionId];
  }

  const timestamp = Math.floor(Date.now() / 1000);
  const fid = uuidv4();
  const model = modelId.replace('-no-thinking', '');

  // ✅ Truncar prompt se necessário
  const finalContent = buildFinalContent(prompt, 8000);
  const { systemContent, userContent } = splitPrompt(finalContent);
  console.log(`[Qwen] System: ${systemContent.length} chars | User: ${userContent.length} chars | Total: ${finalContent.length} chars`);

  // ✅ CRÍTICO: chat_id SEMPRE deve ser string válida
  // Se temos chatSessionId do Playwright, usar ele
  // Se é nova sessão, criar novo chat via API
  let effectiveChatId: string = chatSessionId || '';
  
  if (!effectiveChatId || isNewSession) {
    // ✅ Criar novo chat para obter chat_id válido
    effectiveChatId = await createQwenChat(headers, model);
    // ✅ Resetar parent para nova sessão
    actualParentId = null;
  }

  console.log(`[Qwen] Using chat_id: ${effectiveChatId} | parent_id: ${actualParentId}`);

  const payload: QwenPayload = {
    stream: true,
    version: '2.1',
    incremental_output: true,
    chat_id: effectiveChatId,      // ✅ SEMPRE string válida
    chat_mode: 'normal',
    model: model,
    parent_id: actualParentId,
    messages: [
      {
        fid: fid,
        parentId: actualParentId,
        childrenIds: [],
        role: 'user',
        content: finalContent,
        user_action: 'chat',
        files: [],
        timestamp: timestamp,
        models: [model],
        chat_type: 't2t',
        feature_config: {
          thinking_enabled: enableThinking,
          output_schema: 'phase',
          research_mode: 'normal',
          auto_thinking: false,
          thinking_mode: 'Thinking',
          thinking_format: 'summary',
          auto_search: false,      // ✅ SEMPRE false
        },
        extra: {
          meta: {
            subChatType: 't2t'
          }
        },
        sub_chat_type: 't2t',
        parent_id: actualParentId
      }
    ],
    timestamp: timestamp + 1
  };

  // ✅ URL SEMPRE com chat_id
  const url = `https://chat.qwen.ai/api/v2/chat/completions?chat_id=${effectiveChatId}`;

  console.log(`[Qwen] POST ${url}`);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'accept': 'application/json',
      'accept-language': 'pt-BR,pt;q=0.9',
      'content-type': 'application/json',
      'cookie': headers['cookie'],
      'origin': 'https://chat.qwen.ai',
      'referer': `https://chat.qwen.ai/c/${effectiveChatId}`,
      'sec-fetch-dest': 'empty',
      'sec-fetch-mode': 'cors',
      'sec-fetch-site': 'same-origin',
      'timezone': new Date().toString().split(' (')[0], 
      'user-agent': headers['user-agent'],
      'x-accel-buffering': 'no',
      'x-request-id': uuidv4(),
      'bx-ua': headers['bx-ua'],
      'bx-umidtoken': headers['bx-umidtoken'],
      'bx-v': headers['bx-v']
    },
    body: JSON.stringify(payload)
  });

  const contentType = response.headers.get('content-type') || '';
  const contentLength = response.headers.get('content-length') || 'unknown';
  
  console.log(`[Qwen] Response: ${response.status} | Content-Type: ${contentType} | Content-Length: ${contentLength}`);

  // ✅ Detectar erro JSON
  if (contentType.includes('application/json') || !response.ok) {
    const errorBody = await response.text().catch(() => '(unreadable)');
    console.error(`[Qwen] ❌ Error: ${errorBody}`);
    
    try {
      const errorJson = JSON.parse(errorBody);
      const msg = errorJson?.data?.details || errorJson?.message || errorJson?.error || errorBody;
      throw new Error(`Qwen API error (${response.status}): ${msg}`);
    } catch (e: any) {
      if (e.message.includes('Qwen API error')) throw e;
      throw new Error(`Qwen API error (${response.status}): ${errorBody}`);
    }
  }

  if (!response.body) {
    throw new Error(`Qwen returned empty body (${response.status})`);
  }

  console.log(`[Qwen] ✅ Stream started | chat_id: ${effectiveChatId}`);
  return { stream: response.body, headers, uiSessionId: effectiveChatId };
}
