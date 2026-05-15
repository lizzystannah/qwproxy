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
  chat_id: string | null;
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

// ✅ Limite seguro de caracteres por mensagem para o Qwen
const QWEN_SAFE_PROMPT_LIMIT = 8000;

// ✅ Truncar prompt preservando início e fim (mais importante)
function truncatePrompt(prompt: string, maxChars: number): string {
  if (prompt.length <= maxChars) return prompt;
  
  console.warn(`[Qwen] Prompt truncado: ${prompt.length} → ${maxChars} chars`);
  
  // ✅ Preservar 70% do início (system prompt + tools) e 30% do fim (última mensagem)
  const keepStart = Math.floor(maxChars * 0.7);
  const keepEnd = Math.floor(maxChars * 0.3);
  
  const start = prompt.substring(0, keepStart);
  const end = prompt.substring(prompt.length - keepEnd);
  
  return `${start}\n\n[... contexto truncado por tamanho ...]\n\n${end}`;
}

// ✅ Separar system prompt do user prompt para enviar corretamente
function splitPrompt(fullPrompt: string): { systemContent: string; userContent: string } {
  // ✅ Tentar encontrar a última mensagem "User:" no prompt
  const userPrefix = 'User: ';
  const lastUserIndex = fullPrompt.lastIndexOf(userPrefix);
  
  if (lastUserIndex === -1) {
    // Sem separação clara, enviar tudo como user
    return { systemContent: '', userContent: fullPrompt };
  }
  
  const systemContent = fullPrompt.substring(0, lastUserIndex).trim();
  const userContent = fullPrompt.substring(lastUserIndex + userPrefix.length).trim();
  
  return { systemContent, userContent };
}

export async function createQwenStream(
  prompt: string, 
  enableThinking: boolean, 
  modelId: string,
  forcedParentId?: string | null
): Promise<{ stream: ReadableStream, headers: Record<string, string>, uiSessionId: string }> {
  
  console.log(`[Qwen] Creating stream | Prompt: ${prompt.length} chars | Thinking: ${enableThinking}`);
  
  const { headers, chatSessionId, parentMessageId } = await getQwenHeaders(forcedParentId === null);

  let actualParentId: string | null = parentMessageId;
  
  if (forcedParentId !== undefined) {
    actualParentId = forcedParentId;
  } else if (chatSessionId && sessionStates[chatSessionId] !== undefined) {
    actualParentId = sessionStates[chatSessionId];
  }

  const timestamp = Math.floor(Date.now() / 1000);
  const fid = uuidv4();
  const model = modelId.replace('-no-thinking', '');

  // ✅ Separar system e user content
  const { systemContent, userContent } = splitPrompt(prompt);
  
  console.log(`[Qwen] System: ${systemContent.length} chars | User: ${userContent.length} chars`);

  // ✅ Truncar se necessário para evitar falha silenciosa do Qwen
  let finalContent = prompt;

  if (prompt.length > QWEN_SAFE_PROMPT_LIMIT) {
    // ✅ Estratégia: comprimir system e preservar user message completa
    if (userContent && systemContent) {
      const maxSystemChars = Math.max(QWEN_SAFE_PROMPT_LIMIT - userContent.length - 100, 2000);
      const truncatedSystem = systemContent.length > maxSystemChars
        ? systemContent.substring(0, maxSystemChars) + '\n\n[instrucoes truncadas]\n\n'
        : systemContent;
      
      finalContent = `${truncatedSystem}\n\nUser: ${userContent}`;
      console.log(`[Qwen] Truncated prompt: ${prompt.length} → ${finalContent.length} chars`);
    } else {
      finalContent = truncatePrompt(prompt, QWEN_SAFE_PROMPT_LIMIT);
    }
  }

  const payload: QwenPayload = {
    stream: true,
    version: '2.1',
    incremental_output: true,
    chat_id: chatSessionId || null,
    chat_mode: 'normal',
    model: model,
    parent_id: actualParentId,
    messages: [
      {
        fid: fid,
        parentId: actualParentId,
        childrenIds: [],
        role: 'user',
        content: finalContent,  // ✅ Usar content truncado/otimizado
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
          auto_search: false,    // ✅ CRÍTICO: Desativar auto_search para evitar timeout
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

  const url = chatSessionId 
    ? `https://chat.qwen.ai/api/v2/chat/completions?chat_id=${chatSessionId}`
    : 'https://chat.qwen.ai/api/v2/chat/completions';

  console.log(`[Qwen] Sending to ${url} | Final prompt: ${finalContent.length} chars | auto_search: false`);

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'accept': 'application/json',
      'accept-language': 'pt-BR,pt;q=0.9',
      'content-type': 'application/json',
      'cookie': headers['cookie'],
      'origin': 'https://chat.qwen.ai',
      'referer': chatSessionId ? `https://chat.qwen.ai/c/${chatSessionId}` : 'https://chat.qwen.ai/',
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

  console.log(`[Qwen] Response status: ${response.status} | Has body: ${!!response.body}`);

  if (!response.ok || !response.body) {
    const errText = await response.text().catch(() => '');
    console.error(`[Qwen] Error response: ${errText.substring(0, 500)}`);
    throw new Error(`Failed to fetch from Qwen: ${response.status} ${response.statusText} - ${errText}`);
  }

  // ✅ Verificar se response está retornando conteúdo imediatamente
  const contentLength = response.headers.get('content-length');
  const contentType = response.headers.get('content-type');
  console.log(`[Qwen] Content-Type: ${contentType} | Content-Length: ${contentLength}`);

  return { stream: response.body, headers, uiSessionId: chatSessionId };
}
