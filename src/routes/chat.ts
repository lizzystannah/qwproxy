/*
 * File: chat.ts
 * Project: qwenproxy
 * Author: Pedro Farias
 * Created: 2026-05-09
 */

import { Context } from 'hono';
import { stream as honoStream } from 'hono/streaming';
import { v4 as uuidv4 } from 'uuid';
import { createQwenStream, updateSessionParent } from '../services/qwen.ts';
import { OpenAIRequest } from '../utils/types.ts';
import { StreamingToolParser } from '../tools/parser.ts';

function getIncrementalDelta(oldStr: string, newStr: string): string {
  if (!oldStr) return newStr;
  if (newStr === oldStr) return '';
  if (newStr.startsWith(oldStr)) return newStr.substring(oldStr.length);
  return newStr;
}

export async function chatCompletions(c: Context) {
  try {
    const body: OpenAIRequest = await c.req.json();
    console.log(`[Request] POST ${c.req.path} | Model: ${body.model} | Stream: ${body.stream}`);
    if ((body as any).tools) {
      console.log(`[Tools] Available: ${(body as any).tools.map((t: any) => t.function?.name).join(', ')}`);
    }
    const bodyAny = body as any;
    
    // Extract the prompt
    let prompt = '';
    const messages = body.messages || [];
    let systemPrompt = '';
    
    // Process tools if present
    let toolsJson = '';
    if (bodyAny.tools && Array.isArray(bodyAny.tools) && bodyAny.tools.length > 0) {
      const formattedTools = bodyAny.tools.map((t: any) => {
        if (t.type === 'function') {
          return {
            name: t.function.name,
            description: t.function.description || '',
            parameters: t.function.parameters
          };
        }
        return t;
      });
      toolsJson = JSON.stringify(formattedTools, null, 2);
    }

    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];
      let contentStr = '';
      if (Array.isArray(msg.content)) {
        contentStr = msg.content.map((c: any) => c.text || JSON.stringify(c)).join('\n');
      } else if (typeof msg.content === 'object' && msg.content !== null) {
        contentStr = JSON.stringify(msg.content);
      } else {
        contentStr = msg.content || '';
      }

      if (msg.role === 'system') {
        systemPrompt += contentStr + '\n\n';
      } else if (i === messages.length - 1) {
        // Inject tools instructions right before the last message
        if (toolsJson) {
          systemPrompt += `\n\n# TOOLS AVAILABLE\nYou have access to the following tools:\n${toolsJson}\n\n# TOOL CALLING FORMAT (MANDATORY)\nTo use a tool, you MUST output a JSON object wrapped EXACTLY in these tags:\n<tool_call>\n{"name": "tool_name", "arguments": {"param_name": "value"}}\n</tool_call>\n\nEXAMPLE OF MULTIPLE TOOL CALLS:\n<tool_call>\n{"name": "read_file", "arguments": {"path": "file1.txt"}}\n</tool_call>\n<tool_call>\n{"name": "read_file", "arguments": {"path": "file2.txt"}}\n</tool_call>\n\nCRITICAL RULES:\n1. ONLY use the tags above for tool calling. NEVER output raw JSON without tags.\n2. You can call multiple tools by outputting multiple <tool_call> blocks consecutively.\n3. Do NOT output any other text (explanations, chat, etc.) after your <tool_call> blocks. Wait for the user to provide the tool response.\n4. The JSON inside the tags MUST be valid and include ALL required braces and the "arguments" field.\n5. If you need to use a tool, do it IMMEDIATELY without preamble.\n\n`;
          
          if (bodyAny.tool_choice && typeof bodyAny.tool_choice === 'object' && bodyAny.tool_choice.function) {
            const forcedTool = bodyAny.tool_choice.function.name;
            systemPrompt += `CRITICAL: You MUST call the tool "${forcedTool}" in this response.\n\n`;
          }
        }
        
        if (msg.role === 'user') {
          prompt += `User: ${contentStr}\n\n`;
        } else if (msg.role === 'assistant') {
          let assistantContent = contentStr;
          if ((msg as any).reasoning_content) {
            assistantContent = `<think>\n${(msg as any).reasoning_content}\n</think>\n${assistantContent}`;
          }
          if (msg.tool_calls && Array.isArray(msg.tool_calls)) {
             for (const tc of msg.tool_calls) {
               let args = tc.function?.arguments || '{}';
               if (typeof args !== 'string') args = JSON.stringify(args);
               assistantContent += `\n<tool_call>{"name": "${tc.function?.name}", "arguments": ${args}}</tool_call>`;
             }
          }
          prompt += `Assistant: ${assistantContent.trim()}\n\n`;
        } else if (msg.role === 'tool' || msg.role === 'function') {
          prompt += `Tool Response (${msg.name || 'tool'}): ${contentStr}\n\n`;
        }
      }
    }

    const finalPrompt = systemPrompt ? `${systemPrompt}\n${prompt}` : prompt;
    const isThinkingModel = !body.model.includes('no-thinking');
    const isNewSession = !messages.some(m => m.role === 'assistant');

    const result = await createQwenStream(finalPrompt, isThinkingModel, body.model, isNewSession ? null : undefined);
    const stream = result.stream;
    const uiSessionId = result.uiSessionId;
    const completionId = 'chatcmpl-' + uuidv4();

    if (isStream) {
      c.header('Content-Type', 'text/event-stream');
      c.header('Cache-Control', 'no-cache');
      c.header('Connection', 'keep-alive');

      return honoStream(c, async (streamWriter: any) => {
        const writeEvent = async (data: any) => {
          await streamWriter.write(`data: ${JSON.stringify(data)}\n\n`);
        };
        const makeChoice = (delta: any, finishReason: string | null = null) => ({
          index: 0,
          delta,
          logprobs: null,
          finish_reason: finishReason
        });

        await writeEvent({
          id: completionId,
          object: 'chat.completion.chunk',
          created: Math.floor(Date.now() / 1000),
          model: body.model,
          choices: [makeChoice({ role: 'assistant', content: '' })]
        });

        const reader = stream.getReader();
        const decoder = new TextDecoder();
        let lastFullContent = '';
        const toolParser = new StreamingToolParser();
        let buffer = '';
        let currentThoughtIndex = 0;

        while (true) {
          const { done, value } = await reader.read();
          if (done) break;

          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';

          for (const line of lines) {
            const trimmed = line.trim();
            if (!trimmed || !trimmed.startsWith('data: ')) continue;
            const dataStr = trimmed.slice(6);
            if (dataStr === '[DONE]') continue;

            try {
              const chunk = JSON.parse(dataStr);
              if (chunk.response_id) updateSessionParent(uiSessionId, chunk.response_id);

              if (chunk.choices && chunk.choices[0] && chunk.choices[0].delta) {
                const delta = chunk.choices[0].delta;
                
                if (delta.phase === 'thinking_summary') {
                   if (delta.extra?.summary_thought?.content) {
                      const thoughts = delta.extra.summary_thought.content;
                      if (thoughts.length > currentThoughtIndex) {
                        const vStr = thoughts.slice(currentThoughtIndex).join('\n');
                        currentThoughtIndex = thoughts.length;
                        await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({ reasoning_content: vStr })] });
                      }
                   }
                } else if (delta.phase === 'answer') {
                  if (delta.content !== undefined) {
                    const vStr = getIncrementalDelta(lastFullContent, delta.content);
                    if (vStr) {
                      lastFullContent += vStr;
                      const { text, toolCalls } = toolParser.feed(vStr);
                      if (text) await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({ content: text })] });
                      
                      for (const tc of toolCalls) {
                        await writeEvent({
                          id: completionId,
                          object: 'chat.completion.chunk',
                          created: Math.floor(Date.now() / 1000),
                          model: body.model,
                          choices: [makeChoice({
                            tool_calls: [{
                              index: toolParser.getEmittedToolCallCount() - toolCalls.length + toolCalls.indexOf(tc),
                              id: tc.id,
                              type: 'function',
                              function: {
                                name: tc.name,
                                arguments: JSON.stringify(tc.arguments)
                              }
                            }]
                          })]
                        });
                      }
                    }
                  }
                }
              }
            } catch (e) {}
          }
        }

        // Finalize tool parser for streaming
        const { text: remText, toolCalls: remTools } = toolParser.flush();
        if (remText) await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({ content: remText })] });
        for (const tc of remTools) {
          await writeEvent({
            id: completionId,
            object: 'chat.completion.chunk',
            created: Math.floor(Date.now() / 1000),
            model: body.model,
            choices: [makeChoice({
              tool_calls: [{
                index: toolParser.getEmittedToolCallCount() - remTools.length + remTools.indexOf(tc),
                id: tc.id,
                type: 'function',
                function: {
                  name: tc.name,
                  arguments: JSON.stringify(tc.arguments)
                }
              }]
            })]
          });
        }

        const finalFinishReason = toolParser.getEmittedToolCallCount() > 0 ? 'tool_calls' : 'stop';
        await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({}, finalFinishReason)] });
        await streamWriter.write('data: [DONE]\n\n');
      });
    } else {
      // NON-STREAMING
      const reader = stream.getReader();
      const decoder = new TextDecoder();
      let fullContent = '';
      let fullReasoning = '';
      let buffer = '';

      let lastAnswerContent = '';
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || !trimmed.startsWith('data: ') || trimmed.includes('[DONE]')) continue;
          try {
            const chunk = JSON.parse(trimmed.slice(6));
            const delta = chunk.choices?.[0]?.delta;
            if (delta?.phase === 'answer' && delta.content !== undefined) {
              const vStr = getIncrementalDelta(lastAnswerContent, delta.content);
              if (vStr) {
                fullContent += vStr;
                lastAnswerContent = delta.content;
              }
            }
            if (delta?.phase === 'thinking_summary' && delta.extra?.summary_thought?.content) {
              fullReasoning = delta.extra.summary_thought.content.join('\n');
            }
          } catch (e) {}
        }
      }

      // Process the whole content at once for tools
      const toolParser = new StreamingToolParser();
      toolParser.feed(fullContent);
      const { text: cleanText, toolCalls: callsFromFeed } = toolParser.flush();
      
      const promptTokens = Math.ceil(finalPrompt.length / 4);
      const completionTokens = Math.ceil((cleanText || fullContent || '').length / 4);

      const responseBody = {
        id: completionId,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{
          index: 0,
          message: {
            role: 'assistant',
            content: (callsFromFeed.length > 0) ? "" : (cleanText || fullContent || ""),
            reasoning_content: fullReasoning || undefined,
            tool_calls: callsFromFeed.length > 0 ? callsFromFeed.map((tc, idx) => ({
              id: tc.id,
              type: 'function',
              function: {
                name: tc.name,
                arguments: JSON.stringify(tc.arguments)
              }
            })) : undefined
          },
          finish_reason: callsFromFeed.length > 0 ? 'tool_calls' : 'stop'
        }],
        usage: { 
          prompt_tokens: promptTokens, 
          completion_tokens: completionTokens, 
          total_tokens: promptTokens + completionTokens 
        }
      };

      console.log(`[Response] ${completionId} | Content Length: ${responseBody.choices[0].message.content?.length || 0} | Tools: ${callsFromFeed.length}`);
      return c.json(responseBody);
    }
  } catch (err: any) {
    console.error('Error in chatCompletions:', err);
    return c.json({ error: { message: err.message, type: 'server_error' } }, 500);
  }
}
