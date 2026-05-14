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
    const isStream = body.stream ?? false;
    
    // Extract the prompt
    let prompt = '';
    const messages = body.messages || [];
    let systemPrompt = '';
    
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
        if (msg.role === 'user') {
          prompt += `User: ${contentStr}\n\n`;
        } else if (msg.role === 'assistant') {
          let assistantContent = contentStr;
          if ((msg as any).reasoning_content) {
            assistantContent = `<think>\n${(msg as any).reasoning_content}\n</think>\n${assistantContent}`;
          }
          prompt += `Assistant: ${assistantContent.trim()}\n\n`;
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
                      const { text } = toolParser.feed(vStr);
                      if (text) await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({ content: text })] });
                    }
                  }
                }
              }
            } catch (e) {}
          }
        }
        await writeEvent({ id: completionId, object: 'chat.completion.chunk', created: Math.floor(Date.now() / 1000), model: body.model, choices: [makeChoice({}, 'stop')] });
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

      return c.json({
        id: completionId,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{
          index: 0,
          message: {
            role: 'assistant',
            content: fullContent,
            reasoning_content: fullReasoning || undefined
          },
          finish_reason: 'stop'
        }],
        usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
      });
    }
  } catch (err: any) {
    console.error('Error in chatCompletions:', err);
    return c.json({ error: { message: err.message, type: 'server_error' } }, 500);
  }
}
