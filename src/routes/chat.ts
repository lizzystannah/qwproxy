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
    const isStream = body.stream === true;
    
    // Extract the prompt
    let prompt = '';
    const messages = body.messages || [];
    let systemPrompt = '';
    
    // Process tools if present
    let toolsJson = '';
    const hasTools = bodyAny.tools && Array.isArray(bodyAny.tools) && bodyAny.tools.length > 0;
    
    if (hasTools) {
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

    console.log(`[Qwen] Thinking Model: ${isThinkingModel} | New Session: ${isNewSession}`);
    console.log(`[Prompt Preview] "${finalPrompt.substring(0, 200)}..."`);

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
          delta: delta,
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
        const toolParser = hasTools ? new StreamingToolParser() : null;
        let buffer = '';
        let currentThoughtIndex = 0;
        let totalContentEmitted = '';
        let rawContentAccumulated = '';
        let chunksReceived = 0;
        let answerChunks = 0;
        let thinkingChunks = 0;

        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            console.log(`[Stream] Done. Chunks: ${chunksReceived} | Thinking: ${thinkingChunks} | Answer: ${answerChunks}`);
            break;
          }

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
              chunksReceived++;
              
              if (chunk.response_id) updateSessionParent(uiSessionId, chunk.response_id);

              if (chunk.choices && chunk.choices[0] && chunk.choices[0].delta) {
                const delta = chunk.choices[0].delta;
                
                // ✅ LOG DETALHADO
                console.log(`[Chunk #${chunksReceived}] Phase: ${delta.phase || 'none'} | Has Content: ${delta.content !== undefined} | Content Length: ${delta.content?.length || 0}`);
                
                if (delta.phase === 'thinking_summary') {
                   thinkingChunks++;
                   if (delta.extra?.summary_thought?.content) {
                      const thoughts = delta.extra.summary_thought.content;
                      if (thoughts.length > currentThoughtIndex) {
                        currentThoughtIndex = thoughts.length;
                        console.log(`[Thinking] Received ${thoughts.length - currentThoughtIndex} thoughts`);
                      }
                   }
                } else if (delta.phase === 'answer') {
                  answerChunks++;
                  
                  if (delta.content !== undefined) {
                    const vStr = getIncrementalDelta(lastFullContent, delta.content);
                    console.log(`[Answer Chunk] Delta: "${vStr.substring(0, 50)}${vStr.length > 50 ? '...' : ''}" (${vStr.length} chars)`);
                    
                    if (vStr) {
                      lastFullContent += vStr;
                      rawContentAccumulated += vStr;
                      
                      if (toolParser) {
                        const { text, toolCalls } = toolParser.feed(vStr);
                        
                        if (text) {
                          totalContentEmitted += text;
                          await writeEvent({ 
                            id: completionId, 
                            object: 'chat.completion.chunk', 
                            created: Math.floor(Date.now() / 1000), 
                            model: body.model, 
                            choices: [makeChoice({ content: text })] 
                          });
                        }
                        
                        for (const tc of toolCalls) {
                          const argsString = typeof tc.arguments === 'string' 
                            ? tc.arguments 
                            : JSON.stringify(tc.arguments);
                          
                          const toolCallIndex = toolParser.getEmittedToolCallCount() - toolCalls.length + toolCalls.indexOf(tc);
                          
                          console.log(`[Tool Call Stream] #${toolCallIndex} ${tc.name}`);
                          
                          await writeEvent({
                            id: completionId,
                            object: 'chat.completion.chunk',
                            created: Math.floor(Date.now() / 1000),
                            model: body.model,
                            choices: [makeChoice({
                              tool_calls: [{
                                index: toolCallIndex,
                                id: tc.id,
                                type: 'function',
                                function: {
                                  name: tc.name,
                                  arguments: argsString
                                }
                              }]
                            })]
                          });
                        }
                      } else {
                        totalContentEmitted += vStr;
                        await writeEvent({ 
                          id: completionId, 
                          object: 'chat.completion.chunk', 
                          created: Math.floor(Date.now() / 1000), 
                          model: body.model, 
                          choices: [makeChoice({ content: vStr })] 
                        });
                      }
                    }
                  } else {
                    console.log(`[Answer Chunk] No content in delta`);
                  }
                } else {
                  console.log(`[Unknown Phase] ${delta.phase || 'undefined'}`);
                }
              } else {
                console.log(`[Chunk #${chunksReceived}] No delta or choices`);
              }
            } catch (e) {
              console.error('[Stream Parse Error]', e, 'Line:', trimmed.substring(0, 100));
            }
          }
        }

        // ✅ Finalizar
        if (toolParser) {
          const { text: remText, toolCalls: remTools } = toolParser.flush();
          
          if (remText) {
            totalContentEmitted += remText;
            await writeEvent({ 
              id: completionId, 
              object: 'chat.completion.chunk', 
              created: Math.floor(Date.now() / 1000), 
              model: body.model, 
              choices: [makeChoice({ content: remText })] 
            });
          }
          
          for (const tc of remTools) {
            const argsString = typeof tc.arguments === 'string' 
              ? tc.arguments 
              : JSON.stringify(tc.arguments);
            
            const toolCallIndex = toolParser.getEmittedToolCallCount() - remTools.length + remTools.indexOf(tc);
            
            console.log(`[Tool Call Final] #${toolCallIndex} ${tc.name}`);
            
            await writeEvent({
              id: completionId,
              object: 'chat.completion.chunk',
              created: Math.floor(Date.now() / 1000),
              model: body.model,
              choices: [makeChoice({
                tool_calls: [{
                  index: toolCallIndex,
                  id: tc.id,
                  type: 'function',
                  function: {
                    name: tc.name,
                    arguments: argsString
                  }
                }]
              })]
            });
          }
          
          const totalToolCalls = toolParser.getEmittedToolCallCount();
          const finalFinishReason = totalToolCalls > 0 ? 'tool_calls' : 'stop';
          
          console.log(`[Stream Complete] Raw: ${rawContentAccumulated.length} chars | Emitted: ${totalContentEmitted.length} chars | Tools: ${totalToolCalls}`);
          
          await writeEvent({ 
            id: completionId, 
            object: 'chat.completion.chunk', 
            created: Math.floor(Date.now() / 1000), 
            model: body.model, 
            choices: [makeChoice({}, finalFinishReason)] 
          });
        } else {
          console.log(`[Stream Complete - No Tools] Content: ${totalContentEmitted.length} chars`);
          
          await writeEvent({ 
            id: completionId, 
            object: 'chat.completion.chunk', 
            created: Math.floor(Date.now() / 1000), 
            model: body.model, 
            choices: [makeChoice({}, 'stop')] 
          });
        }
        
        await streamWriter.write('data: [DONE]\n\n');
      });
    } else {
      // ✅ NON-STREAMING com logs detalhados
      const reader = stream.getReader();
      const decoder = new TextDecoder();
      let fullContent = '';
      let fullReasoning = '';
      let buffer = '';
      let lastAnswerContent = '';
      let chunksReceived = 0;
      let answerChunks = 0;
      let thinkingChunks = 0;

      console.log(`[Non-Stream] Starting to read stream...`);

      while (true) {
        const { done, value } = await reader.read();
        if (done) {
          console.log(`[Non-Stream] Done. Chunks: ${chunksReceived} | Thinking: ${thinkingChunks} | Answer: ${answerChunks}`);
          break;
        }
        
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        
        for (const line of lines) {
          const trimmed = line.trim();
          if (!trimmed || !trimmed.startsWith('data: ') || trimmed.includes('[DONE]')) continue;
          
          try {
            const chunk = JSON.parse(trimmed.slice(6));
            chunksReceived++;
            const delta = chunk.choices?.[0]?.delta;
            
            // ✅ LOG CADA CHUNK
            if (delta) {
              console.log(`[Chunk #${chunksReceived}] Phase: ${delta.phase || 'none'} | Has Content: ${delta.content !== undefined}`);
            }
            
            if (delta?.phase === 'answer' && delta.content !== undefined) {
              answerChunks++;
              const vStr = getIncrementalDelta(lastAnswerContent, delta.content);
              if (vStr) {
                fullContent += vStr;
                lastAnswerContent = delta.content;
                console.log(`[Answer] +${vStr.length} chars | Total: ${fullContent.length}`);
              }
            }
            
            if (delta?.phase === 'thinking_summary' && delta.extra?.summary_thought?.content) {
              thinkingChunks++;
              fullReasoning = delta.extra.summary_thought.content.join('\n');
              console.log(`[Thinking] ${fullReasoning.length} chars`);
            }
          } catch (e) {
            console.error('[Non-Stream Parse Error]', e);
          }
        }
      }

      console.log(`[Non-Stream Raw Content] "${fullContent.substring(0, 200)}..." (${fullContent.length} chars)`);

      // ✅ Processar tool calls APENAS se houver tools disponíveis
      let cleanText = fullContent;
      let callsFromFeed: any[] = [];
      
      if (hasTools) {
        const toolParser = new StreamingToolParser();
        toolParser.feed(fullContent);
        const result = toolParser.flush();
        cleanText = result.text;
        callsFromFeed = result.toolCalls;
        
        console.log(`[Tool Parser Result] Clean: "${cleanText.substring(0, 100)}..." (${cleanText.length} chars) | Tool Calls: ${callsFromFeed.length}`);
      }
      
      const promptTokens = Math.ceil(finalPrompt.length / 4);
      const completionTokens = Math.ceil((cleanText || fullContent).length / 4);

      // ✅ Formatar tool_calls
      const formattedToolCalls = callsFromFeed.map((tc) => {
        const argsString = typeof tc.arguments === 'string' 
          ? tc.arguments 
          : JSON.stringify(tc.arguments);
        
        return {
          id: tc.id,
          type: 'function' as const,
          function: {
            name: tc.name,
            arguments: argsString
          }
        };
      });

      // ✅ GARANTIR que content NUNCA seja vazio quando não há tool calls
      const hasToolCallsInResponse = formattedToolCalls.length > 0;
      let finalContent: string | null;
      
      if (hasToolCallsInResponse) {
        finalContent = null;
      } else {
        finalContent = cleanText || fullContent || "Desculpe, não consegui gerar uma resposta.";
      }

      const message: any = {
        role: 'assistant',
        content: finalContent
      };

      if (hasToolCallsInResponse) {
        message.tool_calls = formattedToolCalls;
      }

      const responseBody = {
        id: completionId,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: body.model,
        choices: [{
          index: 0,
          message: message,
          finish_reason: hasToolCallsInResponse ? 'tool_calls' : 'stop'
        }],
        usage: { 
          prompt_tokens: promptTokens, 
          completion_tokens: completionTokens, 
          total_tokens: promptTokens + completionTokens 
        }
      };

      console.log(`[Response] Content: ${finalContent === null ? 'null' : `"${finalContent.substring(0, 100)}..." (${finalContent.length} chars)`} | Tools: ${formattedToolCalls.length}`);
      
      if (formattedToolCalls.length > 0) {
        console.log(`[Tool Calls]`, JSON.stringify(formattedToolCalls, null, 2));
      }
      
      return c.json(responseBody);
    }
  } catch (err: any) {
    console.error('❌ Error in chatCompletions:', err);
    return c.json({ 
      error: { 
        message: err.message || 'Internal server error', 
        type: 'server_error',
        code: 'internal_error'
      } 
    }, 500);
  }
}
