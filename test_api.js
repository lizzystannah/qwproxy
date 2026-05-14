import fetch from 'node-fetch';

const API_URL = 'https://quantterm-qwen.ikicmr.easypanel.host/v1/chat/completions';
const API_KEY = 'SUA_CHAVE_AQUI'; // Coloque a chave que você definiu no Easypanel

async function test() {
  console.log('Enviando requisição para a API...');
  
  try {
    const response = await fetch(API_URL, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${API_KEY}`
      },
      body: JSON.stringify({
        model: 'qwen3.6-plus',
        messages: [{ role: 'user', content: 'responda apenas: "Teste OK"' }],
        stream: false
      })
    });

    const data = await response.json();
    console.log('02199503090909060854F');
    console.log(JSON.stringify(data, null, 2));
    console.log('-----------------------\n');

  } catch (err) {
    console.error('Erro ao testar API:', err.message);
  }
}

test();
