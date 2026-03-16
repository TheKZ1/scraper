const axios = require('axios');

async function check(url) {
  try {
    const response = await axios.get(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://firstcycling.com/'
      },
      timeout: 20000
    });
    const html = String(response.data || '').toLowerCase();
    console.log('URL:', url);
    console.log('contains prologue:', html.includes('prologue'));
    console.log('contains stage 1:', html.includes('stage 1'));
    console.log('contains stage 0:', html.includes('stage 0'));
    console.log('---');
  } catch (err) {
    console.log('URL:', url, 'ERR:', err.response ? err.response.status : err.message);
  }
}

(async () => {
  await check('https://firstcycling.com/widget/?r=1&y=2026&lang=EN&s=0'); // Tour Down Under
  await check('https://firstcycling.com/widget/?r=31&y=2026&lang=EN&s=0'); // UAE Tour
})();
