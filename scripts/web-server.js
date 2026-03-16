const http = require('http');
const PORT = process.env.PORT || 10000;
http.createServer((req, res) => {
  res.end('OK');
}).listen(PORT, () => {
  console.log(`Web server running on port ${PORT}`);
});
