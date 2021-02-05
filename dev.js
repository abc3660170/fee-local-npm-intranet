var localNpm = require("./lib");
localNpm({
    port: 5080,
    pouchPort: 6789,
    remote: 'https://registry.npmjs.org',
    remoteSkim: 'https://replicate.npmjs.com',
    url: 'http://10.0.4.51:5080',
    directory: './tmp/data'
});
//process.env.UV_THREADPOOL_SIZE = 1000;
// process.on('request', (msg) => {
//     console.log(msg);
// });