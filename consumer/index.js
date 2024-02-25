const express = require('express');
const bodyParser = require('body-parser');

const app = express();
const port = 3000;

app.use(bodyParser.json());

app.get('/consumer', (req, res) => {
    res.json("consumer passed");
});

app.listen(port, () => {
    console.log(`CRUD server listening at http://localhost:${port}/consumer`);
});
