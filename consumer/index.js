const express = require('express');
const bodyParser = require('body-parser');
const AppNATService = require('./nat.class');

const app = express();
const port = 3000;

app.use(bodyParser.json());

const natsconfig = {
    serverURL: "nats://localhost:4222",
    clusterID: "test-cluster",
    clientID: "xyz-service",
    channela: "channel.a",
    channelb: "channel.b",
    channelc: "channel.c",
    alpha: "alpha.subject",
    betta: "betta.subject",
    gamma: "gamma.subject",
}
const NATService = new AppNATService(natsconfig.serverURL, natsconfig.clusterID, natsconfig.clientID);

NATService.connect().then( async (nc) => {

    NATService.consume(natsconfig.channela, (action, data)=>{});

    NATService.replyRequest(natsconfig.channelc, (action, data)=>{});
    
});

app.get('/consumer', (req, res) => {
    res.json("consumer passed");
});

app.listen(port, () => {
    console.log(`CRUD server listening at http://localhost:${port}/consumer`);
});
