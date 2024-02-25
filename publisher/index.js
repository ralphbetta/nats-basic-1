const express = require('express');
const bodyParser = require('body-parser');
const AppNATService = require('./nat.class');
const cron = require('node-cron');

const app = express();
const port = 3001;

app.use(bodyParser.json());

const natsconfig = {
    serverURL: "nats://localhost:4222",
    clusterID: "test-cluster",
    clientID: "abc-service",
    channela: "channela",
    channelb: "channelb",
    channelc: "channelc",
}

const NATService = new AppNATService(natsconfig.serverURL, natsconfig.clusterID, natsconfig.clientID);

NATService.connect().then( async (nc) => {

    // PUBLISH
    cron.schedule('* * * * * *', (x) => {
        console.log('Publishing a broadcast of', x.getSeconds());
        NATService.publishMessage(natsconfig.channela, `broadcast ${x.getSeconds()} stream`)
    });

    cron.schedule('* * * * * *', (x) => {
        console.log('Publishing a broadcast of', x.getSeconds());
        NATService.publishMessage(natsconfig.channelb, `broadcast ${x.getSeconds()} stream`)
    });


    // CONSUME
    // NATService.consume(natsconfig.channela, (action, data)=>{});

})

app.get('/publisher', (req, res) => {
    res.json("publisher passed");
});

app.listen(port, () => {
    console.log(`CRUD server listening at http://localhost:${port}/publisher`);
});
