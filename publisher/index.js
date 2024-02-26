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
    channela: "channel.a",
    channelb: "channel.b",
    channelc: "channel.c",
    alpha: "alpha.subject",
    betta: "betta.subject",
    gamma: "gamma.subject",
}

const NATService = new AppNATService(natsconfig.serverURL, natsconfig.clusterID, natsconfig.clientID);

NATService.connect().then( async (nc) => {

    // PUBLISH
    cron.schedule('* * * * * *', (x) => {
        console.log('Publisher log', x.getSeconds(), "for cha");
        NATService.publishMessage(natsconfig.channela, `publshed data ${x.getSeconds()}`)
    });

    // cron.schedule('* * * * * *', (x) => {
    //     console.log('Publisher log', x.getSeconds(), "for chb");
    //     NATService.publishMessage(natsconfig.channelb, `broadcast ${x.getSeconds()} stream to chb`)
    // });

    cron.schedule('* * * * * *', (x) => {
        console.log(`pub - ${natsconfig.channelc} :`, x.getSeconds());
        NATService.requestAction(natsconfig.channelc, `broadcast chb ${x.getSeconds()}`)
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
