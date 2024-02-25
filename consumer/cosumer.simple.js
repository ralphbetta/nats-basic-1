const { connect, JSONCodec } = require('nats');

async function subscribeToLogs() {
    const nc = await connect({ servers: 'nats://localhost:4222', jetstream: true });
    const jc = JSONCodec();

    const channelx = 'channela'; // JetStream subject for logging

    const sub = nc.subscribe(channelx, { durable_name: 'log-consumer' });
    const suby = nc.subscribe("time", {timeout: 1000});

    (async () => {
        for await (const msg of sub) {

            const msgdata = jc.decode(msg.data);
            console.log(msgdata);
            console.log(`Received log message: ${msgdata}`);
            
        }
    })().catch(console.error);
}

subscribeToLogs().catch(console.error);
