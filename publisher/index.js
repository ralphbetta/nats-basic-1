const { connect, JSONCodec, headers } = require('nats');

async function publishLogs() {
    const tokenNC = await connect({ervers: 'localhost:4222', user: 'jenny', pass: "232-432"});
    const securedNC = await connect({port: 4222, token: 't0pS3cret!'});

    const nc = await connect({ servers: 'nats://localhost:4222', jetstream: true });
    const jc = JSONCodec();
    const h = headers()
    h.append("id", "12345"),
    h.append("unix_time", Date.now().toString())

    const channelx = 'channelx'; // JetStream subject for logging

    // Log messages
    const logMessages = [
        { level: 'info', message: 'User logged in' },
        { level: 'error', message: 'Database connection failed' },
        { level: 'warning', message: 'Disk space low' }
    ];

    for (const msg of logMessages) {

         nc.publish(channelx, jc.encode(msg), {headers: h});

        console.log(`Published log message: ${msg}`);
    }

    await nc.flush();
    await nc.close();
}

publishLogs().catch(console.error);
