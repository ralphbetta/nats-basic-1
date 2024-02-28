const { connect, JSONCodec, StringCodec, Empty, ErrorCode, AckPolicy } = require('nats');


class NATJSMService {
    // NAT Jetstream Manager Service

    constructor(serverURL, clusterID, clientID) {
        this.serverURL = serverURL;
        this.clusterID = clusterID;
        this.clientID = clientID;
        this.nc = null;
        this.jsm = null
        this.subscription = null;
        this.sc = StringCodec();
        this.jc = JSONCodec();
        this.streamNames = ["mystream", "streamone", "streamtwo", "streamthree", "streamfour"];
        this.streamSubjects = [`mystream.*`, "a.y", "a.x", "x.y"];
        this.consumers = ["consumerone", "consumertwo", "consumerthree", "consumerfour"]; //same as durable_name
    }

    async connect() {
        try {

            let connectionOptions = {
                servers: this.serverURL,
                jetstream: true,
            };

            this.nc = await connect(connectionOptions);

            console.log(`Connected to ${this.serverURL}`);
            this.jsm = await this.nc.jetstreamManager();

            // create a jetstream client:
            this.js = this.nc.jetstream();

            return this.nc;
        } catch (err) {
            console.error(`Error connecting to ${this.serverURL}: ${err.message}`);
            throw err; // Re-throw the error to let the caller handle it
        }
    }

    async addStream() {
        // add a stream - jetstream can capture nats core messages
        await this.jsm.streams.add({ name: this.streamNames[2], subjects: [this.streamSubjects[2]] });

        for (let i = 0; i < 3; i++) {
            this.nc.publish(`${this.streamSubjects[0]}.a`, Empty);
        }
    }


    async allStreams() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        // list all the streams, the `next()` function
        // retrieves a paged result.
        const streams = await this.jsm.streams.list().next();
        streams.forEach((si, index) => {
            console.log(index, si);
        });
    }

    async findStreamBySubject() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        try {
            // find a stream that stores a specific subject:
            const streamName = await this.jsm.streams.find(this.streamSubjects[2]);
            // retrieve info about the stream by its name
            const streamInfo = await this.jsm.streams.info(streamName);
            console.log(streamName, streamInfo.config.subjects);

        } catch (e) {
            console.log("Error: ", e.message)
        }

    }

    async updateStream() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        try {
            const streamName = await this.jsm.streams.find(this.streamSubjects[2]);
            const streamInfo = await this.jsm.streams.info(streamName);
            streamInfo.config.subjects?.push(this.streamSubjects[1]);
            await this.jsm.streams.update(streamName, streamInfo.config);
            console.log(this.streamSubjects[1]);
        } catch (err) {
            console.log("Error:", err.message);
        }
    }

    async deleteStream() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        try {
            await this.jsm.streams.delete(this.streamNames[1]);
        } catch (err) {
            console.log("Error", err.message)
        }
    }

    async deleteStreamMessage() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        try {
            await this.jsm.streams.deleteMessage(this.streamNames[2], 0);
        } catch (err) {
            console.log("Errord", err.message)
        }
    }

    async purgeStreamMessage() {
        if (!this.jsm) {
            console.error("Not connected to NATS server");
            return;
        }
        await this.jsm.streams.purge(this.streamNames[2], { filter: this.streamSubjects[2] });
    }

    async streamConsumers() {
        try {
            // list all consumers for a stream:
            const consumers = await this.jsm.consumers.list(this.streamNames[2]).next();
            consumers.forEach((ci, index) => {
                console.log(index, ci);
            });
        } catch (err) {
            console.log("StreamConsumerError", err.message)
        }
    }

    async addDurableConsumer() {
        // add a new durable consumer
        await this.jsm.consumers.add(this.streamNames[2], {
            durable_name: this.consumers[2],
            ack_policy: AckPolicy.Explicit,
        });
    }
    async deleteDurableConsumer() {
        try {
            const response = await this.jsm.consumers.delete(this.streamNames[2], this.consumers[2]);
        } catch (err) {
            console.log("Delete Durable Error", err.message)
        }

    }

    async consumerInfo() {
        const ci = await this.jsm.consumers.info(this.streamNames[2], this.consumers[2]);
        console.log("consumer info:", ci);
    }

    async consumerAcknowlegement() {

        /*
        Multiple ways to acknowledge a message:
        ack()
        nak(millis?) - like ack, but tells the server you failed to process it, and it should be resent. If a number is specified, the message will be resent after the specified value. The additional argument only supported on server versions 2.7.1 or greater
        working() - informs the server that you are still working on the message and thus prevent receiving the message again as a redelivery.
        term() - specifies that you failed to process the message and instructs the server to not send it again (to any consumer).
        */

        // list all consumers for a stream:
        const c = await this.js.consumers.get(this.streamNames[2], this.consumers[2]);
        const m = await c.next();
        if (m) {
            console.log(m.subject);
           const ack =  m.ack();

        } else {
            console.log(`didn't get a message`);
        }
        console.log("Consumer:", c);
    }

    async messageAcknowledgement() {
        const c = await this.js.consumers.get(this.streamNames[2], this.consumers[2]);
        let messages = await c.fetch({ max_messages: 4, expires: 2000 });
        for await (const m of messages) {
            //   m.ack();
        }
        // console.log(messages);
        // console.log(`batch completed: ${messages.getPending()} msgs pending`);
        // console.log(`batch completed: ${messages.getReceived()} msgs receiving`);
        // console.log(`batch completed: ${messages.getProcessed()} msgs processed`);
    }


    async consumingMessage() {
        const c = await this.js.consumers.get(this.streamNames[2]); //this cannot be acknowledged
        const messages = await c.consume();
        // console.log("hello", messages)
        for await (const m of messages) {
            console.log(m);
            // m.ack();
        }
    }



    /*-------------------------------------
    JET STREAM CLIENTS
    -------------------------------------*/


    async publishStreamMessage() {
        const subject = `${this.streamSubject}.a`;

        // create a jetstream client:
        const js = this.nc.jetstream();
        // publish a message received by a stream
        try {
            let pa = await js.publish(subject);
            const stream = pa.stream;
            const seq = pa.seq;
            const duplicate = pa.duplicate;
            console.log(`got response:`);
        } catch (err) {
            console.log(`problem with request: ${err}`);
            switch (err.code) {
                case ErrorCode.NoResponders:
                    console.log("no one is listening to 'hello.world'");
                    break;
                case ErrorCode.Timeout:
                    console.log("someone is listening but didn't respond");
                    break;
                default:
                    console.log("request failed", err);
            }
        }

        console.log(subject);
    }

    async existingConsumer() {
        // retrieve an existing consumer
        // const c = await this.js.consumers.get(this.stream, this.stream);
        // getting an ordered consumer requires no name
        const oc = await this.js.consumers.get(this.streamName);

        console.log(oc)
    }



    async disconnect() {
        if (this.nc) {
            await this.nc.close();

            console.log("Disconnected from NATS server");
        }
    }




}

// documentation: https://github.com/nats-io/nats.deno/blob/main/jetstream.md

module.exports = NATJSMService