const { connect, JSONCodec, StringCodec, Empty, ErrorCode, AckPolicy } = require('nats');


class NATStreamManager {

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
      this.js = this.nc.jetstream();
      return this.nc;

    } catch (err) {
      console.error(`Error connecting to ${this.serverURL}: ${err.message}`);
      throw err;
    }
  }



  /*------------------------------
  MANAGER SECTIONS
  -------------------------------*/


  async createAddStream() {
    try {
      const stream = 'my_stream';
      const subj = `my_stream.*`;
      await this.jsm.streams.add({ name: stream, subjects: [subj] });
      console.log('Stream added:', stream);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }

  async readdAllStreams() {
    try {
      const streamIterator = this.jsm.streams.list();

      // Iterate over all streams
      for await (const stream of streamIterator) {
        console.log('Stream:', stream.config.name);
        console.log('Subjects:', stream.config.subjects);
        console.log('Storage:', stream.config.storage);
        console.log('Max messages:', stream.config.max_msgs);
        console.log('Max age:', stream.config.max_age);
        console.log('Max bytes:', stream.config.max_bytes);
        console.log('-----------------------------');

      }
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }


  async updateStreams() {
    try {
      const stream = 'my_stream';
      const subj = `my_stream.*`;
      const streamInfo = await this.jsm.streams.info(stream);
      streamInfo.config.subjects.push('new_subject.*');
      // await this.jsm.streams.update(streamInfo.config);
      await this.jsm.streams.update('my_stream', { max_msgs: 100 });
      console.log('Stream updated:', stream);

    } catch (error) {
      console.error('Error managing streams:', error);
    } finally {
      // nc.close();
    }
  }

  async deleteStream() {
    try {
      const stream = 'my_stream';
      const subj = `my_stream.*`;
      await this.jsm.streams.delete(stream);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }

  async deleteStreamMessage() {
    if (!this.jsm) {
      console.error("Not connected to NATS server");
      return;
    }
    try {
      const stream = 'my_stream';
      const subj = `my_stream.*`;
      const seq = 0
      await this.jsm.streams.deleteMessage(stream, seq, true);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }


  async addConsumerToStream() {
    const stream = 'my_stream';
    const consumer = 'my_consumer';
    const subj = `my_stream.*`;

    try {
      const completedConsumerInfo = await this.jsm.consumers.add(stream, {
        durable_name: consumer,
        // ack_policy: AckPolicy.Explicit,
        ack_policy: 'explicit',
      });

      console.log(completedConsumerInfo);

    } catch (error) {
      console.error('Error creating consumer', error.message);
    } finally {
      // nc.close();
    }
  }

  async updateConsumer() {
    const stream = 'my_stream';
    const subj = `my_stream.*`;
    const consumer = 'my_consumer';

    const newConfig = {
      max_deliver: 100,
      ack_wait: 30,
      start_time: new Date().toISOString(),
    };

    try {
      const completedConsumerInfo = await this.jsm.consumers.update(stream, consumer, newConfig);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }

  async consumerInfo() {
    const stream = 'my_stream';
    const subj = `my_stream.*`;
    const consumer = 'my_consumer';
    try {
      const ci = await this.jsm.consumers.info(stream, consumer);
      console.log("consumer info:", ci);
    } catch (error) {
      console.error('Error deleting stream consumer:', error.message);
    } finally {
      // nc.close();
    }
  }

  async deleteConsumer() {
    const stream = 'my_stream';
    const subj = `my_stream.*`;
    const consumer = 'my_consumer';
    try {
      const completed = await this.jsm.consumers.delete(stream, consumer);
      console.log(completed);
    } catch (error) {
      console.error('Error deleting stream consumer:', error.message);
    } finally {
      // nc.close();
    }
  }

  async readAllConsumerOfStream() {
    try {

      const stream = 'my_stream';

      // const consumerIterator = this.jsm.consumers.list(stream);
      // for await (const consumer of consumerIterator) {
      //   console.log('Consumer:', consumer.config.durable_name);
      //   console.log('Stream:', consumer.stream_name);
      //   console.log('Durable name:', consumer.config.durable_name);
      //   console.log('ack_wait', consumer.config.ack_wait);
      //   console.log('Ack policy:', consumer.config.ack_policy);
      //   console.log('-----------------------------');
      // }

      const consumers = await this.jsm.consumers.list(stream).next();
      consumers.forEach((consumer, index) => {
        // console.log(index, ci);
        console.log('Consumer:', consumer.config.durable_name);
        console.log('Stream:', consumer.stream_name);
        console.log('Durable name:', consumer.config.durable_name);
        console.log('ack_wait', consumer.config.ack_wait);
        console.log('Ack policy:', consumer.config.ack_policy);
        console.log(`------------ Consumer ${index}-----------------`);
      });

    } catch (error) {
      console.error('Error reading consumers:', error.message);
    } finally {
      // Close connection
      // nc.close();
    }
  }


  async readAllOrderedConsumerofStreams() {
    try {
      /*
      Returns the Consumer configured for the specified stream having the specified name. 
      Consumers are typically created with JetStreamManager.
      If no name is specified, the Consumers API will return an ordered consumer.
      An ordered consumer expects messages to be delivered in order.
      If there's any inconsistency, the ordered consumer will recreate the underlying consumer at the correct sequence.
      Note that ordered consumers don't yield messages that can be acked because the client can simply recreate the consumer.
      */
      const stream = 'my_stream';
      const consumer = 'my_consumer';
      const pulledConsumer = await this.js.consumers.get(stream, consumer);
      const orderedConsumer = await this.js.consumers.get(stream); //message can not be acknowleged

      const pulledConsumerInfo = await pulledConsumer.info();
      const orderedConsumerInfo = await orderedConsumer.info()

      console.log(pulledConsumerInfo, orderedConsumerInfo)


    } catch (error) {
      console.error('Error reading consumers:', error.message);
    } finally {
      // Close connection
      // nc.close();
    }
  }



  /*------------------------------
  CLIENT SIDE OF JETSTREAM
  -------------------------------*/


  async publishMessageToStream() {
    try {

      const stream = 'my_stream';
      const subj = `my_stream.*`;
      const subjA = 'my_stream.a';
      const payload = JSON.stringify({ data: `2 payload sent to subject: ${subjA}` });

      const publishAcknowlegment = await this.js.publish(subjA, payload);
      console.log('Published message:', publishAcknowlegment);

    } catch (err) {

      switch (err.code) {
        case ErrorCode.NoResponders:
          console.log("no one is listening to 'hello.world'");
          break;
        case ErrorCode.Timeout:
          console.log("someone is listening but didn't respond");
          break;
        default:
          console.error('Error Publishing to stream:', err.message);
      }
    } finally {
      // nc.close();
    }
  }


  async consumeMessages() {

    const stream = 'my_stream';
    const subj = `my_stream.*`;
    const subjA = 'my_stream.a';
    const consumerName = 'my_consumer';
    const batchSize = 10; // Number of messages to process in each batch
    let receivedMessages = 0;

    try {

      const consumer = await this.js.consumers.get(stream, consumerName);
      // let messages = await consumer.fetch({ max_messages: 4, expires: 2000 }); //it delays longer and only returns at once
      const messages = await consumer.consume();

      console.log(`batch completed: ${messages.getPending()} msgs pending`);
      console.log(`batch completed: ${messages.getReceived()} msgs receiving`);
      console.log(`batch completed: ${messages.getProcessed()} msgs processed`);

      for await (const message of messages) {
        const payload = JSON.parse(message.data);
        console.log('Received message:', payload);
        // console.log('Message info:', message.info);
        // message.ack();

        // Check if the batch size is reached or if there are no more messages
        receivedMessages++;
        if (receivedMessages % batchSize === 0 || receivedMessages === messages.getReceived()) {
          console.log(`Batch completed: ${receivedMessages} messages processed`);
        }
      }

    } catch (error) {
      console.error('Error consuming messages:', error.message);
    } finally {
      // Close connection if necessary
    }
  }


  async disconnect() {
    if (this.nc) {
      await this.nc.close();

      console.log("Disconnected from NATS server");
    }
  }


  /*------------------------------
  CLIENT SIDE OF NAT
  -------------------------------*/


  async publishMessageToSubject() {
    // Publishes the specified data to the specified subject.
    try {

      const stream = 'my_stream';
      const subj = `my_stream.*`;
      const subjA = 'my_stream.a';
      const payload = JSON.stringify({ data: 'greetings devs 2' });

      const response = this.nc.publish(subj, payload);
      console.log('Published message:', response);

    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }




}

module.exports = NATStreamManager



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



const streamManager = new NATStreamManager(natsconfig.serverURL)

streamManager.connect().then(async (nc) => {
  // streamManager.createAddStream();
  // streamManager.updateStreams();
  // streamManager.deleteStream()
  streamManager.readdAllStreams()
  // streamManager.addConsumerToStream();
  // streamManager.updateConsumer();
  // streamManager.deleteConsumer()
  streamManager.readAllConsumerOfStream();
  // streamManager.readAllOrderedConsumerofStreams()
  // streamManager.consumerInfo()

  // ----CLient Side
  // streamManager.publishMessageToStream();
  streamManager.consumeMessages();

})