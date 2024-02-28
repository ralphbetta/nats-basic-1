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
    this.streams = ["streama", "streamb", "streamc", "streamd"];
    this.subjects =  ['subject.a', 'subject.b', 'subject.c', 'subject.d'];
    this.consumers = ["consumera", "consumerb", "consumerc", "consumerd"]; //same as durable_name
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
      console.log(`Error connecting to ${this.serverURL}: ${err.message}`);
      throw err;
    }
  }



  /*------------------------------
  MANAGER SECTIONS
  -------------------------------*/


  async createAddStream(index) {
    try {
      const maxMessages = 1000;
      await this.jsm.streams.add({ name: this.streams[index], subjects: this.subjects });
      console.log('Stream added:', this.streams[index]);
    } catch (error) {
      console.error('Error creating streams:', error.message);
    } finally {
      // nc.close();
    }
  }

  async readdAllStreams() {
    try {
      const streamIterator = this.jsm.streams.list();

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
      console.error('Error Reading streams:', error.message);
    } finally {
      // nc.close();
    }
  }


  async updateStreams(index) {

    try {
      const streamInfo = await this.jsm.streams.info(this.streams[index]);
      // streamInfo.config.subjects.push(this.streams[index+1]);
      // await this.jsm.streams.update(streamInfo.config);
      await this.jsm.streams.update(this.streams[index], { max_msgs: 100 });
      console.log('Stream updated:', this.streams[index]);

    } catch (error) {
      console.error('Error managing streams:', error);
    } finally {
      // nc.close();
    }
  }

  async deleteStream(index) {
    try {
      await this.jsm.streams.delete(this.streams[index]);
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
      await this.jsm.streams.deleteMessage(this.streams[index], seq, true);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }


  async addConsumerToStream(index) {

    //consumer subscribes to a stream so it can read messages published to the corresponding subject
    //subjects like ['subject.a', 'subject.b', 'subject.c', 'subject.d'] that where used during  stream creation

    try {
      const completedConsumerInfo = await this.jsm.consumers.add(this.streams[index], {
        durable_name: this.consumers[index], //same as consumer name
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

  async updateConsumer(index) {
    const newConfig = {
      max_deliver: 100,
      ack_wait: 30,
      start_time: new Date().toISOString(),
    };

    try {
      const completedConsumerInfo = await this.jsm.consumers.update(this.streams[index], this.consumers[index], newConfig);
    } catch (error) {
      console.error('Error managing streams:', error.message);
    } finally {
      // nc.close();
    }
  }

  async consumerInfo(index) {
    try {
      const ci = await this.jsm.consumers.info(this.streams[index], this.consumers[index]);
      console.log("consumer info:", ci);
    } catch (error) {
      console.error('Error fetching stream consumer: info', error.message);
    } finally {
      // nc.close();
    }
  }

  async deleteConsumer(index) {
    try {
      
      if(!index){
        const completed = await this.jsm.consumers.delete(this.streams[0], undefined, undefined);
      }
      const completed = await this.jsm.consumers.delete(this.streams[index], this.consumers[index]);

      console.log(completed);
    } catch (error) {
      console.error('Error deleting stream consumer:', error.message);
    } finally {
      // nc.close();
    }
  }

  async readAllConsumerOfStream(index) {
    try {


      const consumers = await this.jsm.consumers.list(this.streams[index]).next();

      console.log("==========",this.streams[index], "has", consumers.length, "consumers ==================");

      consumers.forEach((consumer, index) => {
        // console.log(index, ci);
        console.log('Consumer:', consumer.config.durable_name);
        console.log('Stream:', consumer.stream_name);
        console.log('Durable name:', consumer.config.durable_name);
        console.log('ack_wait', consumer.config.ack_wait);
        console.log('Ack policy:', consumer.config.ack_policy);
        console.log(`-----------------------------------------------------`);
      });

    } catch (error) {
      console.error('Error reading consumers:', error.message);
    } finally {
      // Close connection
      // nc.close();
    }
  }


  async readAllOrderedConsumerofStreams(index) {
    try {
      /*
      Returns the Consumer configured for the specified stream having the specified name. 
      Consumers are typically created with JetStreamManager.
      If no name is specified, the Consumers API will return an ordered consumer.
      An ordered consumer expects messages to be delivered in order.
      If there's any inconsistency, the ordered consumer will recreate the underlying consumer at the correct sequence.
      Note that ordered consumers don't yield messages that can be acked because the client can simply recreate the consumer.
      */
   
      const pulledConsumer = await this.js.consumers.get(this.streams[index], this.consumers[index]);
      const orderedConsumer = await this.js.consumers.get(this.streams[index]); //message can not be acknowleged

      const pulledConsumerInfo = await pulledConsumer.info();
      const orderedConsumerInfo = await orderedConsumer.info()

      console.log(pulledConsumerInfo.config, orderedConsumerInfo.config)


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


  async publishMessageToStream(index) {
    try {

      const payload = JSON.stringify({ data: `payload sent to subject: ${this.subjects[index]}` });
      const publishAcknowlegment = await this.js.publish(this.subjects[index], payload);
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


  async consumeMessages(index) {
    
    const batchSize = 10; // Number of messages to process in each batch
    let receivedMessages = 0;

    try {

      const consumer = await this.js.consumers.get(this.streams[index], this.consumers[index]);
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


  async publishMessageToSubject(index) {
    // Publishes the specified data to the specified subject.
    try {

      const payload = JSON.stringify({ data: 'greetings devs 2' });
      const response = this.nc.publish(this.subjects[index], payload);

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
  // streamManager.createAddStream(0); //----step 1 create stream with array of subjects
  // streamManager.updateStreams(); //-- update max message from default -1 (unlimited) if you want to limit 
  // streamManager.deleteStream(1); // ---deleting unwanted stream
  streamManager.readdAllStreams(); //----- step 2 view all streams
  // streamManager.addConsumerToStream(0);
  // streamManager.updateConsumer();
  // streamManager.deleteConsumer()
  streamManager.readAllConsumerOfStream(0);
  // streamManager.readAllOrderedConsumerofStreams(0)
  // streamManager.consumerInfo(0)

  // ----CLient Side
  // streamManager.publishMessageToStream(0);
  streamManager.consumeMessages(0);

})