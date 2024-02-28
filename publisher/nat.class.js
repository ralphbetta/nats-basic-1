const { connect, JSONCodec, StringCodec, Empty, ErrorCode } = require('nats');

class AppNATService {

  constructor(serverURL, clusterID, clientID) {
    this.serverURL = serverURL;
    this.clusterID = clusterID;
    this.clientID = clientID;
    this.nc = null;
    this.js = null
    this.subscription = null;
    this.sc = StringCodec();
    this.jc = JSONCodec();
  }

  async connect() {
    // console.log(this.serverURL, this.clusterID, this.clientID);
    this.nc = await connect({
      servers: this.serverURL,
      reconnect: true,
      waitOnFirstConnect: true,
      maxReconnectAttempts: -1,
      reconnectTimeWait: 1000,
      clusterID: this.clusterID,
      clientID: this.clientID,
      user: "jenny", //optional
      pass: "867-5309", //optional
    });

    this.js = this.nc.jetstream();
    this.subscription = this.nc.subscribe("crud");

    console.log(`Connected to ${this.serverURL}`);


    return this.nc;

  }

  async disconnect() {
    if (this.nc) {
      await this.nc.close();
      console.log("Disconnected from NATS server");
    }
  }

  async externalDisconnect(nc) {
    const done = nc.closed();
    await nc.close();
    await done;
    console.log("NATS connection closed successfully");
  }

  async handleMessage(msg) {
    const action = msg.subject.split(".")[1];
    const data = this.sc.decode(msg.data);

    switch (action) {
      case "create":
        console.log("Create:", data);
        break;
      case "read":
        console.log("Read:", data);
        break;
      case "update":
        console.log("Update:", data);
        break;
      case "delete":
        console.log("Delete:", data);
        break;
      default:
        console.log("Unknown action:", action);
        break;
    }
  }

  async publishMessage(subject, data) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    try {

      // const publicationAcknowlegment = await this.js.publish(subject, this.jc.encode(data));  //requires response
       this.nc.publish(subject, this.jc.encode(data));

      console.log("this is passed", publicationAcknowlegment);

    } catch (error) {
      console.log("Error Publishing", error.message);
    }
  }

  async publishMessageWithHeader(action, data) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    const h = headers();
    h.append("id", "123456");
    h.append("unix_time", Date.now().toString());
    this.nc.publish(action, this.jc.encode(data), { headers: h });
  }

  async requestAction(channel) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    try {
      const requestInstance = await this.nc.request(channel, this.sc.encode("Dummy message"), { timeout: 1000 });

      console.log(`got response: ${this.sc.decode(requestInstance.data)}`);
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
  }

  async consume(channel, callback) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }

    this.subscription = this.nc.subscribe(channel);
    console.log(this.subscription)

    for await (const msg of this.subscription) {
      const decodedMsg = this.jc.decode(msg.data);
      console.log(decodedMsg)
    }
    // Return the subscription object
    // return subscription;
  }


  async replyRequest(channel) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    const sub = this.nc.subscribe(channel, {
      callback: (err, msg) => {
        if (err) {
          console.log("subscription error", err.message);
          return;
        }
        const paylaod = this.sc.decode(msg.data);
        const streamedChannel = msg.channel;
        msg.respond(`Seen`);

      },
    });

  }


}


module.exports = AppNATService
