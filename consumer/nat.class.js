const { connect, JSONCodec, StringCodec, headers } = require('nats');

class AppNATService {

  constructor(serverURL, clusterID, clientID) {
    this.serverURL = serverURL;
    this.clusterID = clusterID;
    this.clientID = clientID;
    this.nc = null;
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
      clientID: this.clientID
    });

    console.log(`Connected to ${this.serverURL}`);
    this.subscription = this.nc.subscribe("crud");

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
        // Perform create operation
        break;
      case "read":
        console.log("Read:", data);
        // Perform read operation
        break;
      case "update":
        console.log("Update:", data);
        // Perform update operation
        break;
      case "delete":
        console.log("Delete:", data);
        // Perform delete operation
        break;
      default:
        console.log("Unknown action:", action);
        break;
    }
  }

  async publishMessage(action, data) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    this.nc.publish(action, this.jc.encode(data));
  }

  async publishMessageWithHeader(action, data) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    
    const h = headers();
    h.append("id", "123456");
    h.append("unix_time", Date.now().toString());
    this.nc.publish(action, this.jc.encode(data), {headers: h });
  }

  async consume(channel, callback) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }
    
    this.subscription = this.nc.subscribe(channel);
    const subject = this.subscription.getSubject();

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
