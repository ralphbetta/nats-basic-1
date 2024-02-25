const { connect, JSONCodec, StringCodec } = require('nats');

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

    console.log(this.serverURL, this.clusterID, this.clientID);

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
    // this.subscription.on("message", this.handleMessage.bind(this));
    return this.nc;
  }

  async disconnect() {
    if (this.nc) {
      await this.nc.close();
      console.log("Disconnected from NATS server");
    }
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

  async consume(channel, callback) {
    if (!this.nc) {
      console.error("Not connected to NATS server");
      return;
    }

    this.subscription = this.nc.subscribe(channel);

    console.log(this.subscription)
    
    for await (const msg of this.subscription){
      const decodedMsg = this.jc.decode(msg.data);
      console.log(decodedMsg)
      
    }
  

    // Return the subscription object
    // return subscription;
  }
  
}


module.exports = AppNATService

// // Example usage:
// (async () => {
//   const serverURL = "nats://localhost:4222";
//   const clusterID = "test-cluster";
//   const clientID = "crud-service";

//   const crudService = new CRUDService(serverURL, clusterID, clientID);
//   await crudService.connect();

//   // Example: Publish a create action
//   await crudService.publishMessage("create", { id: 1, name: "Example" });

//   // Example: Consume messages
//   crudService.consume((action, data) => {
//     console.log(`Consumed message - Action: ${action}, Data:`, data);
//   });

//   // Wait for a while to simulate some activity
//   await new Promise(resolve => setTimeout(resolve, 5000));

//   await crudService.disconnect();
// })();



// RabbitMQ.connect(AppService.NOTIFICATION).then((channel) => {

//   console.log('monitoring for', AppService.NOTIFICATION);

//   RabbitMQ.monitorQueues(channel);
// });