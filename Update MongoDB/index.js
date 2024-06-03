const { MongoClient } = require('mongodb');
const functions = require('@google-cloud/functions-framework');

// MongoDB setup with your provided credentials
const uri = "mongodb+srv://alejandramaleman:pdQmclYz20NJmjuG@cluster0.4y7an5n.mongodb.net/BDTransactions?retryWrites=true&w=majority&appName=Cluster0";
const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });

// Register a CloudEvent callback with the Functions Framework that will
// be executed when the Pub/Sub trigger topic receives a message.
functions.cloudEvent('helloPubSub', async cloudEvent => {
  try {
    // Connect to MongoDB
    await client.connect();
    const database = client.db("BDTransactions");
    const collection = database.collection("last_transx"); 
    
    // The Pub/Sub message is passed as the CloudEvent's data payload.
    const base64name = cloudEvent.data.message.data;
    const message = base64name
      ? JSON.parse(Buffer.from(base64name, 'base64').toString())
      : null;

    if (message) {
      // Assuming the document has a unique identifier in 'transactionId' field
      const filter = {};
      const updateDoc = {
        $set: { is_fraud: true}
      };

      const result = await collection.updateOne(filter, updateDoc);

      console.log(`Update result: ${result.modifiedCount} document(s) was/were updated.`);
    } else {
      console.log("No valid message received or missing transactionId/isFraud fields");
    }
  } catch (error) {
    console.error(`Failed to process the message: ${error}`);
  } finally {
    // Ensures that the client will close when you finish/error
    await client.close();
  }
});
