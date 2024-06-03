const functions = require('@google-cloud/functions-framework');
const express = require('express');
const bodyParser = require('body-parser');
const { PubSub } = require('@google-cloud/pubsub');
const { MongoClient } = require('mongodb');

// MongoDB setup
const uri = "mongodb+srv://alejandramaleman:pdQmclYz20NJmjuG@cluster0.4y7an5n.mongodb.net/BDTransactions?retryWrites=true&w=majority&appName=Cluster0";
const client = new MongoClient(uri);
const database = client.db("BDTransactions"); 
const collection1 = database.collection("last_transx"); 
const collection2 = database.collection("transxes2"); 

// Create an Express application
const app = express();

// Middleware to parse JSON bodies
app.use(bodyParser.json());

// Initialize the PubSub client
const pubsub = new PubSub();
const topicName = 'tranx-data';

// Helper function to determine transaction session
function getTransactionSession(dateTime) {
    const hours = new Date(dateTime).getHours();
    if (hours >= 0 && hours < 6) {
        return 'late_night';
    } else if (hours >= 6 && hours < 12) {
        return 'morning';
    } else if (hours >= 12 && hours < 18) {
        return 'afternoon';
    } else {
        return 'evening';
    }
}

// Define the endpoint
app.post('/processJson', async (req, res) => {
  if (req.body && req.body.trans_date_trans_time) {
    const messageBuffer = Buffer.from(JSON.stringify(req.body));

    // Assign transaction session based on date_time
    data_trans = req.body
    data_trans.trans_session = getTransactionSession(req.body.trans_date_trans_time);
    data_trans.is_fraud = false;

    try {
      // Reference to the Pub/Sub topic
      const topic = pubsub.topic(topicName);

      // Publish the message to Pub/Sub
      await topic.publish(messageBuffer);

      // Delete all existing documents in last_transx collection
      await collection1.deleteMany({});

      // Insert the new data into last_transx collection
      await collection1.insertOne(data_trans);

      // Optionally, save to another collection if needed
      await collection2.insertOne(data_trans);

      res.send(`Data: ${JSON.stringify(req.body)}! Message sent to PubSub and data replaced in MongoDB.`);
    } catch (error) {
      console.error('Error processing the request', error);
      res.status(500).send('Error while publishing message to Pub/Sub or replacing data in MongoDB.');
    }
  } else {
    res.status(400).send('No date_time in the body');
  }
});

// Register the Express application
functions.http('helloHttp', app);

// Close the MongoDB client connection when the application is terminated
process.on('SIGINT', async () => {
  await client.close();
  process.exit();
});
