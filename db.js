const MongoClient = require('mongodb').MongoClient;
const uri = process.env.DB || 'mongodb://localhost:27017/coinosis';

const Client = new MongoClient(uri , {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

let db, client;
const connect = async () => {
  client = await Client.connect();
  db = client.db();
}

const getCollections = () => {
  return {
    users: db.collection('users'),
    events: db.collection('events'),
  }
}

const disconnect = () => {
  client.close();
}

module.exports = { connect, getCollections, disconnect };
