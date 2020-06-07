const mongodb = require("mongodb");
const async = require("async");
const customer = require("./data/m3-customer-data.json");
const customerAddress = require("./data/m3-customer-address-data.json");

//Combine customer and address
const customerCombined = customer.map((item, index) => ({
  ...item,
  ...customerAddress[index],
}));

const mongoClient = mongodb.MongoClient;
const url = "mongodb://localhost:27017";
const collectionName = "customers";
const limit = +process.argv[2];
let tasks = [];

mongoClient.connect(url, { useUnifiedTopology: true }, (error, client) => {
  if (error) return process.exit(1);

  const db = client.db("edx-course-db");
  db.dropCollection(collectionName).catch((e) => console.log("No Collection"));

  let i = 0;
  while (i < customerCombined.length) {
    const payload = customerCombined.slice(i, i + limit);
    tasks.push((callback) => {
      db.collection(collectionName).insertMany(payload, (error, result) => {
        callback(error, result);
      });
    });

    i += limit;
  }

  async.parallel(tasks, (error, result) => {
    if (error) console.error("Error: ", error);
    //else console.log(result.length);

    console.log("Migration completed");
    client.close();
  });
});
