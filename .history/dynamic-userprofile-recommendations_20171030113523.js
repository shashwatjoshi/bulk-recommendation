const { MongoClient, ObjectID } = require('mongodb');
const elasticsearch = require('elasticsearch');
const Promise = require('bluebird');
const client = new elasticsearch.Client({
  host: '192.124.120.175:9200'
});

(async function () {
  // global.db = await MongoClient.connect('mongodb://sj:1@localhost:27017/test');
  // console.log('db connected', db.databaseName);
  
  global.db1 = await MongoClient.connect('mongodb://lildev:lildev123@mongodb.dev-stack.f00dd7c8.svc.dockerapp.io:33258/lildev-db');
  console.log('db1 connected', db1.databaseName);  
  getProminent();
})();

// global.db = await MongoClient.connect(mongodbURL);
//     global.collection = db.collection('DigitalBook');
//     const dbs = await collection.find().toArray();

const results = {};
async function getProminent() {
  console.log('call fired');  
  const collectionUser = db1.collection('user');  
  const userIds = await collectionUser.find({}).toArray();
  // console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>',userIds.length);
  const id = userIds[0];
  // console.log('id.........', id);
  const collectionAndroid = db1.collection('ANDROID');  
  results[id._id] = await Promise.props({
    topic: collectionAndroid.aggregate(
      { $match: { userId: `${id._id}` } },
      { $unwind: "$browsed" },
      { $group: { _id: '$browsed.topic', count: { $sum: 1 } } }
    ).sort({ count: -1 }).limit(3),
    courseType : collectionAndroid.aggregate(
      { $match: { userId: `${id._id}` } },
      { $unwind: "$browsed" },
      { $group: { _id: '$browsed.courseType', count: { $sum: 1 } } }
    ).sort({ count: -1 }).limit(3),
    curator : collectionAndroid.aggregate(
      { $match: { userId: `${id._id}` } },
      { $unwind: "$browsed" },
      { $group: { _id: '$browsed.curator', count: { $sum: 1 } } }
    ).sort({ count: -1 }).limit(3),
  });
  // const { _id } = userIds;
  // const required = {};
  // for (const id in userIds) {
  //   const collectionAndroid = db1.collection('ANDROID');
  //   console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>', collectionAndroid.getName());
  //   results[id._id] = await Promise.props({
  //     topic: collectionAndroid.aggregate(
  //       { $match: { userId: `${id._id}` } },
  //       { $unwind: "$browsed" },
  //       { $group: { _id: '$browsed.topic', count: { $sum: 1 } } }
  //     ).sort({ count: -1 }).limit(3).toArray(),
  //     courseType : collectionAndroid.aggregate(
  //       { $match: { userId: `${id._id}` } },
  //       { $unwind: "$browsed" },
  //       { $group: { _id: '$browsed.courseType', count: { $sum: 1 } } }
  //     ).sort({ count: -1 }).limit(3).toArray(),
  //     curator : collectionAndroid.aggregate(
  //       { $match: { userId: `${id._id}` } },
  //       { $unwind: "$browsed" },
  //       { $group: { _id: '$browsed.curator', count: { $sum: 1 } } }
  //     ).sort({ count: -1 }).limit(3).toArray(),
  //   });
  // }
console.log('........................................',results);
}