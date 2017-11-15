const { MongoClient, ObjectID } = require('mongodb');
const elasticsearch = require('elasticsearch');
const Promise = require('bluebird');
const client = new elasticsearch.Client({
  host: '192.124.120.175:9200'
});

(async function () {
  global.db1 = await MongoClient.connect('mongodb://lildev:lildev123@mongodb.dev-stack.f00dd7c8.svc.dockerapp.io:33258/lildev-db');
  console.log('db1 connected', db1.databaseName);  
  getProminent();
})();

const results = {};
async function getProminent() {
  console.log('call fired');  
  const collectionUser = db1.collection('user');  
  const userIds = await collectionUser.find({},{_id: true}).toArray();
  console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> No of Users found :',userIds.length);
  const collectionAndroid = db1.collection('ANDROID');
  for (const id of userIds) {
    results[id._id] = await Promise.props({
      topic: collectionAndroid.aggregate(
        { $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.topic', count: { $sum: 1 } } }
      ).sort({ count: -1 }).limit(3).toArray(),
      courseType : collectionAndroid.aggregate(
        { $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.courseType', count: { $sum: 1 } } }
      ).sort({ count: -1 }).limit(3).toArray(),
      curator : collectionAndroid.aggregate(
        { $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.curator', count: { $sum: 1 } } }
      ).sort({ count: -1 }).limit(3).toArray(),
    });
  }
  console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Results for each user:',results);
  const bulk = collectionUser.initializeOrderedBulkOp();
  // const condition;
  for (const id of userIds){
    bulk.find({_id: `${id._id}`}.upsert().update({'dynamicRecommendations':results[id._id]})); 
 }
  const toStore = await bulk.execute();
  console.log('////////////////////////////////////',toStore);
  
}