const { MongoClient, ObjectID } = require('mongodb');
const _ = require('lodash');
const fs = require('fs');
const elasticsearch = require('elasticsearch');
const Promise = require('bluebird');
const RECOMMENDED_COURSE_TMPL = _.template(fs.readFileSync(path.resolve(__dirname,'../tmpls/recommendedCourse.html')));

const client = new elasticsearch.Client({
  host: '192.124.120.175:9200'
});

(async function () {
  global.db1 = await MongoClient.connect('mongodb://lildev:lildev123@mongodb.dev-stack.f00dd7c8.svc.dockerapp.io:33258/lildev-db');
  console.log('db1 connected', db1.databaseName);
  getProminent();
})();

async function sendEmail(){
  const app = EmailService.app;
  const { email } = results;

  const recomResults = await client.search({
    index: 'coursesindexer',
    type: 'coursesindexer',
    q: `id:${}`,
    '_source': ['type', 'author', 'type', 'title', 'metaInformation.subject.name', 'thumbnail']
  }); 
  for(const id of userIds){
    const 
    
  }
  email.send({
    to: {
    name: `${userIds[id.firstName]}`,
    email: userIds[id.email] 
    },
    from: 'no-reply@learnindialearn.in',
    subject: 'My-lil recommendations',})
}

const results = {};
let userIds = [];
async function getProminent() {
  console.log('call fired');
  const collectionUser = db1.collection('user');
  userIds = await collectionUser.find({}, { _id: true, email: true, firstName: true }).toArray();
  console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> No of Users found :', userIds.length);
  const collectionAndroid = db1.collection('ANDROID');
  for (const id of userIds) {
    results[id._id] = await Promise.props({
      topic: collectionAndroid.aggregate(
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.topic', count: { $sum: 1 } } },
        {$sort: { count: -1 }},
      {$limit: 3}]).toArray(),
      courseType: collectionAndroid.aggregate(
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.courseType', count: { $sum: 1 } } },
        {$sort: { count: -1 }},
      {$limit: 3}]).toArray(),
      curator: collectionAndroid.aggregate(f
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.curator', count: { $sum: 1 } } },
        {$sort: { count: -1 }},
      {$limit: 3}]).toArray()
    });
  }
  // console.log('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Results for each user:', results);
  const bulk = collectionUser.initializeOrderedBulkOp();
  // const condition;
  for (const id of userIds) {
    const obj = { 'dynamicRecommendations': results[id._id] };
    bulk.find({ _id: ObjectID(id._id) }).upsert().update({ $set: obj });
  }
  const toStore = await bulk.execute();
  console.log('////////////////////////////////////  Stored in db ', toStore.isOk());
  //use email function here
}

