const { MongoClient, ObjectID } = require('mongodb');
const _ = require('lodash');
const fs = require('fs');
const elasticsearch = require('elasticsearch');
const Promise = require('bluebird');
// const RECOMMENDED_COURSE_TMPL = _.template(fs.readFileSync(path.resolve(__dirname,'../tmpls/recommendedCourse.html')));

const client = new elasticsearch.Client({
  host: '192.124.120.175:9200'
});

(async function () {
  global.db1 = await MongoClient.connect('mongodb://lildev:lildev123@mongodb.dev-stack.f00dd7c8.svc.dockerapp.io:33258/lildev-db');
  console.log('db1 connected', db1.databaseName);
  getProminent();
})();

async function sendEmail(){
  const toQuery = [];
  for(const id of Object.keys(results)){
    const tempTopic = [], tempCurator = [], tempType = [];
    for(let i= 0;i<3;i++){
      // console.log('results[id].topic',results[id].topic[i]);
      tempTopic.push(results[id].topic[i]._id);
      tempCurator.push(results[id].curator[i]._id);
      tempType.push(results[id].courseType[i]._id);
    }
      const body = {
        query: {
          dis_max: {
            queries: [
              {
                terms: {
                  topic_id: [
                    "593e6736dfe1391d00a267bf"
                  ]
                }
              },
              {
                terms: {
                  sub_id: ["58c92788bddb93e4048f61e7"]
                }
              },
              {
                terms: {
                  author_id:  [
                    "58d3e72c9f69051a00746f9c"
                  ]
                }
              }
            ]
          }
        },
        _source: ['thumbnail','title','course_type','description']
      }
      // tempobj.push(body);
      // console.log('body??????????????????',body);
      toQuery.push(client.search({
        index: 'coursesindexer',
        type: 'coursesindexer',
        body: body}));
  
    if (id === '59e462c64a42a91ef5f4d8d0') console.log('printing array toQuery.',body);
    
  }
  const fetchedCourses = await Promise.all(toQuery);
  

  console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<',JSON.stringify(fetchedCourses,null,2));
  // email.send({
  //   to: {
  //   name: `${userIds[id.firstName]}`,
  //   email: userIds[id.email] 
  //   },
  //   from: 'no-reply@learnindialearn.in',
  //   subject: 'My-lil recommendations',})
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
      curator: collectionAndroid.aggregate(
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.curator', count: { $sum: 1 } } },
        {$sort: { count: -1 }},
      {$limit: 3}]).toArray()
    });
  }
  // console.log(' Results for each user:', results);
  // const bulk = collectionUser.initializeOrderedBulkOp();

  for (const id of userIds) {
    if (!results[id._id].topic.length) {
      delete results[id._id];
    } else {
      const obj = { 'dynamicRecommendations': results[id._id] };
    }
    // bulk.find({ _id: ObjectID(id._id) }).upsert().update({ $set: obj });
  }
  // const toStore = await bulk.execute();
  console.log('////////////////////////////////////  Stored in db ', results);
  //use email function here
  sendEmail();
}

