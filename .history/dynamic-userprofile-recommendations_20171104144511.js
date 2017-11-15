const { MongoClient, ObjectID } = require('mongodb');
const _ = require('lodash');
const fs = require('fs');
const path = require('path');
const nodemailer = require('nodemailer');
const elasticsearch = require('elasticsearch');
const Promise = require('bluebird');
const RECOMMENDED_COURSE_TMPL = _.template(fs.readFileSync(path.resolve(__dirname, './recommendedCourse.html')));
const mailData = [];
let index = 0;
const client = new elasticsearch.Client({
  host: '192.124.120.175:9200'
});
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: 'gallifreyanjohn@gmail.com',
    pass: 'X72ONvML07wF'
  }
});

(async function () {
  global.db1 = await MongoClient.connect('mongodb\://lildev:lildev123@mongodb.dev-stack.f00dd7c8.svc.dockerapp.io:33258/lildev-db');
  console.log('db1 connected', db1.databaseName);
  getProminent();
})();

async function createEmailData() {
  const toQuery = [];
  for (const id of Object.keys(results)) {
    const tempTopic = [], tempCurator = [], tempType = [];
    for (let i = 0; i < 3; i++) {
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
                topic_id: tempTopic
              }
            },
            {
              terms: {
                author_id: tempCurator
              }
            },
            {
              terms: {
                course_type: tempType
              }
            }
          ]
        }
      },
      _source: ['thumbnail.secure_url', 'title', 'course_type', 'description','id']
    }
    // console.log('body??????????????????',body);
    toQuery.push(client.search({
      index: 'coursesindexer',
      type: 'coursesindexer',
      body: body
    }));

  }
  const fetchedCourses = await Promise.all(toQuery);
  console.log('<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< fetched courses data: ', fetchedCourses.length);
  
  for (const id in Object.keys(results)) {
    const user = results[Object.keys(results)[id]];
    user.recommendCourses = [];
    const { hits } = fetchedCourses[id].hits;
    for (const source of hits) {
      user.recommendCourses.push(source._source);
    }
    const emailData = { name: user.firstName };
    for (const course in user.recommendCourses) {
      emailData[`title${course}`] = user.recommendCourses[course].title;
      emailData[`course_type${course}`] = user.recommendCourses[course].course_type;
      emailData[`description${course}`] = user.recommendCourses[course].description;
      emailData[`thumbnail${course}`] = user.recommendCourses[course].thumbnail.secure_url;
      emailData[`courseUrl${course}`] = `https://learnindialearn.com/course/${user.recommendCourses[course].course_type}/${user.recommendCourses[course].id}/}`;
      emailData.fbLogo= "dfdf";
      emailData.linkedInLogo= "sdfgfdg";
      emailData.twitterLogo= "dfghd";
      emailData.privacyUrl= "dfgdgfhcvb";
      emailData.dbId = "sdfsv";
      emailData.projectId="xfbcb";

    }
    // console.log('emailData........................', emailData);
    const mailOptions = {
      from: 'gallifreyanjohn@gmail.com',
      to: 'shashwatjoshi@live.com',
      subject: 'My-lil recommendations',
      html: RECOMMENDED_COURSE_TMPL(emailData)
    };
    mailData.push(mailOptions);
    // transporter.sendMail(mailOptions);



    // email.send({
    //   to: `'${user.email}'`,
    //   html: RECOMMENDED_COURSE_TMPL(emailData),
    //   from: 'no-reply@learnindialearn.in',
    //   subject: 'My-lil recommendations',
    // });
  }
  if (mailData.length) {
    sendEmail(mailData[index]);
  }
}

function sendEmail(data) {
  console.log('i am called..................', index);
  transporter.sendMail(data, function (error, info) {
    if (error) {
      console.log(error);
    } else {
      console.log('Email sent: ' + info.response);
      index += 1;
      if (index !== mailData.length) {
        sendEmail(mailData[index],index);
      } else console.log('done.......................');
    }
  });
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
        { $sort: { count: -1 } },
        { $limit: 3 }]).toArray(),
      courseType: collectionAndroid.aggregate(
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.courseType', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 3 }]).toArray(),
      curator: collectionAndroid.aggregate(
        [{ $match: { userId: `${id._id}` } },
        { $unwind: "$browsed" },
        { $group: { _id: '$browsed.curator', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 3 }]).toArray()
    });
  }
  // console.log(' Results for each user:', results);
  // const bulk = collectionUser.initializeOrderedBulkOp();

  for (const id of userIds) {
    if (!results[id._id].topic.length) {
      delete results[id._id];
    } else {
      const obj = { 'dynamicRecommendations': results[id._id] };
      results[id._id].firstName = id.firstName;
      results[id._id].email = id.email;
    }
    // bulk.find({ _id: ObjectID(id._id) }).upsert().update({ $set: obj });
  }
  // const toStore = await bulk.execute();
  // console.log('////////////////////////////////////  Stored in db ', results);
  //use email function here
  createEmailData();
}

