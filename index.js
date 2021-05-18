"use strict";

const Mongo = require("mongodb");
var Grid = require("gridfs-stream");
const mime = require("mime-types");
const uuidv4 = require("uuid/v4");
const fs = require("fs");
const isStream = require("is-stream");
const { Readable } = require("stream");
const { MoleculerError, ServiceSchemaError } = require("moleculer").Errors;

const MongoClient = Mongo.MongoClient;
const ObjectID = Mongo.ObjectID;
const Bucket = Mongo.GridFSBucket;

class FSAdapter {
  constructor(uri, opts, dbName) {
    this.uri = uri;
    this.opts = opts;
  }

  init(broker, service) {
    this.broker = broker;
    this.service = service;
    this.bucketName = this.service.schema.collection || "fs";

    if (!this.uri) {
      throw new ServiceSchemaError("Missing `uri` definition!");
    }
  }

  async connect() {
    this.client = new MongoClient(
      this.uri,
      Object.assign({ useUnifiedTopology: true }, this.opts)
    );
    return this.client.connect().then(() => {
      this.db = this.client.db(this.dbName);
      this.grid = new Grid(this.db, Mongo);

      let options = {};
      options.bucketName = this.bucketName;
      console.log('options bucketfs', options)

      this.bucketFS = new Mongo.GridFSBucket(this.db, options);
      console.log('bucketFS', this.bucketFS)

      this.service.logger.info("GridFS adapter has connected successfully.");

      /* istanbul ignore next */
      this.db.on("close", () =>
        this.service.logger.warn("MongoDB adapter has disconnected.")
      );
      this.db.on("error", (err) =>
        this.service.logger.error("MongoDB error.", err)
      );
      this.db.on("reconnect", () =>
        this.service.logger.info("MongoDB adapter has reconnected.")
      );
    });
  }

  disconnect() {
    return Promise.resolve();
  }

  async find(filters) {
    // { $regex: /m/i }
    console.log("findall with root");
    console.log("grid files", this.grid.files);
    if (!filters?.root) {
      filters.root = this.bucketName;
    }
    return new Promise((resolve, reject) => {
      this.grid.files
        .find({ root: this.bucketName })
        .toArray(function (err, files) {
          if (err || !files) {
            reject(err);
          } else {
            resolve(files);
          }
        });
    });
  }

  findOne(query) {
    // To be implemented
    return;
  }

  findById(fd) {
    const stream = this.grid.createReadStream({
      filename: fd,
      root: this.bucketName,
    });

    return new Promise((resolve, reject) => {
      stream.on("open", () => resolve(stream));
      stream.on("error", (err) => resolve(null));
    });
  }

  async count(filters = {}) {
    // To be implemented
    return;
  }

  async save(entity, meta) {
    console.log("save", entity);
    console.log("meta", meta);
    console.log("bucketName", this.bucketName);

    if (!isStream(entity)) return { error: "Entity is not a stream" };

    // let uploadStream = this.bucketFS.openUploadStream(trackName, {chunkSizeBytes:null, metadata:{speaker: "Bill Gates", duration:"1hr"}, contentType: null, aliases: null});

    let stream = this.bucketFS.openUploadStream(meta.filename)
    entity.pipe(stream)
      .on("error", function (error) {
        console.log('error openupload', error);
        return error
      })
      .on("finish", function () {
        console.log("done!");
        return true
      });
  }

  async updateById(entity, meta) {
    return await this.save(entity, meta);
  }

  removeMany(query) {
    // To Be Implemented.
  }

  removeById(_id) {
    return new Promise((resolve, reject) => {
      this.grid.remove({ filename }, (err, gs) => {
        if (err) return reject(err);
        return { id: _id };
      });
    });
  }

  clear() {
    // To be implemented
    return;
  }
}

module.exports = FSAdapter;
