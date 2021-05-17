"use strict";

const Mongo = require("mongodb");
var Grid = require("gridfs-stream");
const mime = require("mime-types");
const uuidv4 = require("uuid/v4");
const fs = require("fs");
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
    const gfs = this.grid;
    if (!filters?.root) {
      filters.root = this.bucketName;
    }
    return new Promise((resolve, reject) => {
      gfs.files.find().toArray(function (err, files) {
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

  save(entity, meta) {
    console.log("entity", entity);

    const filename = meta.id || uuidv4();
    const contentType = meta.contentType || mime.lookup(meta.id);

    return new Promise(async (resolve, reject) => {
      const stream = this.grid.createWriteStream({
        filename,
        content_type: contentType,
        root: this.bucketName,
        metadata: meta,
      });
      entity.pipe(stream);
      stream.on("finish", () => resolve(stream));
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
