"use strict";

const Mongo = require("mongodb");
var Grid = require("gridfs-stream");
const mime = require("mime-types");
const uuidv4 = require("uuid/v4");
const fs = require("fs");
const isStream = require("is-stream");
const { MoleculerError, ServiceSchemaError } = require("moleculer").Errors;

const MongoClient = Mongo.MongoClient;

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

      this.bucketFS = new Mongo.GridFSBucket(this.db, {
        bucketName: this.bucketName,
      });

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
    try {
      let files = await this.bucketFS.find(filters).toArray();
      return files;
    } catch (error) {
      return error;
    }
  }

  findOne(query) {
    // To be implemented
    return;
  }

  findById(fd) {
    let stream = this.bucketFS.openDownloadStreamByName(fd);
    return new Promise((resolve, reject) => {
      stream
        .on("error", function (error) {
          reject(error);
        })
        .on("finish", function () {
          resolve(stream);
        });
    });
  }

  async count(filters = {}) {
    // To be implemented
    return;
  }

  async save(entity, meta) {
    return new Promise((resolve, reject) => {
      if (!isStream(entity)) reject("Entity is not a stream");

      const filename = meta.id || meta.filename || uuidv4();
      const contentType = meta.contentType || mime.lookup(filename);

      if (meta?.$multipart) {
        delete meta.$multipart;
      }

      let stream = this.bucketFS.openUploadStream(meta.filename, {
        metadata: meta,
        contentType: contentType,
      });

      entity
        .pipe(stream)
        .on("error", function (error) {
          reject(error);
        })
        .on("finish", function (response) {
          resolve(response);
        });
    });
  }

  async updateById(entity, meta) {
    return await this.save(entity, meta);
  }

  removeMany(query) {
    // To Be Implemented.
  }

  removeById(_id) {
    return this.bucketFS.delete(_id);
  }

  clear() {
    // To be implemented
    return;
  }
}

module.exports = FSAdapter;
