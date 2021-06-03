"use strict";

const Mongo = require("mongodb");
const ObjectId = require("mongodb").ObjectId;
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
      return await this.bucketFS.find(filters).toArray();
    } catch (error) {
      return error;
    }
  }

  findOne(query) {
    // To be implemented
    return;
  }

  findById(fd) {
    return this.bucketFS.openDownloadStreamByName(fd);
  }

  async count(filters = {}) {
    // To be implemented
    return;
  }

  async save(entity, meta) {
    if (!isStream(entity)) reject("Entity is not a stream");

    const filename = meta.id || meta.filename || uuidv4();
    const contentType = meta.contentType || mime.lookup(filename);
    meta.version = "1"

    if (meta?.$multipart) {
      delete meta.$multipart;
    }

    // If filename exists - version it
    try {
      let file = await this.bucketFS.find({id: filename}).sort( { "metadata.version": -1 } ).toArray();
      // Get file latest version and increment to new file
      if( file.length > 0 && file[0].metadata ){
        if( file[0].metadata.version )
          meta.version = String( (parseInt(file[0].metadata.version) || 0) + 1 )
      }
    } catch (error) {}

    let stream = this.bucketFS.openUploadStream(meta.filename, {
      metadata: meta,
      contentType: contentType,
    });

    return await entity
      .pipe(stream)
      .on("error", function (error) {
        return error;
      })
      .on("finish", function (response) {
        return response;
      });
  }

  async updateById(entity, meta) {
    return await this.save(entity, meta);
  }

  removeMany(query) {
    // To Be Implemented.
  }

  async removeById(_id) {
    _id = new ObjectId(_id);
    this.bucketFS.delete(_id);
    return { id: _id };
  }

  clear() {
    // To be implemented
    return;
  }
}

module.exports = FSAdapter;
