/// <reference types="node" />
import { Db, Filter, GridFSBucket, Document, GridFSFile, MongoClient, MongoClientOptions, ObjectId } from "mongodb";
import type { Service, ServiceBroker } from "moleculer";
import type { AdapterSchema } from "moleculer-files";
declare class FSAdapter implements AdapterSchema {
    broker: ServiceBroker;
    bucketFS: GridFSBucket;
    bucketName: string;
    client: MongoClient;
    db: Db;
    dbName: string;
    service: Service;
    uri: string;
    opts: MongoClientOptions;
    constructor(uri: string, opts: MongoClientOptions);
    init(broker: ServiceBroker, service: Service): void;
    connect(): Promise<void>;
    disconnect(): Promise<void>;
    find(filters: Filter<GridFSFile>): Promise<any>;
    findOne(query: Filter<GridFSFile>): Promise<unknown>;
    findById(fd: string): Promise<unknown>;
    count(_filter?: Filter<GridFSFile>): Promise<void>;
    save(entity: NodeJS.ReadableStream, meta: Document): Promise<void>;
    updateById(entity: NodeJS.ReadableStream, meta: Document): Promise<void>;
    removeMany(_filter?: Filter<GridFSFile>): void;
    removeById(_id: string): Promise<{
        id: ObjectId;
    }>;
    clear(): void;
}
export default FSAdapter;
//# sourceMappingURL=index.d.ts.map