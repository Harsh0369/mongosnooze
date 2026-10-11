import type { Collection, Db } from "mongodb";

export class MockCollection {
  public docs = new Map<string, any>();
  public indexes: any[] = [];
  public changeStreamListeners: ((change: any) => void)[] = [];
  public errorListeners: ((err: any) => void)[] = [];
  public shouldFailWithIndexConflict = false;
  public shouldFailWithReplicaSet = false;

  constructor(public readonly name: string) {}

  async createIndex(keys: any, options?: any) {
    if (this.shouldFailWithIndexConflict) {
      const err: any = new Error("Index already exists with different options");
      err.code = 85;
      err.codeName = "IndexOptionsConflict";
      throw err;
    }
    this.indexes.push({ keys, options });
    return "mock_index";
  }

  async insertOne(doc: any) {
    const id = String(doc._id);
    this.docs.set(id, JSON.parse(JSON.stringify(doc)));
    return { acknowledged: true, insertedId: doc._id };
  }

  async findOne(filter: any) {
    for (const doc of this.docs.values()) {
      if (this.matches(doc, filter)) {
        return JSON.parse(JSON.stringify(doc));
      }
    }
    return null;
  }

  async updateOne(filter: any, update: any, options?: { upsert?: boolean }) {
    for (const [id, doc] of this.docs.entries()) {
      if (this.matches(doc, filter)) {
        this.applyUpdate(doc, update);
        this.docs.set(id, doc);
        return { matchedCount: 1, modifiedCount: 1, upsertedCount: 0 };
      }
    }

    if (options?.upsert) {
      const newDoc: any = { _id: filter._id };
      this.applyUpdate(newDoc, update);
      this.docs.set(String(newDoc._id), newDoc);
      return { matchedCount: 0, modifiedCount: 0, upsertedCount: 1 };
    }

    return { matchedCount: 0, modifiedCount: 0, upsertedCount: 0 };
  }

  async updateMany(filter: any, update: any) {
    let modified = 0;
    for (const [id, doc] of this.docs.entries()) {
      if (this.matches(doc, filter)) {
        this.applyUpdate(doc, update);
        this.docs.set(id, doc);
        modified++;
      }
    }
    return { matchedCount: modified, modifiedCount: modified };
  }

  async deleteOne(filter: any) {
    for (const [id, doc] of this.docs.entries()) {
      if (this.matches(doc, filter)) {
        this.docs.delete(id);
        this.changeStreamListeners.forEach((listener) =>
          listener({
            operationType: "delete",
            documentKey: { _id: doc._id },
          })
        );
        return { deletedCount: 1 };
      }
    }
    return { deletedCount: 0 };
  }

  async findOneAndUpdate(filter: any, update: any) {
    for (const [id, doc] of this.docs.entries()) {
      if (this.matches(doc, filter)) {
        this.applyUpdate(doc, update);
        this.docs.set(id, doc);
        return JSON.parse(JSON.stringify(doc));
      }
    }
    return null;
  }

  find(filter: any) {
    const matchedDocs: any[] = [];
    for (const doc of this.docs.values()) {
      if (this.matches(doc, filter)) {
        matchedDocs.push(JSON.parse(JSON.stringify(doc)));
      }
    }

    let limitVal = matchedDocs.length;
    const cursor = {
      limit: (n: number) => {
        limitVal = n;
        return cursor;
      },
      [Symbol.asyncIterator]: async function* () {
        for (const item of matchedDocs.slice(0, limitVal)) {
          yield item;
        }
      },
    };
    return cursor as any;
  }

  watch() {
    if (this.shouldFailWithReplicaSet) {
      const err: any = new Error(
        "The $changeStream stage is only supported on replica sets"
      );
      err.code = 40573;
      err.codeName = "Location40573";
      throw err;
    }

    const emitter: any = {
      on: (event: string, handler: (data: any) => void) => {
        if (event === "change") {
          this.changeStreamListeners.push(handler);
        } else if (event === "error") {
          this.errorListeners.push(handler);
        }
        return emitter;
      },
      close: async () => {},
    };
    return emitter;
  }

  private matches(doc: any, filter: any): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (key === "_id" && String(doc._id) !== String(value)) {
        return false;
      }
      if (key === "status") {
        if (typeof value === "object" && value !== null && "$in" in value) {
          if (!(value as any).$in.includes(doc.status)) return false;
        } else if (doc.status !== value) {
          return false;
        }
      }
      if (key === "runAt" && typeof value === "object" && value !== null) {
        if ("$lte" in value) {
          const target = new Date((value as any).$lte).getTime();
          const docTime = new Date(doc.runAt).getTime();
          if (docTime > target) return false;
        }
      }
      if (key === "leaseExpiresAt" && typeof value === "object" && value !== null) {
        if ("$lt" in value) {
          if (!doc.leaseExpiresAt) return false;
          const target = new Date((value as any).$lt).getTime();
          const docTime = new Date(doc.leaseExpiresAt).getTime();
          if (docTime >= target) return false;
        }
      }
      if (key === "lockedBy" && doc.lockedBy !== value) {
        return false;
      }
    }
    return true;
  }

  private applyUpdate(doc: any, update: any) {
    if (update.$set) {
      Object.assign(doc, update.$set);
    }
    if (update.$inc) {
      for (const [k, v] of Object.entries(update.$inc)) {
        doc[k] = (doc[k] || 0) + (v as number);
      }
    }
  }
}

export function createMockDb(): Db {
  const collections = new Map<string, MockCollection>();

  const mockDb: any = {
    collection: (name: string) => {
      if (!collections.has(name)) {
        collections.set(name, new MockCollection(name));
      }
      return collections.get(name)! as unknown as Collection;
    },
  };

  return mockDb as Db;
}
