export class MongoSnoozeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MongoSnoozeError";
  }
}

export class JobNotFoundError extends MongoSnoozeError {
  constructor(public readonly jobId: string) {
    super(`Job with ID "${jobId}" was not found.`);
    this.name = "JobNotFoundError";
  }
}

export class HandlerNotRegisteredError extends MongoSnoozeError {
  constructor(public readonly jobName: string) {
    super(`No handler registered for job type "${jobName}".`);
    this.name = "HandlerNotRegisteredError";
  }
}

export class ReplicaSetRequiredError extends MongoSnoozeError {
  constructor() {
    super(
      "MongoDB Change Streams require a replica set or sharded cluster. MongoSnooze cannot start change streams on a standalone MongoDB deployment."
    );
    this.name = "ReplicaSetRequiredError";
  }
}

export class IndexConflictError extends MongoSnoozeError {
  constructor(public readonly originalError: Error) {
    super(
      `An incompatible index already exists on the collection. Please migrate or drop the existing conflicting index: ${originalError.message}`
    );
    this.name = "IndexConflictError";
  }
}

export class LeaseLostError extends MongoSnoozeError {
  constructor(public readonly jobId: string) {
    super(`Worker lost execution lease for job "${jobId}" to another worker or recovery sweep.`);
    this.name = "LeaseLostError";
  }
}
