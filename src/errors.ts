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
