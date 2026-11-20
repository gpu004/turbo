interface Env {
  QUEUE_BUCKET: string;
  QUEUE_LEASE_SECONDS: string;
}

export function read(env: Env): string {
  return env.QUEUE_LEASE_SECONDS; // expect: turbo/no-raw-env-vars
}

export function binding(env: Env): string {
  return env.QUEUE_BUCKET;
}

export class Broker {
  constructor(private readonly env: Env) {}

  lease(): string {
    return this.env.QUEUE_LEASE_SECONDS; // expect: turbo/no-raw-env-vars
  }
}
