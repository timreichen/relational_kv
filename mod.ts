const keySymbol = Symbol("key");

export class Table<T> {
  #kv: Deno.Kv;
  [keySymbol]: Deno.KvKey;
  constructor(kv: Deno.Kv, key: Deno.KvKey) {
    this.#kv = kv;
    this[keySymbol] = key;
  }

  async add(data: T) {
    const id = crypto.randomUUID();
    return await this.set(id, data);
  }
  async update(id: string, data: Partial<T>) {
    const oldData = (await this.get(id)).value;
    const newData = { ...oldData, ...data } as T;
    await this.set(id, newData);
  }
  async set(id: string, data: T) {
    const key = [...this[keySymbol], id];
    await this.#kv.set(key, data);
    return key;
  }
  async has(id: string) {
    return await this.get(id) !== undefined;
  }
  async get(id: string) {
    return await this.#kv.get<T>([...this[keySymbol], id]);
  }
  async getMany(ids: string[]) {
    return await this.#kv.getMany<T[]>(
      ids.map((id) => [...this[keySymbol], id]),
    );
  }
  async getAll() {
    const items: Deno.KvEntry<T>[] = [];
    for await (const entry of this) items.push(entry);
    return items;
  }
  async find(callback: (entry: Deno.KvEntry<T>) => boolean) {
    for await (const entry of this.entries()) {
      if (callback(entry)) return entry;
    }
  }
  async filter(callback: (entry: Deno.KvEntry<T>) => boolean) {
    const items: Deno.KvEntry<T>[] = [];
    for await (const entry of this.entries()) {
      if (callback(entry)) items.push(entry);
    }
    return items;
  }

  entries() {
    return this[Symbol.asyncIterator]();
  }
  async *keys() {
    for await (const entry of this) {
      yield entry.key;
    }
  }
  async *values() {
    for await (const entry of this) yield entry.value;
  }
  async *[Symbol.asyncIterator](): AsyncIterableIterator<Deno.KvEntry<T>> {
    for await (const entry of this.#kv.list<T>({ prefix: this[keySymbol] })) {
      yield entry;
    }
  }
}

export type RelationalKvEntry<R, T> = Deno.KvEntry<T> & { relationalValue: R };

export class RelationalTable<R, LeftT = unknown, RightT = unknown> {
  #kv: Deno.Kv;
  [keySymbol]: Deno.KvKey;

  constructor(
    kv: Deno.Kv,
    leftTable: Table<LeftT>,
    rightTable: Table<RightT>,
  ) {
    this.#kv = kv;
    this[keySymbol] = [
      ":relational:",
      ...leftTable[keySymbol],
      ...rightTable[keySymbol],
    ];
  }

  async set(leftKey: Deno.KvKey, rightKey: Deno.KvKey, data?: R) {
    const leftRelationalKey = [...this[keySymbol], ...rightKey];
    const leftRelationalEntry = await this.#kv.get<Deno.KvKey>(
      leftRelationalKey,
    );

    const rightRelationalKey = [...this[keySymbol], ...leftKey];
    const rightRelationalEntry = await this.#kv.get<Deno.KvKey>(
      rightRelationalKey,
    );
    await this.#kv.atomic()
      .set([...leftRelationalEntry.key, ...leftKey], data)
      .set([...rightRelationalEntry.key, ...rightKey], data)
      .commit();
  }
  async delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
    const leftRelationalKey = [...this[keySymbol], ...rightKey, ...leftKey];
    const rightRelationalKey = [...this[keySymbol], ...leftKey, ...rightKey];

    await this.#kv.atomic()
      .delete(leftRelationalKey)
      .delete(rightRelationalKey)
      .commit();
  }
  async has(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
    return await this.get(leftKey, rightKey) !== undefined;
  }
  async get<T>(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
    for await (
      const entry of this.entries<T>([
        ...this[keySymbol],
        ...leftKey,
        ...rightKey,
      ])
    ) return entry;
  }
  async getAll<T>(key: Deno.KvKey) {
    const entries: RelationalKvEntry<R, T>[] = [];
    for await (const entry of this.entries<T>(key)) entries.push(entry);
    return entries;
  }

  async find<T>(
    key: Deno.KvKey,
    callback: (entry: Deno.KvEntry<T>) => boolean,
  ) {
    for await (const entry of this.entries<T>(key)) {
      if (callback(entry)) return entry;
    }
  }
  async filter<T>(
    key: Deno.KvKey,
    callback: (entry: Deno.KvEntry<T>) => boolean,
  ) {
    const items: Deno.KvEntry<T>[] = [];
    for await (const entry of this.entries<T>(key)) {
      if (callback(entry)) items.push(entry);
    }
    return items;
  }

  async *keys<T>(key: Deno.KvKey) {
    for await (const entry of this.entries<T>(key)) {
      yield entry.key;
    }
  }
  async *values<T>(key: Deno.KvKey) {
    for await (const entry of this.entries<T>(key)) yield entry.value;
  }
  async *entries<T>(key: Deno.KvKey) {
    key = [...this[keySymbol], ...key];
    for await (const entry of this.#kv.list<R>({ prefix: key })) {
      const referenceKey = entry.key.slice(key.length);
      const referenceEntry = await this.#kv.get<T>(referenceKey);
      const relationalEntry = {
        ...referenceEntry,
        relationalValue: entry.value,
      } as RelationalKvEntry<R, T>;
      yield relationalEntry;
    }
  }
}
