export class Table<T> {
  #kv: Deno.Kv;
  #key: Deno.KvKey;
  constructor(kv: Deno.Kv, key: Deno.KvKey) {
    this.#kv = kv;
    this.#key = key;
  }
  get key() {
    return this.#key;
  }

  async add(data: T) {
    const id = crypto.randomUUID();
    await this.set(id, data);
    return await this.get(id);
  }
  async update(id: string, data: Partial<T>) {
    const oldData = (await this.get(id)).value as T;
    const newData = { ...oldData, ...data };
    await this.set(id, newData);
  }
  async set(id: string, data: T) {
    await this.#kv.set([...this.key, id], data);
  }
  async has(id: string) {
    return await this.get(id) !== undefined;
  }
  async get(id: string) {
    return await this.#kv.get<T>([...this.key, id]);
  }
  async getAll() {
    const items: Deno.KvEntry<T>[] = [];
    for await (const entry of this.#kv.list<T>({ prefix: this.#key })) {
      items.push(entry);
    }
    return items;
  }
  async find(callback: (entry: Deno.KvEntry<T>) => boolean) {
    for await (const entry of this.#kv.list<T>({ prefix: this.#key })) {
      if (callback(entry)) return entry;
    }
  }
  async filter(callback: (entry: Deno.KvEntry<T>) => boolean) {
    const items: Deno.KvEntry<T>[] = [];
    for await (const entry of this.#kv.list<T>({ prefix: this.#key })) {
      if (callback(entry)) items.push(entry);
    }
    return items;
  }
}

export type RelationalKvEntry<R, T> = Deno.KvEntry<T> & { relationalValue: R };

export class RelationalTable<R, LeftT = unknown, RightT = unknown> {
  #kv: Deno.Kv;
  #key: Deno.KvKey;

  constructor(kv: Deno.Kv, leftTable: Table<LeftT>, rightTable: Table<RightT>) {
    this.#kv = kv;
    this.#key = [":" + [...leftTable.key, ...rightTable.key].join(":") + ":"];
  }

  async set(leftKey: Deno.KvKey, rightKey: Deno.KvKey, data?: R) {
    const leftRelationalKey = [...this.#key, ...rightKey];
    const leftRelationalEntry = await this.#kv.get<Deno.KvKey>(
      leftRelationalKey,
    );

    const rightRelationalKey = [...this.#key, ...leftKey];
    const rightRelationalEntry = await this.#kv.get<Deno.KvKey>(
      rightRelationalKey,
    );

    await this.#kv.atomic()
      .set([...leftRelationalEntry.key, ...leftKey], data)
      .set([...rightRelationalEntry.key, ...rightKey], data)
      .commit();
  }
  async delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
    const leftRelationalKey = [...this.#key, ...rightKey, ...leftKey];
    const rightRelationalKey = [...this.#key, ...leftKey, ...rightKey];

    await this.#kv.atomic()
      .delete(leftRelationalKey)
      .delete(rightRelationalKey)
      .commit();
  }
  async get<T>(key: Deno.KvKey) {
    key = [...this.#key, ...key];
    for await (
      const entry of this.#kv.list<R>({ prefix: key })
    ) {
      const referenceKey = entry.key.slice(key.length);
      const referenceEntry = await this.#kv.get<T>(referenceKey);
      const relationalEntry = {
        ...referenceEntry,
        relationalValue: entry.value,
      } as RelationalKvEntry<R, T>;
      return relationalEntry;
    }
  }
  async getMany<T>(key: Deno.KvKey) {
    const entries: RelationalKvEntry<R, T>[] = [];
    key = [...this.#key, ...key];
    for await (
      const entry of this.#kv.list<R>({ prefix: key })
    ) {
      const referenceKey = entry.key.slice(key.length);
      const referenceEntry = await this.#kv.get<T>(referenceKey);
      const relationalEntry = {
        ...referenceEntry,
        relationalValue: entry.value,
      } as RelationalKvEntry<R, T>;
      entries.push(relationalEntry);
    }
    return entries;
  }
}
