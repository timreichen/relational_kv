interface Relations {
  set<T>(
    leftKey: Deno.KvKey,
    rightKey: Deno.KvKey,
    data: T,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError>;
  get<T, U = unknown>(
    key: Deno.KvKey,
  ): Promise<RelationKvEntry<T, U> | undefined>;
  getMany<T, U = unknown>(
    keys: Deno.KvKey[],
  ): Promise<RelationKvEntry<T, U>[]>;
  delete(
    leftKey: Deno.KvKey,
    rightKey: Deno.KvKey,
  ): Promise<Deno.KvCommitResult | Deno.KvCommitError>;
  list<T, U = unknown>(
    selector: { prefix: Deno.KvKey },
  ): AsyncGenerator<RelationKvEntry<T, U>>;
}

export interface RelationKv extends Deno.Kv {
  relations: Relations;
}
interface RelationValue<T> {
  value: T;
  key: Deno.KvKey;
}

export interface RelationKvEntry<T, U> extends Deno.KvEntry<T> {
  relations: { value: U };
}

const RELATIONS = ":relations:";

export async function openRelationKv(path?: string) {
  const kv = await Deno.openKv(path) as RelationKv;
  const relations: Relations = {
    async set<T>(leftKey: Deno.KvKey, rightKey: Deno.KvKey, value: T) {
      const [leftEntry, rightEntry] = await kv.getMany([leftKey, rightKey]);
      const leftRelationalValue: RelationValue<T> = { key: leftKey, value };
      const rightRelationalValue: RelationValue<T> = { key: rightKey, value };
      return await kv.atomic()
        .check(leftEntry)
        .check(rightEntry)
        .set([RELATIONS, ...leftKey, ...rightKey], rightRelationalValue)
        .set([RELATIONS, ...rightKey, ...leftKey], leftRelationalValue)
        .commit();
    },
    async get<T, U>(
      key: Deno.KvKey,
    ): Promise<RelationKvEntry<T, U> | undefined> {
      for await (const entry of this.list<T, U>({ prefix: key })) {
        return entry;
      }
    },
    async getMany<T, U>(
      keys: Deno.KvKey[],
    ): Promise<RelationKvEntry<T, U>[]> {
      const items: RelationKvEntry<T, U>[] = [];
      const entries = await kv.getMany<RelationValue<U>[]>(
        keys.map((key) => [RELATIONS, ...key]),
      );
      for (const entry of entries) {
        if (!entry.value) continue;
        const referenceEntry = await kv.get(entry.value.key);
        items.push({
          ...referenceEntry,
          relations: { value: entry.value.value },
        } as RelationKvEntry<T, U>);
      }
      return items;
    },
    async delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
      return await kv.atomic()
        .delete([RELATIONS, ...leftKey, ...rightKey])
        .delete([RELATIONS, ...rightKey, ...leftKey])
        .commit();
    },
    async *list<T, U>(
      selector: { prefix: Deno.KvKey },
    ): AsyncGenerator<RelationKvEntry<T, U>> {
      for await (
        const entry of kv.list<RelationValue<U>>({
          prefix: [RELATIONS, ...selector.prefix],
        })
      ) {
        const referenceEntry = await kv.get<T>(entry.value.key);
        yield {
          ...referenceEntry,
          relations: { value: entry.value.value },
        } as RelationKvEntry<T, U>;
      }
    },
  };
  Object.defineProperty(kv, "relations", { value: relations });
  return kv;
}
