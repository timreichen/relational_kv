/**
 * @module
 *
 * @example
 * ```ts
 * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
 * const kv = relationKv(await Deno.openKv());
 * const alice = ["students", "alice"];
 * const maths = ["classes", "maths"];
 * await kv.set(alice, { name: "Alice" });
 * await kv.set(maths, { name: "Maths" });
 * await kv.relations.set(alice, maths, { mark: "A+" });
 * const result = await kv.relations.get(alice, maths);
 * result; // { mark: "A+" }
 * ```
 *
 * @example
 * ```ts
 * import { relationKv } from "./mod.ts";
 *
 * const kv = relationKv(await Deno.openKv());
 *
 * const alice = ["students", "alice"];
 * const bob = ["students", "bob"];
 *
 * const maths = ["classes", "maths"];
 * const biology = ["classes", "biology"];
 *
 * const lesson1 = ["lessons", "2023-01-01"];
 * const lesson2 = ["lessons", "2023-01-08"];
 *
 * await kv.set(alice, { name: "Alice" });
 * await kv.set(bob, { name: "Bob" });
 *
 * await kv.set(maths, { name: "Maths" });
 * await kv.set(biology, { name: "Biology" });
 *
 * await kv.set(lesson1, { date: "2023-01-01" });
 * await kv.set(lesson2, { date: "2023-01-08" });
 *
 * await kv.relations.set(maths, lesson1);
 * await kv.relations.set(maths, lesson2);
 *
 * await kv.relations.set(alice, maths, { mark: "C" });
 * await kv.relations.set(alice, biology, { mark: "B" });
 *
 * await kv.relations.set(alice, lesson1, { status: "late" });
 * await kv.relations.set(alice, lesson2, { status: "present" });
 *
 * await kv.relations.set(bob, maths, { mark: "F" });
 * await kv.relations.set(bob, biology, { mark: "A+" });
 *
 * await kv.relations.set(bob, lesson1, { status: "present" });
 * await kv.relations.set(bob, lesson2, { status: "absent" });
 *
 * const student = await kv.get(alice, {
 *   relations: {
 *     classes: {
 *       getMany: ["classes"],
 *       relations: { lessons: { getMany: ["lessons"] } },
 *     },
 *   },
 * });
 *
 * const lesson = await kv.get(lesson1, {
 *   relations: {
 *     class: { get: ["classes"] },
 *     students: { getMany: ["students"] },
 *   },
 * });
 * ```
 */

export interface RelationKv extends Deno.Kv {
  relations: {
    /**
     * @example
     * ```ts
     * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
     * const kv = relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.set(alice, { name: "Alice" });
     * await kv.set(maths, { name: "Maths" });
     * await kv.relations.set(alice, maths, { mark: "A+" });
     * ```
     */
    set<U>(
      leftKey: Deno.KvKey,
      rightKey: Deno.KvKey,
      value?: U,
    ): Promise<void>;
    /**
     * @example
     * ```ts
     * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
     * const kv = relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.relations.delete(alice, maths);
     * ```
     */
    delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey): Promise<void>;
    /**
     * @example
     * ```ts
     * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
     * const kv = relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.set(alice, { name: "Alice" });
     * await kv.set(maths, { name: "Maths" });
     * await kv.relations.set(alice, maths, { mark: "A+" });
     * const result = await kv.relations.get(alice, maths);
     * result; // { mark: "A+" }
     * ```
     */
    get<U>(leftKey: Deno.KvKey, rightKey: Deno.KvKey): Promise<U | null>;
  };
  atomic(): RelationAtomicOperation;

  get<T = unknown>(
    key: Deno.KvKey,
    options?: { consistency?: Deno.KvConsistencyLevel },
  ): Promise<Deno.KvEntryMaybe<T>>;
  get<T = unknown>(
    key: Deno.KvKey,
    options: { consistency?: Deno.KvConsistencyLevel } & {
      relations: RelationsSelector;
    },
  ): Promise<RelationalKvEntry<T>>;

  /**
   * @example
   * ```ts
   * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
   * const kv = relationKv(await Deno.openKv());
   * const alice = ["students", "alice"];
   * const maths = ["classes", "maths"];
   * await kv.set(alice, { name: "Alice" });
   * await kv.set(maths, { name: "Maths" });
   * await kv.relations.set(alice, maths, { mark: "A+" });
   * for await (const entry of kv.list(
   *   { prefix: ["students"] },
   *   { relations: { classes: { getMany: ["classes"] } }
   * })) {
   *   entry.key; // ["users", "alice"]
   *   entry.value; // { name: "Alice" }
   *   entry.versionstamp; // "00000000000000010000"
   *   entry.relations; // { classes: [ { key: [ "classes", "maths" ], value: { name: "Maths" }, versionstamp: "00000000000000020000", relation: { mark: "A+" } } ] }
   * }
   * ```
   */
  list<T = unknown>(
    selector: Deno.KvListSelector,
    options: Deno.KvListOptions & { relations: RelationsSelector },
  ): Deno.KvListIterator<RelationalKvEntry<T>>;
  list<T = unknown>(
    selector: Deno.KvListSelector,
    options?: Deno.KvListOptions,
  ): Deno.KvListIterator<T>;
}
export interface RelationKvEntry<U> {
  key: Deno.KvKey;
  value?: U;
}
export interface RelationAtomicOperation extends Deno.AtomicOperation {
  relations: {
    set<T>(
      leftKey: Deno.KvKey,
      rightKey: Deno.KvKey,
      value?: T,
    ): RelationAtomicOperation;
    delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey): RelationAtomicOperation;
  };
}

interface RelationsSelectorBase {
  relations?: RelationsSelector;
}
interface RelationsGetManySelector extends RelationsSelectorBase {
  get?: never;
  getMany: Deno.KvKey;
}
interface RelationsGetSelector extends RelationsSelectorBase {
  get: Deno.KvKey;
  getMany?: never;
}
export interface RelationsSelector {
  [K: string]: RelationsGetSelector | RelationsGetManySelector;
}
export interface RelationalKvEntry<T, C = unknown, R = never>
  extends Deno.KvEntry<T> {
  relation?: R;
  relations: C;
}

const RELATION_KEY = ":relations:";

function setRelation(
  atomic: Deno.AtomicOperation,
  leftKey: Deno.KvKey,
  rightKey: Deno.KvKey,
  value?: unknown,
) {
  return atomic
    .set([RELATION_KEY, ...leftKey, ...rightKey], { key: rightKey, value })
    .set([RELATION_KEY, ...rightKey, ...leftKey], { key: leftKey, value });
}
function deleteRelation(
  atomic: Deno.AtomicOperation,
  leftKey: Deno.KvKey,
  rightKey: Deno.KvKey,
) {
  return atomic
    .delete([RELATION_KEY, ...leftKey, ...rightKey])
    .delete([RELATION_KEY, ...rightKey, ...leftKey]);
}
async function getRelation<T>(
  kv: Deno.Kv,
  leftKey: Deno.KvKey,
  rightKey: Deno.KvKey,
) {
  const entry = await kv.get<RelationKvEntry<T>>([
    RELATION_KEY,
    ...leftKey,
    ...rightKey,
  ]);
  return entry.value;
}

async function compose<T>(
  kv: Deno.Kv,
  entry: Deno.KvEntryMaybe<T>,
  relationsSelector: RelationsSelector,
) {
  const relationsObject: Record<PropertyKey, unknown> = {};

  for (
    const [key, { get, getMany, relations }] of Object
      .entries(
        relationsSelector as RelationsSelector,
      )
  ) {
    if (getMany) relationsObject[key] = [];

    for await (
      const relationEntry of kv.list<RelationKvEntry<unknown>>({
        prefix: [
          RELATION_KEY,
          ...entry.key,
          ...(getMany || get) as Deno.KvKey,
        ],
      })
    ) {
      let subEntry = await kv.get(relationEntry.value.key);
      if (!subEntry.value) continue;
      if (relations) subEntry = await compose(kv, subEntry, relations);
      const relationsEntry = { ...subEntry } as Record<PropertyKey, unknown>;
      const relationValue = relationEntry.value.value;
      if (relationValue !== undefined) {
        relationsEntry.relation = relationValue;
      }

      if (getMany) {
        (relationsObject[key] as unknown[]).push(relationsEntry);
      } else {
        relationsObject[key] = relationsEntry;
        break;
      }
    }
  }
  return { ...entry, relations: relationsObject } as RelationalKvEntry<T>;
}

/**
 * @example
 * ```ts
 * import { relationKv } from "https://deno.land/x/relational_kv/mod.ts";
 * const kv = relationKv(await Deno.openKv());
 * ```
 */
export function relationKv(kv: Deno.Kv) {
  const atomic = kv.atomic.bind(kv);
  const _delete = kv.delete.bind(kv);
  const get = kv.get.bind(kv);
  const list = kv.list.bind(kv);

  Object.defineProperties(kv, {
    delete: {
      async value(key: Deno.KvKey) {
        const result = await _delete(key);
        for await (
          const entry of list<RelationKvEntry<unknown>>({
            prefix: [RELATION_KEY, ...key],
          })
        ) {
          const atomic = kv.atomic();
          await deleteRelation(atomic, key, entry.value.key)
            .commit();
        }
        return result;
      },
    },
    get: {
      async value<T>(
        key: Deno.KvKey,
        options?:
          & { consistency?: Deno.KvConsistencyLevel }
          & { relations?: RelationsSelector },
      ) {
        const entry = await get<T>(key);
        if (options?.relations) {
          return await compose<T>(kv, entry, options.relations);
        }
        return entry;
      },
    },
    list: {
      async *value<T>(
        selector: Deno.KvListSelector,
        options: Deno.KvListOptions & { relations: RelationsSelector },
      ) {
        for await (const entry of list<T>(selector)) {
          if (options?.relations) {
            yield await compose<T>(kv, entry, options.relations);
          } else {
            yield entry;
          }
        }
      },
    },
    relations: {
      value: {
        async set(leftKey: Deno.KvKey, rightKey: Deno.KvKey, value?: unknown) {
          const atomic = kv.atomic();
          return await setRelation(atomic, leftKey, rightKey, value)
            .commit();
        },
        async delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
          const atomic = kv.atomic();
          return await deleteRelation(atomic, leftKey, rightKey)
            .commit();
        },
        async get<T>(leftKey: Deno.KvKey, rightKey: Deno.KvKey) {
          const entry = await getRelation<T>(kv, leftKey, rightKey);
          return entry?.value;
        },
      },
    },
    atomic: {
      value() {
        const atomicOperation = atomic();
        Object.defineProperty(atomicOperation, "relations", {
          value: {
            set(
              leftKey: Deno.KvKey,
              rightKey: Deno.KvKey,
              value?: unknown,
            ) {
              return setRelation(atomicOperation, leftKey, rightKey, value);
            },
            delete(
              leftKey: Deno.KvKey,
              rightKey: Deno.KvKey,
            ) {
              return deleteRelation(atomicOperation, leftKey, rightKey);
            },
          },
        });
        return atomicOperation;
      },
    },
  });
  return kv as RelationKv;
}
