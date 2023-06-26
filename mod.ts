export interface RelationKv extends Deno.Kv {
  relations: {
    /**
     * @example
     * ```ts
     * import { relationKv } from "./mod.ts";
     * const kv = await relationKv(await Deno.openKv());
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
     * import { relationKv } from "./mod.ts";
     * const kv = await relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.relations.delete(alice, maths);
     * ```
     */
    delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey): Promise<void>;
    /**
     * @example
     * ```ts
     * import { relationKv } from "./mod.ts";
     * const kv = await relationKv(await Deno.openKv());
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
  composition: {
    /**
     * @example
     * ```ts
     * import { relationKv } from "./mod.ts";
     * const kv = await relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.set(alice, { name: "Alice" });
     * await kv.set(maths, { name: "Maths" });
     * await kv.relations.set(alice, maths, { mark: "A+" });
     * const compositionEntry = await kv.composition.get(alice, { classes: { getMany: ["classes"] } })
     * ```
     */
    get<
      T,
      C extends Record<PropertyKey, unknown> = Record<PropertyKey, unknown>,
    >(
      key: Deno.KvKey,
      compositionSelector: CompositionSelector,
    ): Promise<Readonly<CompositionKvEntry<T, C>>>;
    /**
     * @example
     * ```ts
     * import { relationKv } from "./mod.ts";
     * const kv = await relationKv(await Deno.openKv());
     * const alice = ["students", "alice"];
     * const maths = ["classes", "maths"];
     * await kv.set(alice, { name: "Alice" });
     * await kv.set(maths, { name: "Maths" });
     * await kv.relations.set(alice, maths, { mark: "A+" });
     * for await (const compositionEntry of kv.composition.list({ prefix: ["students"] }, { classes: { getMany: ["classes"] } })) {
     *   compositionEntry.key; // ["users", "alice"]
     *   compositionEntry.value; // { name: "Alice" }
     *   compositionEntry.versionstamp; // "00000000000000010000"
     *   compositionEntry.composition; // { classes: [ { key: [ "classes", "maths" ], value: { name: "Maths" }, versionstamp: "00000000000000020000", relation: { mark: "A+" } } ] }
     * }
     * ```
     */
    list<
      T,
      C extends Record<PropertyKey, unknown> = Record<PropertyKey, unknown>,
    >(
      selector: Deno.KvListSelector,
      compositionSelector: CompositionSelector,
    ): IterableIterator<Readonly<CompositionKvEntry<T, C>>>;
  };
  atomic(): RelationAtomicOperation;
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

interface CompositionSelectorBase {
  value?: CompositionSelector;
  relationKey?: string;
}
interface CompositionGetManySelector extends CompositionSelectorBase {
  get?: never;
  getMany: Deno.KvKey;
}
interface CompositionGetSelector extends CompositionSelectorBase {
  get: Deno.KvKey;
  getMany?: never;
}
export interface CompositionSelector {
  [K: string]: CompositionGetSelector | CompositionGetManySelector;
}
export interface CompositionKvEntry<T, C = never, R = never>
  extends Deno.KvEntry<T> {
  relation?: R;
  composition: C;
}

const RELATION_KEY = ":relations:";

function setRelation<T>(
  atomic: Deno.AtomicOperation,
  leftKey: Deno.KvKey,
  rightKey: Deno.KvKey,
  value?: T,
) {
  return atomic
    .set([RELATION_KEY, ...leftKey, ...rightKey], { key: rightKey, value })
    .set([RELATION_KEY, ...rightKey, ...leftKey], { key: leftKey, value });
}
function deleteRelation<T>(
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
  compositionSelector: CompositionSelector,
) {
  const composition: Record<PropertyKey, unknown> = {};

  for (
    const [key, { get, getMany, value, relationKey = "relation" }] of Object
      .entries(
        compositionSelector as CompositionSelector,
      )
  ) {
    if (getMany) composition[key] = [];

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
      if (value) subEntry = await compose(kv, subEntry, value);
      const composedEntry = { ...subEntry } as Record<PropertyKey, unknown>;
      const relationValue = relationEntry.value.value;
      if (relationValue !== undefined) {
        composedEntry[relationKey] = relationValue;
      }

      if (getMany) {
        (composition[key] as unknown[]).push(composedEntry);
      } else {
        composition[key] = composedEntry;
        break;
      }
    }
  }
  return { ...entry, composition } as CompositionKvEntry<T>;
}

/**
 * @example
 * ```ts
 * import { relationKv } from "./mod.ts";
 * const kv = await relationKv(await Deno.openKv(":memory:"));
 * ```
 */
export function relationKv(kv: Deno.Kv) {
  const atomic = kv.atomic.bind(kv);
  Object.defineProperties(kv, {
    relations: {
      value: {
        async set<T>(leftKey: Deno.KvKey, rightKey: Deno.KvKey, value?: T) {
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
    composition: {
      value: {
        async get<T>(
          key: Deno.KvKey,
          compositionSelector: CompositionSelector,
        ) {
          const entry = await kv.get<T>(key);
          return await compose<T>(kv, entry, compositionSelector);
        },
        async *list<T>(
          selector: Deno.KvListSelector,
          compositionSelector: CompositionSelector,
        ) {
          for await (const entry of kv.list<T>(selector)) {
            yield compose<T>(kv, entry, compositionSelector);
          }
        },
      },
    },
    atomic: {
      value() {
        const result = atomic();
        Object.defineProperty(result, "relations", {
          value: {
            set<T>(
              leftKey: Deno.KvKey,
              rightKey: Deno.KvKey,
              value?: T,
            ) {
              return setRelation(result, leftKey, rightKey, value);
            },
            delete(
              leftKey: Deno.KvKey,
              rightKey: Deno.KvKey,
            ) {
              return deleteRelation(result, leftKey, rightKey);
            },
          },
        });
        return result;
      },
    },
  });
  return kv as RelationKv;
}
