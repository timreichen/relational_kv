export interface RelationKvEntry<U> {
  key: Deno.KvKey;
  value?: U;
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

export type CompositionSelector = Record<
  string,
  CompositionGetSelector | CompositionGetManySelector
>;

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

export interface RelationKv extends Deno.Kv {
  relations: {
    set<U>(
      leftKey: Deno.KvKey,
      rightKey: Deno.KvKey,
      value?: U,
    ): Promise<void>;
    delete(leftKey: Deno.KvKey, rightKey: Deno.KvKey): Promise<void>;
    get<U>(leftKey: Deno.KvKey, rightKey: Deno.KvKey): Promise<U | null>;
  };
  composition: {
    get<T>(
      key: Deno.KvKey,
      compositionSelector: CompositionSelector,
    ): Promise<T>;
    list<T>(
      selector: Deno.KvListSelector,
      compositionSelector: CompositionSelector,
    ): AsyncGenerator<T>;
  };
  atomic(): RelationAtomicOperation;
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
): Promise<T> {
  const entryValue = entry.value as any;
  if (!entry.value) return entryValue;

  for (
    const [key, { get, getMany, value, relationKey = "relation" }] of Object
      .entries(
        compositionSelector as CompositionSelector,
      )
  ) {
    if (getMany) entryValue[key] = [];

    for await (
      const relationEntry of kv.list<RelationKvEntry<unknown>>({
        prefix: [
          RELATION_KEY,
          ...entry.key,
          ...(getMany || get) as Deno.KvKey,
        ],
      })
    ) {
      const subEntry = await kv.get(relationEntry.value.key);
      if (!subEntry.value) continue;
      if (value) await compose(kv, subEntry, value);
      const composedValue = {
        value: subEntry.value,
        [relationKey]: relationEntry.value.value,
      };
      if (getMany) {
        entryValue[key].push(composedValue);
      } else {
        entryValue[key] = composedValue;
      }
    }
  }
  return entry.value as T;
}

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
