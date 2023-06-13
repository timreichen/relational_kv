import { openRelationKv } from "./mod.ts";

async function fromAsync<T>(generator: AsyncGenerator<T, any, unknown>) {
  const items: T[] = [];
  for await (const item of generator) items.push(item);
  return items;
}

interface Student {
  name: string;
}
interface Class {
  name: string;
}
interface ClassStudent {
  mark: string;
}

const kv = await openRelationKv(":memory:");

const alice = ["students", "alice"];
const bob = ["students", "bob"];
const maths = ["classes", "maths"];
const biology = ["classes", "biology"];

await kv.set(alice, { name: "Alice" });
await kv.set(bob, { name: "Bob" });

await kv.set(maths, { name: "Maths" });
await kv.set(biology, { name: "Biology" });

await kv.relations.set(alice, maths, { mark: "C" });
await kv.relations.set(alice, biology, { mark: "A+" });

await kv.relations.set(bob, maths, { mark: "F" });
await kv.relations.set(bob, biology, { mark: "A+" });

const alicesClasses = await fromAsync(
  kv.relations.list<Class, ClassStudent>({ prefix: [...alice, "classes"] }),
);
console.table(
  alicesClasses.map((entry) => ({
    name: entry.value.name,
    mark: entry.relations.value.mark,
  })),
);
/*
┌───────┬───────────┬──────┐
│ (idx) │ name      │ mark │
├───────┼───────────┼──────┤
│     0 │ "Biology" │ "A+" │
│     1 │ "Maths"   │ "C"  │
└───────┴───────────┴──────┘
*/

const mathStudents = await fromAsync(
  kv.relations.list<Student, ClassStudent>({ prefix: [...maths, "students"] }),
);
console.table(
  mathStudents.map((entry) => ({
    name: entry.value.name,
    mark: entry.relations.value.mark,
  })),
);
/*
┌───────┬─────────┬──────┐
│ (idx) │ name    │ mark │
├───────┼─────────┼──────┤
│     0 │ "Alice" │ "C"  │
│     1 │ "Bob"   │ "F"  │
└───────┴─────────┴──────┘
*/
