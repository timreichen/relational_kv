import { openRelationKv } from "./mod.ts";

export const kv = await openRelationKv(":memory:");

interface Student {
  name: string;
}
interface Class {
  name: string;
}
interface ClassStudent {
  mark: string;
}

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

console.log("Math Students");
for await (
  const entry of await kv.relations.list<Student, ClassStudent>({
    prefix: [...maths, "students"],
  })
) {
  console.log("-", `${entry.value.name}:`, entry.relations.value.mark);
}

console.log("Bob's classes");
for await (
  const entry of await kv.relations.list<Class, ClassStudent>({
    prefix: [...bob, "classes"],
  })
) {
  console.log("-", `${entry.value.name}:`, entry.relations.value.mark);
}
