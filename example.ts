import { relationKv } from "./mod.ts";

const kv = await relationKv(await Deno.openKv(":memory:"));

const alice = ["students", "alice"];
const bob = ["students", "bob"];

const maths = ["classes", "maths"];
const biology = ["classes", "biology"];

const lesson1 = ["lessons", "2023-01-01"];
const lesson2 = ["lessons", "2023-01-08"];

await kv.set(alice, { name: "Alice" });
await kv.set(bob, { name: "Bob" });

await kv.set(maths, { name: "Maths" });
await kv.set(biology, { name: "Biology" });

await kv.set(lesson1, { date: "2023-01-01" });
await kv.set(lesson2, { date: "2023-01-08" });

await kv.relations.set(maths, lesson1);
await kv.relations.set(maths, lesson2);

await kv.relations.set(alice, maths, { mark: "C" });
await kv.relations.set(alice, biology, { mark: "A+" });

await kv.relations.set(alice, lesson1, { status: "late" });
await kv.relations.set(alice, lesson2, { status: "present" });

await kv.relations.set(bob, maths, { mark: "F" });
await kv.relations.set(bob, biology, { mark: "A+" });

await kv.relations.set(bob, lesson1, { status: "present" });
await kv.relations.set(bob, lesson2, { status: "absent" });

const composedStudent = await kv.composition.get(alice, {
  classes: {
    getMany: ["classes"],
    value: { lessons: { getMany: ["lessons"] } },
  },
});
console.log(composedStudent);
/*
{
  name: "Alice",
  classes: [
    {
      value: { name: "Biology", lessons: [] },
      relation: { mark: "A+" }
    },
    {
      value: {
        name: "Maths",
        lessons: [
          { value: { date: "2023-01-01" }, relation: undefined },
          { value: { date: "2023-01-08" }, relation: undefined }
        ]
      },
      relation: { mark: "C" }
    }
  ]
}
*/

const composedLesson = await kv.composition.get(lesson1, {
  classes: {
    getMany: ["classes"],
  },
  students: { getMany: ["students"] },
});
console.log(composedLesson);
/*
{
  date: "2023-01-01",
  classes: [ { value: { name: "Maths" }, relation: undefined } ],
  students: [
    { value: { name: "Alice" }, relation: { status: "late" } },
    { value: { name: "Bob" }, relation: { status: "present" } }
  ]
}
*/
