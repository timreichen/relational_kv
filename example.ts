import { relationKv } from "./mod.ts";

const kv = relationKv(await Deno.openKv(":memory:"));

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
await kv.relations.set(alice, biology, { mark: "B" });

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
  key: [ "students", "alice" ],
  value: { name: "Alice" },
  versionstamp: "00000000000000010000",
  composition: {
    classes: [
      {
        key: [ "classes", "biology" ],
        value: { name: "Biology" },
        versionstamp: "00000000000000040000",
        composition: { lessons: [] },
        relation: { mark: "B" }
      },
      {
        key: [ "classes", "maths" ],
        value: { name: "Maths" },
        versionstamp: "00000000000000030000",
        composition: {
          lessons: [
            {
              key: [ "lessons", "2023-01-01" ],
              value: { date: "2023-01-01" },
              versionstamp: "00000000000000050000"
            },
            {
              key: [ "lessons", "2023-01-08" ],
              value: { date: "2023-01-08" },
              versionstamp: "00000000000000060000"
            }
          ]
        },
        relation: { mark: "C" }
      }
    ]
  }
}
*/

const composedLesson = await kv.composition.get(lesson1, {
  class: { get: ["classes"] },
  students: { getMany: ["students"] },
});
console.log(composedLesson);
/*
{
  key: [ "lessons", "2023-01-01" ],
  value: { date: "2023-01-01" },
  versionstamp: "00000000000000050000",
  composition: {
    class: {
      key: [ "classes", "maths" ],
      value: { name: "Maths" },
      versionstamp: "00000000000000030000"
    },
    students: [
      {
        key: [ "students", "alice" ],
        value: { name: "Alice" },
        versionstamp: "00000000000000010000",
        relation: { status: "late" }
      },
      {
        key: [ "students", "bob" ],
        value: { name: "Bob" },
        versionstamp: "00000000000000020000",
        relation: { status: "present" }
      }
    ]
  }
}
*/
