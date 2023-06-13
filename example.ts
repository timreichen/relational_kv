import { RelationalTable, Table } from "./mod.ts";

interface Student {
  name: string;
}
interface Class {
  name: string;
}
interface ClassStudent {
  status: string;
}

export const kv = await Deno.openKv(":memory:");

const students = new Table<Student>(kv, ["students"]);
const classes = new Table<Class>(kv, ["classes"]);
const classStudents = new RelationalTable<ClassStudent>(kv, classes, students);

const maths = await classes.add({ name: "Maths" });
const biology = await classes.add({ name: "Biology" });

const alice = await students.add({ name: "Alice" });
await classStudents.set(maths.key, alice.key, { status: "present" });
await classStudents.set(biology.key, alice.key, { status: "late" });

const bob = await students.add({ name: "Bob" });
await classStudents.set(maths.key, bob.key, { status: "absent" });
await classStudents.set(biology.key, bob.key, { status: "present" });

const bobsClasses = await classStudents.getMany<Class>(bob.key);
console.log(bobsClasses);
/*
[
  { key: "...", versionstamp: "...", value: { name: "Maths" }, relationalValue: { status: "absent" } },
  { key: "...", versionstamp: "...", value: { name: "Biology" }, relationalValue: { status: "present" } },
]
*/

const biologyStudents = await classStudents.getMany<Student>(biology.key);
console.log(biologyStudents);
/*
[
  { key: "...", versionstamp: "...", value: { name: "Alice" }, relationalValue: { status: "late" } },
  { key: "...", versionstamp: "...", value: { name: "Bob" }, relationalValue: { status: "present" } },
]
*/
