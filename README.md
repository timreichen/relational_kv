# relational deno kv

Wrapper that extends the current deno kv API with simple relational data
capabilities.

> 🚧 This is a proof of concept and will likey be changed. Do not use in
> production. 🚧

## Usage

```ts
import { relationKv } from "./mod.ts";
const kv = await relationKv(await Deno.openKv(":memory:"));
```

## Relations

### Setup

```ts
import { relationKv } from "./mod.ts";
const kv = await relationKv(await Deno.openKv(":memory:"));

const alice = ["students", "alice"];
const bob = ["students", "bob"];

const maths = ["classes", "maths"];

const lesson1 = ["lessons", "2023-01-01"];
const lesson2 = ["lessons", "2023-01-08"];

await kv.set(alice, { name: "Alice" });
await kv.set(bob, { name: "Bob" });

await kv.set(maths, { name: "Maths" });

await kv.set(lesson1, { date: "2023-01-01" });
await kv.set(lesson2, { date: "2023-01-08" });
```

### Set relations

```ts
await kv.relations.set(alice, maths, { mark: "C" });
await kv.relations.set(bob, maths, { mark: "F" });

await kv.relations.set(maths, lesson1);
await kv.relations.set(maths, lesson2);
```

### Get relations

```ts
await kv.relations.get(alice, maths); // output: { mark: "C" }
await kv.relations.get(bob, maths); // output: { mark: "F" }
```

### Data composition with `get`

```ts
await kv.composition.get(bob, {
  class: { get: ["classes"] },
});
```

returns

```ts
{
  name: "Bob",
  class: { value: { name: "Maths" }, relation: { mark: "F" } }
}
```

### Data composition with `getMany`

```ts
await kv.composition.get(maths, {
  students: { getMany: ["students"] },
});
```

returns

```ts
{
  name: "Maths",
  students: [
    { value: { name: "Alice" }, relation: { mark: "C" } },
    { value: { name: "Bob" }, relation: { mark: "F" } }
  ]
}
```

### Nested Data composition

```ts
await kv.composition.get(alice, {
  class: { get: ["classes"], value: { lessons: { getMany: ["lessons"] } } },
});
```

returns

```ts
{
  name: "Alice",
  class: {
    value: {
      name: "Maths",
      lessons: [
        { value: { date: "2023-01-01" } },
        { value: { date: "2023-01-08" } }
      ]
    },
    relation: { mark: "C" }
  }
}
```
