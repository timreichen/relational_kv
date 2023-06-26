import { assertEquals } from "https://deno.land/std@0.192.0/testing/asserts.ts";

import { relationKv } from "./mod.ts";

Deno.test("relations.set", async () => {
  const kv = relationKv(await Deno.openKv(":memory:"));

  await kv.set(["foo"], "foo");
  await kv.set(["bar"], "bar");
  await kv.relations.set(["foo"], ["bar"], "data");

  const leftEntry = (await kv.get([":relations:", "foo", "bar"])).value;
  assertEquals(leftEntry, { key: ["bar"], value: "data" });

  const rightEntry = (await kv.get([":relations:", "bar", "foo"])).value;
  assertEquals(rightEntry, { key: ["foo"], value: "data" });

  kv.close();
});
Deno.test("relations.delete", async () => {
  const kv = relationKv(await Deno.openKv(":memory:"));

  await kv.set(["foo"], "foo");
  await kv.set(["bar"], "bar");
  await kv.relations.set(["foo"], ["bar"], "data");

  await kv.relations.delete(["foo"], ["bar"]);

  const leftData = await kv.relations.get(["foo"], ["bar"]);
  assertEquals(leftData, undefined);
  const rightData = await kv.relations.get(["bar"], ["foo"]);
  assertEquals(rightData, undefined);

  kv.close();
});
Deno.test("relations.get", async () => {
  const kv = relationKv(await Deno.openKv(":memory:"));

  await kv.set(["foo"], "foo");
  await kv.set(["bar"], "bar");
  await kv.relations.set(["foo"], ["bar"], "data");

  const leftData = await kv.relations.get(["foo"], ["bar"]);
  assertEquals(leftData, "data");
  const rightData = await kv.relations.get(["bar"], ["foo"]);
  assertEquals(rightData, "data");

  kv.close();
});

Deno.test("composition.get", async () => {
  const kv = relationKv(await Deno.openKv(":memory:"));

  await kv.set(["guestlists", "vip"], { name: "vip" });

  await kv.set(["guests", "alice"], "alice");
  await kv.set(["guests", "bob"], "bob");

  await kv.relations.set(["guestlists", "vip"], ["guests", "alice"], {
    number: 1,
  });
  await kv.relations.set(["guestlists", "vip"], ["guests", "bob"], {
    number: 2,
  });

  const list = await kv.composition.get(["guestlists", "vip"], {
    guests: { getMany: ["guests"] },
  });

  assertEquals(list, {
    name: "vip",
    guests: [
      { value: "alice", relation: { number: 1 } },
      { value: "bob", relation: { number: 2 } },
    ],
  });

  kv.close();
});
Deno.test("composition.get", async () => {
  const kv = relationKv(await Deno.openKv(":memory:"));

  await kv.set(["guestlists", "vip"], { name: "vip" });

  await kv.set(["guests", "alice"], "alice");
  await kv.set(["guests", "bob"], "bob");

  await kv.relations.set(["guestlists", "vip"], ["guests", "alice"], {
    number: 1,
  });
  await kv.relations.set(["guestlists", "vip"], ["guests", "bob"], {
    number: 2,
  });

  const generator =  kv.composition.list({ prefix: ["guestlists"] }, {
    guests: { getMany: ["guests"] },
  })

  assertEquals((await generator.next()).value, {
    name: "vip",
    guests: [
      { value: "alice", relation: { number: 1 } },
      { value: "bob", relation: { number: 2 } },
    ],
  });
  assertEquals((await generator.next()).done, true);

  kv.close();
});
