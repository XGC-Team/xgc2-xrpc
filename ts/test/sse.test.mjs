import assert from "node:assert/strict";
import { test } from "node:test";
import { SseParser } from "../dist/sse.js";

function parse(chunks, max = 1 << 20) {
  const parser = new SseParser(max);
  return chunks.flatMap((chunk) => parser.feed(chunk));
}

const WHOLE =
  "﻿retry: 1000\n\n" + // BOM, ignored field, no frame
  ": hb\n\n" + // comment
  "id: 1\nevent: update\ndata: {\"a\":1}\n\n" +
  "data: one\ndata: two\r\ndata\rdata:  three\n\n" + // multi-line, CRLF, bare CR, no colon, extra space
  "event: closing\n\n" + // event without data
  "id: 2\n\n" + // cursor-only frame
  "id: bad\0id\ndata: x\n\n" + // NUL in id is ignored
  "id:\ndata: cleared\n\n"; // empty id

const EXPECTED = [
  { event: "update", data: '{"a":1}', id: "1" },
  { event: undefined, data: "one\ntwo\n\n three", id: undefined },
  { event: "closing", data: undefined, id: undefined },
  { event: undefined, data: undefined, id: "2" },
  { event: undefined, data: "x", id: undefined },
  { event: undefined, data: "cleared", id: "" },
];

test("frames, comments, line endings and fields follow the SSE rules", () => {
  assert.deepEqual(parse([WHOLE]), EXPECTED);
});

test("the result does not depend on how the text is split into chunks", () => {
  for (let at = 0; at <= WHOLE.length; at++) {
    assert.deepEqual(parse([WHOLE.slice(0, at), WHOLE.slice(at)]), EXPECTED, `split at ${at}`);
  }
  assert.deepEqual(parse([...WHOLE]), EXPECTED, "one character at a time");
});

test("a CRLF pair split across chunks ends one line, not two", () => {
  assert.deepEqual(parse(["data: a\r", "\ndata: b\r\n\r", "\n"]), [{ event: undefined, data: "a\nb", id: undefined }]);
  assert.deepEqual(parse(["data: a\r", "\r"]), [{ event: undefined, data: "a", id: undefined }]);
});

test("only a leading BOM is skipped and an unfinished frame is dropped", () => {
  assert.deepEqual(parse(["", "﻿data: a\n\n"]), [{ event: undefined, data: "a", id: undefined }]);
  assert.deepEqual(parse(["data: a\n\n﻿data: b\n\n"]), [
    { event: undefined, data: "a", id: undefined },
  ]);
  assert.deepEqual(parse(["data: whole\n\ndata: cut off\n"]), [{ event: undefined, data: "whole", id: undefined }]);
});

test("a frame larger than the ceiling is refused", () => {
  const parser = new SseParser(64);
  assert.throws(() => parser.feed("data: " + "x".repeat(100)), (error) => error.code === "resource_exhausted");
  const lines = new SseParser(64);
  assert.throws(
    () => {
      for (let i = 0; i < 20; i++) lines.feed("data: xxxxxxxx\n");
    },
    (error) => error.code === "resource_exhausted",
  );
  const fine = new SseParser(64);
  for (let i = 0; i < 100; i++) assert.equal(fine.feed("data: xxxxxxxx\n\n").length, 1);
});
