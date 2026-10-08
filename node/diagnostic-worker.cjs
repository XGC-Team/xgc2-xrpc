"use strict";
const { parentPort, workerData } = require("node:worker_threads");
const { writeSync } = require("node:fs");

// Only this owned worker performs blocking stderr I/O. It opens no file and
// performs no rotation; the declared supervisor owns collection and rotation.
// A stalled write keeps the worker alive. Main-thread close must report failed
// quiescence, not terminate/unref the worker or abandon the admitted record.
const queue = new Array(workerData.maxPendingRecords);
let head = 0, length = 0, active = false, closing = false;

function maybeExit() {
  if (!closing || active || length) return;
  // All admitted writes have completed and their ACKs were posted. Closing the
  // port alone does not stop a Bun worker; exit this owned worker explicitly.
  // The parent still waits for the native Worker exit event before completing.
  parentPort.close();
  process.exit(0);
}

async function writeRecord(message) {
  let ok = false;
  try {
    if (typeof message.line !== "string" || Buffer.byteLength(message.line) > workerData.maxRecordBytes) throw new Error("invalid bounded diagnostic record");
    const buffer = Buffer.from(message.line);
    let offset = 0;
    while (offset < buffer.length) {
      try {
        const written = writeSync(2, buffer, offset, buffer.length - offset);
        if (written <= 0) throw new Error("diagnostic sink write failed");
        offset += written;
      } catch (error) {
        // Supervisors may provide a nonblocking pipe/socket as fd2. Backpressure
        // retains this same active record and its admission; it is not a sink
        // failure and does not create another retry queue. A blocking fd can
        // instead stall writeSync itself; either case remains owned until real
        // completion and can make a finite main-thread close fail truthfully.
        if (error.code !== "EAGAIN" && error.code !== "EWOULDBLOCK") throw error;
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    }
    ok = true;
  } catch {
    // No retry queue or recursive logging on the failed sink.
  }
  parentPort.postMessage({ kind: "ack", id: message.id, ok });
}

async function pump() {
  if (active) return;
  active = true;
  while (length) {
    const message = queue[head]; queue[head] = undefined;
    head = (head + 1) % queue.length; length--;
    await writeRecord(message);
  }
  active = false;
  maybeExit();
}

parentPort.on("message", (message) => {
  if (message.kind === "close") { closing = true; maybeExit(); return; }
  if (message.kind !== "record") return;
  if (length >= queue.length) { parentPort.postMessage({ kind: "ack", id: message.id, ok: false }); return; }
  queue[(head + length) % queue.length] = message; length++;
  void pump();
});
