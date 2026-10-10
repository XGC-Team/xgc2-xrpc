import { XrpcError } from "./errors.js";

/**
 * One server-sent-events frame, as written between blank lines. A frame with
 * neither `event` nor `data` (for example one that only carries `id:`) still
 * matters: it moves the resume cursor.
 */
export interface SseFrame {
  /** The `event:` field, if the frame had one. */
  event: string | undefined;
  /** The `data:` lines joined with `\n`, if the frame had any. */
  data: string | undefined;
  /** The `id:` field of this frame; `""` clears the cursor. */
  id: string | undefined;
}

/**
 * Incremental SSE tokenizer (WHATWG "event stream interpretation"): `\n`,
 * `\r` and `\r\n` end lines, even when a pair is split across chunks; lines
 * starting with `:` are comments (heartbeats) and produce nothing; one space
 * after the colon is dropped; an `id:` containing NUL is ignored; a leading
 * BOM is skipped. `retry:` is parsed and ignored: reconnect timing belongs to
 * the client's backoff. A frame cut off by the end of the stream is dropped.
 */
export class SseParser {
  private partial = "";
  private skipLF = false;
  private started = false;
  private event: string | undefined;
  private data: string[] = [];
  private id: string | undefined;
  private size = 0;

  /** @param maxChars ceiling for one unfinished line plus the frame it belongs to */
  constructor(private readonly maxChars: number) {}

  /** Consume decoded text and return the frames it completed. */
  feed(text: string): SseFrame[] {
    const frames: SseFrame[] = [];
    if (!this.started && text !== "") {
      this.started = true;
      if (text.charCodeAt(0) === 0xfeff) text = text.slice(1);
    }
    let index = 0;
    if (this.skipLF && text !== "") {
      this.skipLF = false;
      if (text.charCodeAt(0) === 0x0a) index = 1;
    }
    let lineStart = index;
    for (; index < text.length; index++) {
      const code = text.charCodeAt(index);
      if (code !== 0x0a && code !== 0x0d) continue;
      this.line(this.partial + text.slice(lineStart, index), frames);
      this.partial = "";
      if (code === 0x0d) {
        if (index + 1 < text.length) {
          if (text.charCodeAt(index + 1) === 0x0a) index++;
        } else {
          this.skipLF = true;
        }
      }
      lineStart = index + 1;
    }
    this.partial += text.slice(lineStart);
    if (this.size + this.partial.length > this.maxChars) {
      throw new XrpcError(
        "resource_exhausted",
        `event frame exceeds ${this.maxChars} characters`,
        "response_received",
      );
    }
    return frames;
  }

  private line(line: string, frames: SseFrame[]): void {
    if (line === "") {
      if (this.event !== undefined || this.data.length > 0 || this.id !== undefined) {
        frames.push({
          event: this.event,
          data: this.data.length > 0 ? this.data.join("\n") : undefined,
          id: this.id,
        });
      }
      this.event = undefined;
      this.data = [];
      this.id = undefined;
      this.size = 0;
      return;
    }
    if (line.charCodeAt(0) === 0x3a) return;
    const colon = line.indexOf(":");
    const field = colon === -1 ? line : line.slice(0, colon);
    let value = colon === -1 ? "" : line.slice(colon + 1);
    if (value.charCodeAt(0) === 0x20) value = value.slice(1);
    switch (field) {
      case "event":
        this.event = value;
        break;
      case "data":
        this.data.push(value);
        break;
      case "id":
        if (!value.includes("\0")) this.id = value;
        break;
      default:
        break;
    }
    this.size += line.length;
  }
}
