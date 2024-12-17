import { describe, it, expect } from "bun:test";
import { readVarInt } from "../app/utils";

describe("readVarInt", () => {
  it("should correctly decode a single-byte varint", () => {
    const buffer = new ArrayBuffer(10);
    const dataView = new DataView(buffer);

    // Example varint encoding
    // Let's encode the number 1 (0x01) and 127 (0x7F) as a varint
    dataView.setUint8(0, 0x01); // 0000 0001
    dataView.setUint8(1, 0x7F); // 0111 1111
    
    const [val1, offset1] = readVarInt(dataView, 0);
    expect(val1).toBe(1);
    expect(offset1).toBe(1);

    const [val2, offset2] = readVarInt(dataView, 1);
    expect(val2).toBe(127);
    expect(offset2).toBe(2);
  });

  it("should correctly decode a 2-byte varint", () => {
    const buffer = new ArrayBuffer(10);
    const dataView = new DataView(buffer);

    // Let's encode the number 128 as a varint
    // 128 in binary: 1000_0000
    // Varint encoding: 1000_0001 0000_0000
    dataView.setUint8(0, 0x81); // 1000 0001
    dataView.setUint8(1, 0x00); // 0000 0000

    const [value, nextOffset] = readVarInt(dataView, 0);

    expect(value).toBe(128);
    expect(nextOffset).toBe(2);
  });
});

it("should correctly decode a 3-byte varint", () => {
  const buffer = new ArrayBuffer(10);
  const dataView = new DataView(buffer);

  // Let's encode the number 123456 as a varint
  // 123456 in binary: 0001_1110_0010_0100_0000
  // Varint encoding: 1000_0111 1100_0100 0100_0000
  dataView.setUint8(0, 0x87); // 1000_0111
  dataView.setUint8(1, 0xC4); // 1100_0100
  dataView.setUint8(2, 0x40); // 0100_0000

  const [value, nextOffset] = readVarInt(dataView, 0);

  expect(value).toBe(123456);
  expect(nextOffset).toBe(3);
})