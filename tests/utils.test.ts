import { describe, test, expect } from "bun:test";
import { readVarInt, readSerialTypeCode, SerialType } from "../app/utils";

describe("readVarInt", () => {
  test("should correctly decode a single-byte varint", () => {
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

  test("should correctly decode a 2-byte varint", () => {
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
  test("should correctly decode a 3-byte varint", () => {
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
});


describe('readSerialTypeCode', () => {
  test('should correctly parse NULL type', () => {
    const result = readSerialTypeCode(0);
    expect(result).toEqual({ type: SerialType.Null, size: 0 });
  });

  test('should correctly parse 8-bit signed integer', () => {
    const result = readSerialTypeCode(1);
    expect(result).toEqual({ type: SerialType.Int8, size: 1 });
  });

  test('should correctly parse 16-bit signed integer', () => {
    const result = readSerialTypeCode(2);
    expect(result).toEqual({ type: SerialType.Int16, size: 2 });
  });

  test('should correctly parse 24-bit signed integer', () => {
    const result = readSerialTypeCode(3);
    expect(result).toEqual({ type: SerialType.Int24, size: 3 });
  });

  test('should correctly parse 32-bit signed integer', () => {
    const result = readSerialTypeCode(4);
    expect(result).toEqual({ type: SerialType.Int32, size: 4 });
  });

  test('should correctly parse 48-bit signed integer', () => {
    const result = readSerialTypeCode(5);
    expect(result).toEqual({ type: SerialType.Int48, size: 6 });
  });

  test('should correctly parse 64-bit signed integer', () => {
    const result = readSerialTypeCode(6);
    expect(result).toEqual({ type: SerialType.Int64, size: 8 });
  });

  test('should correctly parse 64-bit floating point number', () => {
    const result = readSerialTypeCode(7);
    expect(result).toEqual({ type: SerialType.Float, size: 8 });
  });

  test('should correctly parse constant 0', () => {
    const result = readSerialTypeCode(8);
    expect(result).toEqual({ type: SerialType.Const0, size: 0 });
  });

  test('should correctly parse constant 1', () => {
    const result = readSerialTypeCode(9);
    expect(result).toEqual({ type: SerialType.Const1, size: 0 });
  });

  test('should correctly parse BLOB type', () => {
    const result = readSerialTypeCode(12);
    expect(result).toEqual({ type: SerialType.BLOB, size: 0 });
  });

  test('should correctly parse String type', () => {
    const result = readSerialTypeCode(13);
    expect(result).toEqual({ type: SerialType.String, size: 0 });
  });

  test('should correctly parse larger BLOB type', () => {
    const result = readSerialTypeCode(14);
    expect(result).toEqual({ type: SerialType.BLOB, size: 1 });
  });

  test('should correctly parse larger String type', () => {
    const result = readSerialTypeCode(15);
    expect(result).toEqual({ type: SerialType.String, size: 1 });
  });

  test('should throw an error for invalid serial type code', () => {
    expect(() => readSerialTypeCode(10)).toThrow('Invalid serial type code: 10');
  });
});