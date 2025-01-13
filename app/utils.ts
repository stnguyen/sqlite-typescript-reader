/**
 * Standalone functions
 */

import type { ColumnValue } from './types';

export function readVarInt(dataView: DataView, offset: number): [number, number] {
    const mask = 0x7f;
    const msb = 0x80;
    let value = 0;
    for (let i = 0; i < 8; i++) {
        const byte = dataView.getUint8(offset + i);
        if (i > 0) {
            value <<= 7;
        }
        value |= byte & mask;

        if ((byte & msb) === 0) {
            return [value, offset + i + 1]
        }
    }

    const lastByte = dataView.getUint8(offset + 8);
    value <<= 8;
    value |= lastByte;
    return [value, offset + 9];
}

const textDecoder = new TextDecoder();
export function decodeString(dataView: DataView, offset: number, size: number): string {
    return textDecoder.decode(dataView.buffer.slice(offset, offset + size));
}

export enum SerialType {
    Null = 0,
    Int8, Int16, Int24, Int32, Int48, Int64,
    Float,
    Const0, Const1,
    Internal1, Internal2,
    BLOB,
    String
}

export interface SerialTypeWithSize {
    type: SerialType,
    size: number
}

export function readSerialTypeCode(code: number): SerialTypeWithSize {
    let type: number;
    if (code < SerialType.Internal1) {
        type = code;
    } else if (code >= 12 && code % 2 == 0) {
        type = SerialType.BLOB;
    } else if (code >= 13 && code % 2 == 1) {
        type = SerialType.String;
    } else {
        throw Error(`Invalid serial type code: ${code}`);
    }

    let size: number = -1;
    switch (type) {
        case SerialType.Null:
        case SerialType.Const0:
        case SerialType.Const1:
            size = 0;
            break;

        case SerialType.Int8:
        case SerialType.Int16:
        case SerialType.Int24:
        case SerialType.Int32:
            size = type;
            break;

        case SerialType.Int48:
            size = 6;
            break;

        case SerialType.Int64:
        case SerialType.Float:
            size = 8;
            break;

        case SerialType.BLOB:
        case SerialType.String:
            size = (code - type) / 2;
            break;
    }
    return { type, size };
}

export function readColumnValue(serialTypeWithSize: SerialTypeWithSize, dataView: DataView, byteOffset: number): ColumnValue {
    switch (serialTypeWithSize.type) {
        case SerialType.Null:
            return null;
        case SerialType.Const0:
            return 0;
        case SerialType.Const1:
            return 1;
        case SerialType.Int8:
            return dataView.getInt8(byteOffset);
        case SerialType.Int16:
            return dataView.getInt16(byteOffset);
        case SerialType.Int24:
            // Read the three bytes as unsigned
            const int24 = (dataView.getUint8(byteOffset) << 16) |
                (dataView.getUint8(byteOffset + 1) << 8) |
                dataView.getUint8(byteOffset + 2);

            // Convert to signed 24-bit integer
            return (int24 & 0x800000) ? int24 | 0xFF000000 : int24;
        case SerialType.Int32:
            return dataView.getInt32(byteOffset);
        case SerialType.Float:
            return dataView.getFloat64(byteOffset);
        case SerialType.String:
            return decodeString(dataView, byteOffset, serialTypeWithSize.size);

        default:
            throw new Error(`Not implemented: reading column value of type ${serialTypeWithSize.type}`);
    }
}