
const textDecoder = new TextDecoder();

/**
 * Read a signed 64bit variable-length integer starting at offset.
 * TODO will fail for ints more than 53 bits
 * @param dataView DataView
 * @param offset offset to start reading from
 * @returns two numbers: the decoded varint, and byte position right after it
 */
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

export function decodeString(dataView: DataView, offset: number, size: number): string {
    return textDecoder.decode(dataView.buffer.slice(offset, offset + size));
}