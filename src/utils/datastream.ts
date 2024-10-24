const SHORT_BYTES = 2;
const INT_BYTES = 4;
const LONG_BYTES = 8;

/**
 * Decodes primitive types from a byte stream (similar to Java's DataInputStream class).
 */
class DataInputStream {
  #dataView: DataView;
  #isLittleEndian: boolean;
  #byteIx: number;

  /**
   * Constructor
   * @param arrayBuffer Underlying array buffer
   * @param isLittleEndian Byte endianness
   */
  constructor(arrayBuffer: ArrayBufferLike, isLittleEndian: boolean = false) {
    this.#dataView = new DataView(arrayBuffer);
    this.#isLittleEndian = isLittleEndian;
    this.#byteIx = 0;
  }

  /**
   * Seeks to the given index.
   * @param idx
   */
  seek(idx: number): void {
    if (idx > this.#dataView.byteLength) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, idx);
    }
    this.#byteIx = idx;
  }

  /**
   * @return The position of the read head in the stream.
   */
  getPos(): number {
    return this.#byteIx;
  }

  /**
   * Reads the specified amount of data.
   * @param length
   * @return The data read
   */
  readFully(length: number): Uint8Array {
    const requiredLen: number = this.#byteIx + length;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }

    const val: Uint8Array = new Uint8Array(this.#dataView.buffer, this.#byteIx, length);
    this.#byteIx += length;
    return val;
  }

  /**
   * Reads an unsigned byte.
   * @return The read byte
   */
  readUnsignedByte(): number {
    const requiredLen: number = this.#byteIx + 1;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    return this.#dataView.getUint8(this.#byteIx++);
  }

  /**
   * Reads a signed byte
   * @return The read byte
   */
  readSignedByte(): number {
    const requiredLen: number = this.#byteIx + 1;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    return this.#dataView.getInt8(this.#byteIx++);
  }

  /**
   * Reads an unsigned short
   * @return The read short
   */
  readUnsignedShort(): number {
    const requiredLen: number = this.#byteIx + 2;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    const val: number = this.#dataView.getUint16(this.#byteIx, this.#isLittleEndian);
    this.#byteIx += SHORT_BYTES;
    return val;
  }

  /**
   * Reads a signed short
   * @return The read short
   */
  readSignedShort(): number {
    const requiredLen: number = this.#byteIx + 2;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    const val: number = this.#dataView.getInt16(this.#byteIx, this.#isLittleEndian);
    this.#byteIx += SHORT_BYTES;
    return val;
  }

  /**
   * Reads an int.
   * @return The read int
   */
  readInt(): number {
    const requiredLen: number = this.#byteIx + 4;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    const val: number = this.#dataView.getInt32(this.#byteIx, this.#isLittleEndian);
    this.#byteIx += INT_BYTES;
    return val;
  }

  /**
   * Reads a signed long int (64 bit).
   * @return The read signed long int
   */
  readSignedLong(): bigint {
    const requiredLen = this.#byteIx + 8;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    const val: bigint = this.#dataView.getBigInt64(this.#byteIx, this.#isLittleEndian);
    this.#byteIx += LONG_BYTES;
    return val;
  }

  /**
   * Reads an unsigned long int (64 bit).
   * @return The read unsigned long int
   */
  readUnsignedLong(): bigint {
    const requiredLen = this.#byteIx + 8;
    if (this.#dataView.byteLength < requiredLen) {
      this.#byteIx = this.#dataView.byteLength;
      throw new DataInputStreamEOFError(this.#dataView.byteLength, requiredLen);
    }
    const val: bigint = this.#dataView.getBigUint64(this.#byteIx, this.#isLittleEndian);
    this.#byteIx += LONG_BYTES;
    return val;
  }
}

/**
 * EOF error thrown by DataInputStream
 */
class DataInputStreamEOFError extends Error {
  bufLen: number;
  requiredLen: number;

  /**
   * Constructor
   * @param bufLen The length of the buffer when the error occurred
   * @param requiredLen The required length of the buffer when the error occurred
   * @param message
   */
  constructor(bufLen: number, requiredLen: number, message: string = "") {
    let formattedMessage = `[bufLen=${bufLen}, requiredLen=${requiredLen}]`;
    if (message !== "") {
      formattedMessage += ` ${message}`;
    }
    super(formattedMessage);
    this.name = "DataInputStreamEOFError";
    this.bufLen = bufLen;
    this.requiredLen = requiredLen;
  }
}

export { DataInputStream, DataInputStreamEOFError };
