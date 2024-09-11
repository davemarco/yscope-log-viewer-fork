import { Nullable } from "../../typings/common";
import {
  Decoder,
  DecodeResultType,
  JsonlDecoderOptionsType,
  LogEventCount,
} from "../../typings/decoders";
import { decode } from "@msgpack/msgpack";
import utc from "dayjs/plugin/utc";
// @ts-ignore:
import { XzReadableStream } from "xzwasm";
import Long from "long";

import initSqlJs from "sql.js";

import { DataInputStream, DataInputStreamEOFError } from "../../utils/datastream";
import dayjs from "dayjs";

dayjs.extend(utc);

type ByteArray = Uint8Array;

// Define the ArchiveFile type
interface ArchiveFile {
  fileName: string;
  offset: number;
  size: number;
  bytes?: Uint8Array | null; // Make bytes optional or allow null
}

// Initialize the array with correct type
const archiveFiles: ArchiveFile[] = [
  { fileName: "file1.txt", offset: 0, size: 100 },
  { fileName: "file2.txt", offset: 100, size: 200 },
];

// Define types for the function parameters
type Timestamps = number[];
type LogTypes = number[];
type Variables = any[]; // Replace `any` with the actual type if known

interface LogTypeDict {
  [key: number]: number[];
}

/**
 * A decoder for JSONL (JSON lines) files that contain log events. See `JsonlDecodeOptionsType` for
 * properties that are specific to log events (compared to generic JSON records).
 */
class ClpDecoder implements Decoder {
  #dataInputStream: DataInputStream;
  #metadata: any | null;
  #numMessages: number | null;
  #numVariables: number | null;
  #segmentTimestampsPosition: number | null;
  #segmentLogtypesPosition: number | null;
  #segmentVariablesPosition: number | null;

  #logTypeDict: any | null;
  #varDict: any | null;
  #segmentFiles: any[];
  #segments: any[];
  #decompressedLogs: any[];

  static #enc = new TextEncoder();
  static #dec = new TextDecoder();

  constructor(dataArray: Uint8Array, decoderOptions: JsonlDecoderOptionsType) {
    this.#dataInputStream = new DataInputStream(dataArray.buffer, true);
    this.#metadata = null;

    this.#numMessages = null;
    this.#numVariables = null;
    this.#segmentTimestampsPosition = null;
    this.#segmentLogtypesPosition = null;
    this.#segmentVariablesPosition = null;

    this.#logTypeDict = null;
    this.#varDict = null;
    this.#segmentFiles = [];
    this.#segments = [];

    this.#decompressedLogs = [];
    this.testdecode();
  }

  async testdecode() {
    this.#parseHeader();
    await this.#populateFiles();
    await this.#unpackSegments();
    await this.#decodeSegments();
  }

  getEstimatedNumEvents(): number {
    return 0;
  }

  buildIdx(beginIdx: number, endIdx: number): Nullable<LogEventCount> {
    return null;
  }

  setDecoderOptions(options: JsonlDecoderOptionsType): boolean {
    return true;
  }

  decode(beginIdx: number, endIdx: number): Nullable<DecodeResultType[]> {
    return null;
  }

  #parseHeader(): void {
    console.log("I parsed header");
    const magicNumber: Uint8Array = this.#dataInputStream.readFully(4);
    console.log(magicNumber);
    const patchVersion: number = this.#dataInputStream.readUnsignedShort();
    const minorVersion: number = this.#dataInputStream.readUnsignedByte();
    console.log(`Hello, ${minorVersion}!`);
    const majorVersion: number = this.#dataInputStream.readUnsignedByte();
    console.log(`Hello, ${majorVersion}!`);
    const metaSectionSize: number = Number(
      this.#dataInputStream.readUnsignedLong(),
    );
    const reserved: Uint8Array = this.#dataInputStream.readFully(6 * 8);
    console.log(reserved);

    const metaSection: Uint8Array =
      this.#dataInputStream.readFully(metaSectionSize);
    this.#metadata = decode(metaSection);
    console.log(this.#metadata);
  }

  // Reads and processes various files from the archive.
  // Decompresses and unpacks dictionaries and metadata.
  #populateFiles = async (): Promise<void> => {
    console.log("I populated files");
    const fileOffsets = this.#metadata.archive_files;
    const archiveFiles: ArchiveFile[] = [];

    for (let i = 0; i < fileOffsets.length - 1; i++) {
      const fileInfo = fileOffsets[i];
      const nextFileInfo = fileOffsets[1 + i];
      archiveFiles.push({
        fileName: fileInfo.n,
        offset: fileInfo.o,
        size: nextFileInfo.o - fileInfo.o,
        bytes: null,
      });
    }

    let segmentsAvailable = false;

    for (let i = 0; i < archiveFiles.length; i++) {
      const f = archiveFiles[i];
      if (f) {
        const { fileName } = f; // Destructure fileName
        f.bytes = this.#dataInputStream.readFully(f.size);
        console.log(fileName)
        console.log(f.size)

        if ("logtype.dict" === fileName) {
          this.#logTypeDict = this.#unpackDictionary(
            await this.#lzmaDecompress(f.bytes.slice(8)),
          );
        } else if ("var.dict" === fileName) {
          this.#varDict = this.#unpackDictionary(
            await this.#lzmaDecompress(f.bytes.slice(8)),
          );
        } else if ("metadata.db" === fileName) {
          await this.__readMetaDataDb(f.bytes); // Note: This method must also be synchronous
        } else if ("var.segindex" === fileName) {
          segmentsAvailable = true;
        } else if (segmentsAvailable) {
          this.#segmentFiles.push(f);
        } else {
          console.error("Item is undefined at index:", i);
          console.error("Filename is :", fileName);
        }
      }
    }
    console.log("finsihed populating files");
  };

  #unpackDictionary = (byteArray: Uint8Array): Uint8Array[] => {
    const dataInputStream = new DataInputStream(byteArray.buffer, true);
    const dictionary: Uint8Array[] = [];

    while (true) {
      try {
        const id = dataInputStream.readUnsignedLong();
        const payloadSize = Number(dataInputStream.readUnsignedLong());
        const payload = dataInputStream.readFully(payloadSize);

        dictionary.push(payload);
        //console.log("this is payload");
        //console.log(payload);
      } catch (e) {
        if (!(e instanceof DataInputStreamEOFError)) {
          throw e;
        }

        console.log(`Read ${dictionary.length} variables`);
        break;
      }
    }

    return dictionary;
  };

  #lzmaDecompress = async (byteArray: ByteArray): Promise<Uint8Array> => {
    // Create a readable stream from the byte array
    const stream = new ReadableStream<Uint8Array>({
      /**
       * Start method for the readable stream
       * @param controller - The stream controller used to enqueue data and close the stream
       */
      start(controller: ReadableStreamDefaultController<Uint8Array>) {
        controller.enqueue(byteArray);
        controller.close();
      },
    });

    const decompressedResponse = new Response(new XzReadableStream(stream));
    const arrayBuffer = await decompressedResponse.arrayBuffer();
    return new Uint8Array(arrayBuffer);
  };

  /**
   * Reads and executes an SQL query on a database file within the archive
   * to populate various properties like number of messages, segment positions, etc.
   *
   * @param fileBytes - The bytes of the database file to read.
   */
  async __readMetaDataDb(fileBytes: Uint8Array): Promise<void> {
    try {
      const SQL = await initSqlJs({
        locateFile: (file: string) => `static/js/${file}`,
      });

      const db = new SQL.Database(fileBytes);
      const queryResult = db.exec(`
                SELECT num_messages,
                       num_variables,
                       segment_timestamps_position,
                       segment_logtypes_position,
                       segment_variables_position
                FROM files
            `)[0];

      const queryResult2 = db.exec(`
                SELECT *
                FROM files
            `)[0];

      if (!queryResult) {
        console.error("Query returned no results");
        return;
      }

      console.log(queryResult);
      console.log(queryResult2);

      // Iterate over the columns and populate class properties
      for (let i = 0; i < queryResult.columns.length; i++) {
        const columnName = queryResult.columns[i];
        const value = queryResult.values[0][i];

        switch (columnName) {
          case "num_messages":
            this.#numMessages = value;
            break;
          case "num_variables":
            this.#numVariables = value;
            break;
          case "segment_timestamps_position":
            this.#segmentTimestampsPosition = value;
            break;
          case "segment_logtypes_position":
            this.#segmentLogtypesPosition;
            break;
          case "segment_variables_position":
            this.#segmentVariablesPosition;
            break;
          default:
            console.error(`Unexpected column name: ${columnName}`);
        }
        console.log(`Read ${this.#numMessages} messages`);
      }
    } catch (error) {
      console.error("Error reading metadata database:", error);
    }
  }

  // Decode a single segment into a readable log format
  #decodeOneSegment(
    timestamps: Timestamps,
    logTypes: LogTypes,
    variables: Variables,
  ): void {
    const variablesIterator = variables[Symbol.iterator]();

    console.log(`Did i get here`);

    if (this.#numMessages === null || this.#numMessages === undefined) {
      console.error("Number of messages is not defined");
      return;
    }

    console.log(variables);

    for (let i = 0; i < Number(this.#numMessages); i++) {
      const timestamp = Number(timestamps[i]);
      const logType = logTypes[i];
      const logMessageBytes: number[] = [];

      if (logType === null || logType === undefined) {
        console.error("Number of messages is not defined");
        return;
      }

      // Ensure _logTypeDict is defined for logType
      const logTypeChars = this.#logTypeDict[logType] || [];
      logTypeChars.forEach((logTypeCharByte: number) => {
        if ([0x11, 0x12, 0x13].includes(logTypeCharByte)) {
          const variable = variablesIterator.next().value;
          const bytes = this.#getBytesForVariable(logTypeCharByte, variable);

          logMessageBytes.push(...bytes);
        } else {
          logMessageBytes.push(logTypeCharByte);
        }
      });

      const timezoneElement = (globalThis as any).timezone;

      const formattedTimestamp =
        timestamp === 0
          ? ""
          : `${(timezoneElement === "UTC"
              ? dayjs.utc(timestamp)
              : dayjs(timestamp)
            ).format()} `;
      const formattedMessage = ClpDecoder.#dec
        .decode(new Uint8Array(logMessageBytes))
        .trim();

      this.#decompressedLogs.push(`${formattedTimestamp}${formattedMessage}`);
      console.log(`${formattedTimestamp}${formattedMessage}`);
    } // for (let i = 0; i < this._numMessages; i++)
  }

  // Unpacks all segment files
  async #unpackSegments(): Promise<void> {
    // Use type assertion to ensure the correct type
    const unpackPromises = this.#segmentFiles.map((file) =>
      this.#unpackOneSegment(file.bytes),
    );

    // Wait for all promises to complete
    await Promise.all(unpackPromises);
  }

  // Get bytes for a variable based on logTypeCharByte
  #getBytesForVariable(logTypeCharByte: number, variable: any): any {
    const variable2: number = Number(variable);
    console.log(logTypeCharByte);
    switch (logTypeCharByte) {
      //case 0x11:
      //  return ClpDecoder.#enc.encode(variable);
      case 0x11:
        /*

        if (typeof variable === "string" && this.#varDict[variable]) {
          return this.#varDict[variable];
        }
        console.error(`Variable ${variable} not found in _varDict`);
        return new Uint8Array();
        ```
        */
        console.log(variable2);
        console.log(this.#varDict[variable]);
        return ClpDecoder.#enc.encode(variable);
      //return new Uint8Array();
      //return variable2;
      //return variable2.toString();

      //return this.#varDict[variable];
      //return new Uint8Array();
      case 0x12:
        /*
        if (typeof variable === "string" && this.#varDict[variable]) {
          return this.#varDict[variable];
        }
        console.error(`Variable ${variable} not found in _varDict`);
        return new Uint8Array();
        */
        //return this.#varDict[variable];
        console.log(variable2);
        console.log(this.#varDict[variable2]);
        return this.#varDict[variable];
      case 0x13:
        return this.#decodeClpFloat(variable);
      default:
        console.error(`Unexpected logTypeCharByte: ${logTypeCharByte}`);
        return new Uint8Array();
    }
  }

  #decodeClpFloat = (encodedVar: bigint): Uint8Array => {
    // Mask: (1 << 54) - 1
    const EIGHT_BYTE_ENCODED_FLOAT_DIGITS_BIT_MASK = (1n << 54n) - 1n;
    const HYPHEN_CHAR_CODE = "-".charCodeAt(0);
    const PERIOD_CHAR_CODE = ".".charCodeAt(0);
    const ZERO_CHAR_CODE = "0".charCodeAt(0);

    let encodedFloat = encodedVar;

    // Decode according to the format
    const decimalPos = Number(encodedFloat & BigInt(0x0f)) + 1;
    encodedFloat >>= BigInt(4);
    const numDigits = Number(encodedFloat & BigInt(0x0f)) + 1;
    encodedFloat >>= BigInt(4);
    let digits = encodedFloat & EIGHT_BYTE_ENCODED_FLOAT_DIGITS_BIT_MASK;
    encodedFloat >>= BigInt(55);
    const isNegative = encodedFloat > 0n;

    const valueLength = numDigits + 1 + (isNegative ? 1 : 0);
    const value = new Uint8Array(valueLength);
    let pos = valueLength - 1;
    const decimalIdx = valueLength - 1 - decimalPos;

    for (let i = 0; i < numDigits; i++, --pos) {
      if (decimalIdx === pos) {
        --pos;
      }
      value[pos] = ZERO_CHAR_CODE + Number(digits % 10n);
      digits /= 10n;
    }
    value[decimalIdx] = PERIOD_CHAR_CODE;

    if (isNegative) {
      value[0] = HYPHEN_CHAR_CODE;
    }

    return value;
  };

  // Decodes a single segment into a readable log format
  // Formats timestamps and decodes log messages
  async #unpackOneSegment(segmentBytes: ByteArray): Promise<void> {
    // Decompress the segment bytes
    const decompressedBytes = await this.#lzmaDecompress(segmentBytes);

    // Create a DataInputStream from the decompressed bytes
    const dataInputStream = new DataInputStream(decompressedBytes.buffer, true);

    // Initialize arrays to hold timestamps, log types, and variables
    const timestamps: BigInt[] = [];
    const logTypes: BigInt[] = [];
    const variables: BigInt[] = [];

    console.log(`I have this for messages ${this.#numMessages}`);

    if (this.#numMessages === null || this.#numMessages === undefined) {
      console.error("Number of messages is not defined");
      return;
    }

    if (this.#numVariables === null || this.#numVariables === undefined) {
      console.error("Number of messages is not defined");
      return;
    }

    // Read timestamps
    for (let i = 0; i < this.#numMessages; i++) {
      timestamps.push(dataInputStream.readUnsignedLong());
    }

    // Read log types
    for (let i = 0; i < this.#numMessages; i++) {
      logTypes.push(dataInputStream.readUnsignedLong());
    }

    // Read variables
    for (let i = 0; i < this.#numVariables; i++) {
      variables.push(dataInputStream.readUnsignedLong());
    }

    // Push the segment data into _segments
    this.#segments.push({
      timestamps,
      logTypes,
      variables,
    });
  }

  async #decodeSegments(): Promise<void> {
    console.log("calling decode segments");
    for (const s of this.#segments) {
      this.#decodeOneSegment(s.timestamps, s.logTypes, s.variables);
    }
  }
}

export default ClpDecoder;
