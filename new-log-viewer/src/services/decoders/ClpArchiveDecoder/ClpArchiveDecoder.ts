import {decode as msgpackDecode} from "@msgpack/msgpack";
import dayjs from "dayjs";
import bigIntSupport from "dayjs/plugin/bigIntSupport";
import {lt} from "semver";
import initSqlJs from "sql.js";
// @ts-expect-error type missing from js library
import {XzReadableStream} from "xzwasm";

import {Nullable} from "../../../typings/common";
import {
    Decoder,
    DecodeResultType,
    JsonlDecoderOptionsType,
    LOG_EVENT_FILE_END_IDX,
    LogEventCount
} from "../../../typings/decoders";
import {LOG_LEVEL, LOG_LEVEL_NAMES_LIST} from "../../../typings/logs";
import {DataInputStream} from "../../../utils/datastream";
import "./metadata";


dayjs.extend(bigIntSupport);

/**
 * List of byte sizes for each segment in archive. Index corresponds to
 * segment number.
 */
type SegmentFileSizes = number[];

/**
 * Byte sizes for non-segment archive files.
 */
interface NonSegmentFileSizes {
  metadataDB: number;
  logTypeDict: number;
  logTypeSegIndex: number;
  varDict: number;
  varSegIndex: number;
}

/**
 * Metadata for segment.
 */
interface SegmentInfo {
  numMessages: number;
  numVariables: number;
}

/**
 * Parsed segment data.
 * - Timestamps are duration from unix epoch.
 * - Log types are indices into archive log type dictionary.
 * - Variables are indices into archive variable dictionary or encoded value.
 */
interface Segment {
  timestamps: bigint[];
  logTypes: bigint[];
  variables: bigint[];
}

/**
 * CLP log type placeholders.
 */
class Placeholder {
    static readonly Integer: number = 0x11;
    static readonly Dictionary: number = 0x12;
    static readonly Float: number = 0x13;
    // TODO: escape functionality not implemented.
    static readonly Escape: string = "\\";
}

interface LogEvent {
  timestamp: bigint;
  logLevel: LOG_LEVEL;
  logType: Uint8Array;
  dictVars: Uint8Array[];
  encodedVars: bigint[];
}

/**
 * Minimum version of CLP single file archive supported by this decoder.
 * If the archive header contains a lower version, an error will be thrown.
 */
const minSupportedVersion: string = "0.1.0";

/**
 * A decoder for CLP single file archives. Single file archives are an
 * alternate archive format where CLP compressed output is stored in a
 * single file rather than in a directory with multiple files.
 * Note: Current implementation does not preserve timestamp formatting,
 * i.e. it is meant to be used for archives compressed from IR.
 */
class ClpArchiveDecoder implements Decoder {
    #dataInputStream: DataInputStream;
    #segmentSizes: number[];
    #segmentInfos: SegmentInfo[];
    #logTypeDict: Uint8Array[];
    #varDict: Uint8Array[];
    #logEvents: LogEvent[] = [];

    /**
   * Private constructor for ClpArchiveDecoder. This is not intended to be
   * invoked publicly. Instead, use ClpArchiveDecoder.create() to create a
   * new instance of the class.
   *
   * @param dataInputStream Byte stream containing segment data (metadata
   * already parsed).
   * @param segmentSizes Segments byte sizes.
   * @param segmentInfos Segment metadata.
   * @param logTypeDict Log type dictionary.
   * @param varDict Variable dictionary.
   */
    constructor (
        dataInputStream: DataInputStream,
        segmentSizes: number[],
        segmentInfos: SegmentInfo[],
        logTypeDict: Uint8Array[],
        varDict: Uint8Array[]
    ) {
        this.#dataInputStream = dataInputStream;
        this.#segmentSizes = segmentSizes;
        this.#segmentInfos = segmentInfos;
        this.#logTypeDict = logTypeDict;
        this.#varDict = varDict;
    }

    /**
   * Creates a new ClpArchiveDecoder. Single file archive metadata as well as
   * CLP archive metadata is parsed in this method. In addition, the log type
   * dictionary and the variable dictionary are decoded. The returned decoder
   * is ready to parse segment data.
   *
   * @param dataArray Byte array containing single file archive. When this
   * method is finished, the position of the data array will be the start of
   * segment data.
   * @return A Promise that resolves to the created ClpArchiveDecoder instance.
   */
    static async create (dataArray: Uint8Array): Promise<ClpArchiveDecoder> {
        const dataInputStream: DataInputStream = new DataInputStream(
            dataArray.buffer,
            true
        );

        const singleFileArchiveMetadata =
      ClpArchiveDecoder.#getSingleFileArchiveMetadata(dataInputStream);

        const [nonSegmentSizes, segmentSizes] =
      ClpArchiveDecoder.#parseSingleFileArchiveMetadata(
          singleFileArchiveMetadata
      );

        const segmentInfos: SegmentInfo[] =
      await ClpArchiveDecoder.#getSegmentInfos(
          dataInputStream,
          nonSegmentSizes.metadataDB
      );

        const logTypeDict: Uint8Array[] = await ClpArchiveDecoder.#parseDictionary(
            dataInputStream,
            nonSegmentSizes.logTypeDict
        );

        // Skip over file as not needed for decoding.
        dataInputStream.readFully(nonSegmentSizes.logTypeSegIndex);

        const varDict: Uint8Array[] = await ClpArchiveDecoder.#parseDictionary(
            dataInputStream,
            nonSegmentSizes.varDict
        );

        // Skip over file as not needed for decoding.
        dataInputStream.readFully(nonSegmentSizes.varSegIndex);

        const clpArchiveDecoder: ClpArchiveDecoder = new ClpArchiveDecoder(
            dataInputStream,
            segmentSizes,
            segmentInfos,
            logTypeDict,
            varDict
        );

        return clpArchiveDecoder;
    }

    getEstimatedNumEvents (): number {
        return this.#logEvents.length;
    }

    async buildIdx (
        beginIdx: number,
        endIdx: number
    ): Promise<Nullable<LogEventCount>> {
        if (0 !== beginIdx || endIdx !== LOG_EVENT_FILE_END_IDX) {
            throw new Error("Partial range deserialization is not yet supported.");
        }

        return await this.#deserializeSegments();
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    // TODO: To be removed as log level filtering change removes this function.
    setDecoderOptions (options: JsonlDecoderOptionsType): boolean {
        return true;
    }

    decode (beginIdx: number, endIdx: number): Nullable<DecodeResultType[]> {
        if (0 > beginIdx || this.#logEvents.length < endIdx) {
            return null;
        }

        const textDecoder: TextDecoder = new TextDecoder();
        const results: DecodeResultType[] = [];

        for (let logEventIdx = beginIdx; logEventIdx < endIdx; logEventIdx++) {
            const logEvent: LogEvent = this.#logEvents[logEventIdx] as LogEvent;
            const message: string = this.#messageFromLogEvent(logEvent, textDecoder);

            const logLevel: LOG_LEVEL = logEvent.logLevel;
            const timestamp: bigint = logEvent.timestamp;

            results.push([message, Number(timestamp), logLevel, logEventIdx + 1]);
        }

        return results;
    }

    /**
   * Parses single archive preamble from data input stream and retrieves
   * metadata. The metadata is parsed by parseSingleFileArchiveMetadata.
   *
   * @param dataInputStream Byte stream containing single file archive.
   * @return Metadata to decode single file archive format.
   */
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    static #getSingleFileArchiveMetadata (dataInputStream: DataInputStream): any {
    // Skip over magic number, which is not currently used in decoding.
        dataInputStream.readFully(4);

        const patchVersion: number = dataInputStream.readUnsignedShort();
        const minorVersion: number = dataInputStream.readUnsignedByte();
        const majorVersion: number = dataInputStream.readUnsignedByte();
        const version: string = `${majorVersion}.${minorVersion}.${patchVersion}`;

        console.log(`CLP single archive version is ${version}`);

        // Ensure version is supported.
        if (lt(version, minSupportedVersion)) {
            throw new Error(
                `CLP single archive version ${version} is not supported. Minimum required version is ${minSupportedVersion}.`
            );
        }

        const msgPackSize: number = Number(dataInputStream.readUnsignedLong());

        // Skip over reserved section, which is not currently used in decoding.
        dataInputStream.readFully(6 * 8);

        const msgPack: Uint8Array = dataInputStream.readFully(msgPackSize);
        return msgpackDecode(msgPack);
    }

    /**
   * Parses single file archive metadata and retrieves byte sizes of all files
   * in the archive. The sizes are needed to accurately decode individual files.
   *
   * @param singleFileArchiveMetadata Metadata containing archived file sizes.
   * @return Array with two elements. The first element is an object containing sizes of non-segment files. The second element is an array containing the size for each segment.
   */
    static #parseSingleFileArchiveMetadata (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
        singleFileArchiveMetadata: any
    ): [NonSegmentFileSizes, SegmentFileSizes] {
        if (!singleFileArchiveMetadata.archive_files) {
            throw new Error("Archive file metadata not found");
        }

        // List of files in the archive each containing a name (fileInfo.n) and an offset (fileInfo.o).
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const fileInfos: any[] = singleFileArchiveMetadata.archive_files;

        const nonSegmentSizes: NonSegmentFileSizes = {
            metadataDB: 0,
            logTypeDict: 0,
            logTypeSegIndex: 0,
            varDict: 0,
            varSegIndex: 0,
        };
        const segmentSizes: SegmentFileSizes = [];

        for (let i = 0; i < fileInfos.length - 1; i++) {
            const fileInfo = fileInfos[i];
            const nextFileInfo = fileInfos[i + 1];

            const name: string = fileInfo.n;

            // Calculate size of each file by comparing its offset to the next file's offset.
            const size: number = nextFileInfo.o - fileInfo.o;

            // Retrieve size from metadata and populate file size types with data.
            if (false === this.#isSegment(name)) {
                switch (name) {
                    case "metadata.db":
                        nonSegmentSizes.metadataDB = size;
                        break;
                    case "logtype.dict":
                        nonSegmentSizes.logTypeDict = size;
                        break;
                    case "logtype.segindex":
                        nonSegmentSizes.logTypeSegIndex = size;
                        break;
                    case "var.dict":
                        nonSegmentSizes.varDict = size;
                        break;
                    case "var.segindex":
                        nonSegmentSizes.varSegIndex = size;
                        break;
                }
            } else {
                segmentSizes.push(size);
            }
        }
        return [nonSegmentSizes, segmentSizes];
    }

    /**
   * Checks if the file name corresponds to a segment.
   *
   * @param name
   * @return Boolean whether is a segment
   */
    static #isSegment (name: string) {
    // Convert the string to a number.
        const num = Number(name);
        // Check exact match.
        return Number.isInteger(num) && String(num) === name;
    }

    /**
   * Queries CLP archive database for segment metadata. Segment metadata is required to decode
   * segments.
   *
   * @param dataInputStream Byte stream containing single file archive.
   * @param metadataDBsize Byte size of database.
   * @return Array containing metadata for each segment.
   */
    static async #getSegmentInfos (
        dataInputStream: DataInputStream,
        metadataDBsize: number
    ): Promise<SegmentInfo[]> {
    // Required to load the sqljs wasm binary asynchronously.
        const SQL: initSqlJs.SqlJsStatic = await initSqlJs({
            locateFile: (file) => `static/js/${file}`,
        });

        const bytes: Uint8Array = dataInputStream.readFully(metadataDBsize);

        const db = new SQL.Database(bytes);
        const queryResult: initSqlJs.QueryExecResult[] = db.exec(`
      SELECT num_messages, num_variables
      FROM files
  `);

        console.log(queryResult);

        if (!queryResult[0]) {
            throw new Error("Segments not found in sql database");
        }

        // Each row from query result corresponds to one segment. Transform result by mapping each row
        // to an element in SegmentInfos array. Each element contains SegmentInfo object with
        // corresponding segment metadata.
        const segmentInfos: SegmentInfo[] = queryResult[0].values.map((row) => {
            const [numMessages, numVariables] = row;

            if (typeof numMessages !== "number" || typeof numVariables !== "number") {
                throw new Error("Error retrieving data from archive database");
            }
            return {numMessages, numVariables};
        });

        return segmentInfos;
    }

    /**
   * Decompress dictionary with xz then parse from binary into an array.
   *
   * @param dataInputStream Byte stream containing single file archive.
   * @param dictionarySize Byte size of dictionary.
   * @return Array containing metadata for each segment.
   */
    static async #parseDictionary (
        dataInputStream: DataInputStream,
        dictionarySize: number
    ): Promise<Uint8Array[]> {
        const dictionary: Uint8Array[] = [];

        const compressedBytes: Uint8Array =
      dataInputStream.readFully(dictionarySize);

        const decompressedBytes: ArrayBuffer =
      await ClpArchiveDecoder.#lzmaDecompress(compressedBytes.slice(8));

        const dictStream: DataInputStream = new DataInputStream(
            decompressedBytes,
            true
        );

        const length = decompressedBytes.byteLength;

        while (dictStream.getPos() < length) {
            // skip over ID, not used in decoding.
            dictStream.readUnsignedLong();

            const payloadSize: number = Number(dictStream.readUnsignedLong());
            const payload: Uint8Array = dictStream.readFully(payloadSize);
            dictionary.push(payload);
        }
        return dictionary;
    }

    /**
   * Decompress LZMA byte array using xz wasm.
   *
   * @param dataArray LZMA compressed byte array.
   * @return Decompressed buffer.
   */
    static #lzmaDecompress = async (
        dataArray: Uint8Array
    ): Promise<ArrayBuffer> => {
    // Wrapper to create a readable stream from the byte array.
        const stream = new ReadableStream<Uint8Array>({
            start (controller: ReadableStreamDefaultController<Uint8Array>) {
                // Enqueue all the data into the stream.
                controller.enqueue(dataArray);
                // Terminate the stream.
                controller.close();
            },
        });

        const decompressedResponse: Response = new Response(
            new XzReadableStream(stream)
        );
        const arrayBuffer: ArrayBuffer = await decompressedResponse.arrayBuffer();
        return arrayBuffer;
    };

    /**
   * Deserialize all segments, and convert segments into logEvents. All log
   * events are combined into a single array which is stored as a class field.
   *
   * @return Count of deserialized log events.
   */
    async #deserializeSegments (): Promise<Nullable<LogEventCount>> {
        for (let index = 0; index < this.#segmentSizes.length; index++) {
            const size: number | undefined = this.#segmentSizes[index];
            const segmentInfo: SegmentInfo | undefined = this.#segmentInfos[index];

            if (!size || !segmentInfo) {
                throw new Error("Segment metadata was not found");
            }

            const segment: Segment = await this.#deserializeSegment(
                this.#dataInputStream,
                size,
                segmentInfo
            );

            console.log(
                `Retrieved ${segmentInfo.numMessages} messages from segment ${index}`
            );
            this.#segmentToLogEvents(segment);
        }
        return {
            numValidEvents: this.#logEvents.length,
            numInvalidEvents: 0,
        };
    }

    /**
   * Decompress segment with xz then parse from binary into arrays for timestamps, log types and variables. The segment is compressed in columnar format, so can parse all the timestamps, then log types and finally variables. Number of timestamps, log types, and variables are known since already queried from archive database.
   *
   * @param dataInputStream Byte stream containing single file archive.
   * @param segmentSize Byte size of segment.
   * @param segmentInfo Segment metadata.
   * @return Parsed segment data.
   */
    async #deserializeSegment (
        dataInputStream: DataInputStream,
        segmentSize: number,
        segmentInfo: SegmentInfo
    ): Promise<Segment> {
        const compressedBytes: Uint8Array = dataInputStream.readFully(segmentSize);
        const decompressedBytes =
      await ClpArchiveDecoder.#lzmaDecompress(compressedBytes);
        const segmentStream: DataInputStream = new DataInputStream(
            decompressedBytes,
            true
        );

        const segment: Segment = {
            timestamps: [],
            logTypes: [],
            variables: [],
        };

        // Parse data from columnar format.
        for (let i = 0; i < segmentInfo.numMessages; i++) {
            segment.timestamps.push(segmentStream.readUnsignedLong());
        }
        for (let i = 0; i < segmentInfo.numMessages; i++) {
            segment.logTypes.push(segmentStream.readUnsignedLong());
        }
        for (let i = 0; i < segmentInfo.numVariables; i++) {
            segment.variables.push(segmentStream.readUnsignedLong());
        }

        return segment;
    }

    /**
   * Converts deserialized segment into log events and adds new log events to combined array for
   * all segments.
   *
   * @param segment Deserialized segment.
   */
    #segmentToLogEvents (segment: Segment) {
    // Iterator over segment variables. Segment variables are either an index (for dictionary
    // variables) or an encoded value.
        const variablesIterator: Iterator<bigint> =
      segment.variables[Symbol.iterator]();

        const numMessages: number = segment.timestamps.length;
        for (let i = 0; i < numMessages; i++) {
            const timestamp: bigint | undefined = segment.timestamps[i];
            if (!timestamp) {
                throw new Error("Timestamp does not exist");
            }

            const logTypeIdx: number = Number(segment.logTypes[i]);
            const logType: Uint8Array | undefined = this.#logTypeDict[logTypeIdx];
            if (!logType) {
                throw new Error("Log type does not exist");
            }

            const [dictVars, encodedVars] = this.#getLogEventVariables(
                logType,
                variablesIterator
            );

            const logLevel: LOG_LEVEL = this.#getLogLevel(logType);

            const logEvent: LogEvent = {
                timestamp: timestamp,
                logLevel: logLevel,
                logType: logType,
                dictVars: dictVars,
                encodedVars: encodedVars,
            };

            this.#logEvents.push(logEvent);
        }
    }

    /**
   * Retrieves dictionary and encoded variables for a specific log message. Traverses log type until a placeholder variable is found. For each variable found, a value is popped from the segment variables array. If the variable is an encoded variable, the encoded value is the value from the segment. If the variable is a dictionary variable, the segment value is used to index into the archive's variable dictionary, and the lookup value is the dictionary variable. Dictionary and encoded variables are distinguished by different byte placeholders. The function returns two arrays, one for each dictionary variables, and the other for encoded variables.
   *
   * @param logType Log with placeholders for variables.
   * @param segmentVarIterator Iterator for segment variables.
   * @return Two arrays, the first for dictionary variables and the second for encoded variables.
   */
    #getLogEventVariables (
        logType: Uint8Array,
        segmentVarIterator: Iterator<bigint>
    ): [Uint8Array[], bigint[]] {
        const dictVars: Uint8Array[] = [];
        const encodedVars: bigint[] = [];

        logType.forEach((logTypeCharByte) => {
            switch (logTypeCharByte) {
                case Placeholder.Integer || Placeholder.Float:
                    const encodedVar: bigint = segmentVarIterator.next().value;
                    encodedVars.push(encodedVar);
                    break;

                case Placeholder.Dictionary:
                    const index: number = Number(segmentVarIterator.next().value);
                    if (typeof this.#varDict[index] === "undefined") {
                        throw new Error("Log type does not exist");
                    }
                    const dictVar: Uint8Array = this.#varDict[index];
                    dictVars.push(dictVar);
                    break;
            }
        });
        return [dictVars, encodedVars];
    }

    /**
   * Gets log level from log type.
   *
   * @param logType Log with placeholders for variables.
   * @return The log level.
   */
    #getLogLevel (logType: Uint8Array): LOG_LEVEL {
        const textDecoder: TextDecoder = new TextDecoder();
        const message = textDecoder.decode(logType);

        // Default log level value.
        let logLevel: LOG_LEVEL = LOG_LEVEL.NONE;

        // Offset from start of logType to beginning of log level. This is normally a single space.
        // Note log type should not include the timestamp.
        const LogLevelPositionInMessages: number = 1;

        const messageLevelPart: string = message.substring(
            LogLevelPositionInMessages
        );

        const ValidLogLevelsBeginIdx: number = 1;

        // Excluded NONE as a valid log level.
        const validNames: string[] = LOG_LEVEL_NAMES_LIST.slice(
            ValidLogLevelsBeginIdx
        );

        const logLevelFound: string | undefined = validNames.find((level: string) =>
            messageLevelPart.startsWith(level)
        );

        if (logLevelFound) {
            logLevel = LOG_LEVEL_NAMES_LIST.indexOf(logLevelFound);
        }

        return logLevel;
    }

    /**
   * Decodes a message from a logEvent. Iterates through the log type bytes and replaces placeholders
   * with actual values. Formats the timestamp and and prepends to message.
   *
   * @param logEvent
   * @param textDecoder
   * @return The message as a string
   */
    #messageFromLogEvent (logEvent: LogEvent, textDecoder: TextDecoder): string {
        let message: string = "";
        const timestamp: bigint = logEvent.timestamp;
        let integer: bigint;
        let float: Uint8Array;
        let dictVar: Uint8Array;

        const encodedVarsIterator: Iterator<bigint> =
      logEvent.encodedVars[Symbol.iterator]();
        const dictVarsIterator: Iterator<Uint8Array> =
      logEvent.dictVars[Symbol.iterator]();

        logEvent.logType.forEach((logTypeCharByte) => {
            switch (logTypeCharByte) {
                case Placeholder.Integer:
                    integer = encodedVarsIterator.next().value;
                    message += integer.toString();
                    break;
                case Placeholder.Float:
                    float = this.#decodeClpFloat(encodedVarsIterator.next().value);
                    message += textDecoder.decode(float);
                    break;
                case Placeholder.Dictionary:
                    dictVar = dictVarsIterator.next().value;
                    message += textDecoder.decode(dictVar);
                    break;
                default:
                    // For each on logType returns a number and not a Uint8Array. Thus using
                    // fromCharCode instead of textDecoder.
                    message += String.fromCharCode(logTypeCharByte);
            }
        });

        const formattedTimestamp: string =
      timestamp === BigInt(0) ? "" : dayjs(timestamp).format();

        // No space is needed since the logType should already start with space.
        return formattedTimestamp + message;
    }

    /**
   * Decodes CLP float into byte array. See CLP EncodedVariableInterpreter.hpp for
   * more information.
   *
   * @param encodedVar
   * @return Decoded float as a byte array
   */
    #decodeClpFloat = (encodedVar: bigint): Uint8Array => {
    // Mask: (1 << 54) - 1
        const EIGHT_BYTE_ENCODED_FLOAT_DIGITS_BIT_MASK = (1n << 54n) - 1n;
        const HYPHEN_CHAR_CODE = "-".charCodeAt(0);
        const PERIOD_CHAR_CODE = ".".charCodeAt(0);
        const ZERO_CHAR_CODE = "0".charCodeAt(0);

        let encodedFloat: bigint = encodedVar;

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
}

export default ClpArchiveDecoder;
