import {
    LOG_LEVEL,
    LOG_LEVEL_NAMES,
} from "../../../typings/logs";
import {
    DICTIONARY_PLACEHOLDER,
    FLOAT_PLACEHOLDER,
    INTEGER_PLACEHOLDER,
} from "../../../typings/placeholder";
import {DataInputStream} from "../../../utils/datastream";
import {lzmaDecompress} from "../../../utils/xz";
import {ArchiveLogEvent} from "./logevent";
import {SegmentInfo} from "./metadata";


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
 * Decompress segment with xz then deserialize into three arrays for timestamps,
 * log types and variables. The segment is serialized in columnar format, so
 * can parse the timestamps, log types and variables sequentially.
 *
 * @param dataInputStream Byte stream containing single file archive with offset
 * at start of segment.
 * @param segmentSize Byte size of segment.
 * @param segmentInfo Segment metadata.
 * @return Deserialized segment.
 */
const deserializeSegment = async (
    dataInputStream: DataInputStream,
    segmentSize: number,
    segmentInfo: SegmentInfo
): Promise<Segment> => {
    const compressedBytes: Uint8Array = dataInputStream.readFully(segmentSize);
    const decompressedBytes = await lzmaDecompress(compressedBytes);
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
};

/**
 * Retrieves dictionary and encoded variables for a specific log message.
 * The function returns two arrays; The first for dictionary variables,
 * and the second for encoded variables. Traverses log type until a variable
 * placeholder is found. For each variable placeholder found, a value is
 * popped from the segment variables array. Dictionary and encoded variables
 * are distinguished by different byte placeholders. If the variable is a
 * dictionary variable, the value is used to index into the archive's variable
 * dictionary, and the lookup result is added to dictionary array. If the variable is
 * an encoded variable, the value is simply added to encoded array.
 *
 * @param logType Log with placeholders for variables.
 * @param segmentVarIterator Iterator for segment variables.
 * @param varDict Archive variable dictionary.
 * @return Two arrays, the first for dictionary variables and the second for
 * encoded variables.
 */
const getLogEventVariables = (
    logType: Uint8Array,
    segmentVarIterator: Iterator<bigint>,
    varDict: Uint8Array[]
): [Uint8Array[], bigint[]] => {
    const dictVars: Uint8Array[] = [];
    const encodedVars: bigint[] = [];

    logType.forEach((logTypeCharByte) => {
        switch (logTypeCharByte) {
            //  Both `INTEGER_PLACEHOLDER` and `FLOAT_PLACEHOLDER will execute the same block.
            case INTEGER_PLACEHOLDER:
            case FLOAT_PLACEHOLDER: {
                const result = segmentVarIterator.next();
                if (result.done) {
                    throw new Error("Attempted out-of-bounds access for segment variable");
                }
                const encodedVar: bigint = result.value;
                encodedVars.push(encodedVar);
                break;
            }
            case DICTIONARY_PLACEHOLDER: {
                const result = segmentVarIterator.next();
                if (result.done) {
                    throw new Error("Attempted out-of-bounds access for segment variable");
                }
                const index: number = Number(result.value);
                const dictVar: Uint8Array | undefined = varDict[index];
                if ("undefined" === typeof dictVar) {
                    throw new Error(
                        `Variable at index ${index} in
                        variable dictionary does not exist`
                    );
                }
                dictVars.push(dictVar);
                break;
            }
            default:
                break;
        }
    });

    return [
        dictVars,
        encodedVars,
    ];
};

/**
 * Gets log level from log type.
 *
 * @param logType Log with placeholders for variables.
 * @return
 */
const getLogLevel = (logType: Uint8Array): LOG_LEVEL => {
    const textDecoder: TextDecoder = new TextDecoder();
    const decodedLogType: string = textDecoder.decode(logType);

    // Default log level value.
    let logLevelValue: LOG_LEVEL = LOG_LEVEL.UNKNOWN;

    // Offset from start of logType to beginning of log level. This is normally a single space.
    // Note log type should not include the timestamp.
    const logLevelPositionInLogType: number = 1;

    const logTypeSubString: string = decodedLogType.substring(
        logLevelPositionInLogType
    );

    const validLogLevelsBeginIdx: number = 1;

    // Excluded UNKNOWN as a valid log level.
    const validLevelNames: string[] = LOG_LEVEL_NAMES.slice(
        validLogLevelsBeginIdx
    );

    const logLevelNameFound: string | undefined =
        validLevelNames.find((level: string) => logTypeSubString.startsWith(level));

    if ("undefined" !== typeof logLevelNameFound) {
        logLevelValue = LOG_LEVEL_NAMES.indexOf(logLevelNameFound);
    }

    return logLevelValue;
};

/**
 * Converts segment into log events and adds new log events to combined array
 * for all segments.
 *
 * @param segment Deserialized segment.
 * @param logEvents Array with combined log events from all segments.
 * @param logTypeDict Archive log type dictionary.
 * @param varDict Archive variable dictionary.
 * @throws {Error} If out-of-bounds access on log type dictionary.
 */
const toLogEvents = (
    segment: Segment,
    logEvents: ArchiveLogEvent[],
    logTypeDict: Uint8Array[],
    varDict: Uint8Array[]
) => {
    // Iterator over segment variables. Segment variables are either an index (for dictionary
    // variables) or an encoded value.
    const variablesIterator: Iterator<bigint> =
    segment.variables[Symbol.iterator]();

    const numMessages: number = segment.timestamps.length;
    for (let i = 0; i < numMessages; i++) {
        // Explicit cast since typescript thinks `fileInfos[i]` can be undefined, but
        // it can't because of bounds check in for loop.
        const timestamp = segment.timestamps[i] as bigint;
        const logTypeIdx: number = Number(segment.logTypes[i]);
        const logType: Uint8Array | undefined = logTypeDict[logTypeIdx];
        if ("undefined" === typeof logType) {
            throw new Error(
                `Variable at index ${logTypeIdx}
                 in log type dictionary does not exist`
            );
        }

        const [dictVars, encodedVars] = getLogEventVariables(
            logType,
            variablesIterator,
            varDict
        );

        const level: LOG_LEVEL = getLogLevel(logType);

        const logEvent: ArchiveLogEvent = {
            dictVars: dictVars,
            encodedVars: encodedVars,
            level: level,
            logType: logType,
            timestamp: timestamp,
        };

        logEvents.push(logEvent);
    }
};

export {
    deserializeSegment,
    toLogEvents,
};
export type {Segment};
