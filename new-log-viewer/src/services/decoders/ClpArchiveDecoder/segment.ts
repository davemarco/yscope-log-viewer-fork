import {Nullable} from "../../../typings/common";
import {LogEventCount} from "../../../typings/decoders";
import {LOG_LEVEL, LOG_LEVEL_NAMES_LIST} from "../../../typings/logs";
import {Placeholder} from "../../../typings/placeholder";
import {DataInputStream} from "../../../utils/datastream";
import {lzmaDecompress} from "../../../utils/xz";
import * as Metadata from "./metadata";

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

interface LogEvent {
  timestamp: bigint;
  logLevel: LOG_LEVEL;
  logType: Uint8Array;
  dictVars: Uint8Array[];
  encodedVars: bigint[];
}

/**
 * Deserialize all segments, and convert segments into logEvents. All log
 * events are combined into a single array which is stored as a class field.
 *
 * @return Count of deserialized log events.
 */
const deserializeSegments = async (
    dataInputStream: DataInputStream,
    segmentSizes: Metadata.SegmentFileSizes,
    segmentInfos: Metadata.SegmentInfo[],
    logEvents: LogEvent[],
    logTypeDict: Uint8Array[],
    varDict: Uint8Array[]
): Promise<Nullable<LogEventCount>> => {
    for (let index = 0; index < segmentSizes.length; index++) {
        const size: number | undefined = segmentSizes[index];
        const segmentInfo: Metadata.SegmentInfo | undefined = segmentInfos[index];

        if (!size || !segmentInfo) {
            throw new Error("Segment metadata was not found");
        }

        const segment: Segment = await deserializeSegment(
            dataInputStream,
            size,
            segmentInfo
        );

        console.log(
            `Retrieved ${segmentInfo.numMessages} messages from segment ${index}`
        );
        segmentToLogEvents(segment, logEvents, logTypeDict, varDict);
    }
    return {
        numValidEvents: logEvents.length,
        numInvalidEvents: 0,
    };
};

/**
 * Decompress segment with xz then parse from binary into arrays for timestamps, log types and
 * variables. The segment is compressed in columnar format, so can parse all the timestamps, then
 * log types and finally variables. Number of timestamps, log types, and variables are known since
 * already queried from archive database.
 *
 * @param dataInputStream Byte stream containing single file archive.
 * @param segmentSize Byte size of segment.
 * @param segmentInfo Segment metadata.
 * @return Parsed segment data.
 */
const deserializeSegment = async (
    dataInputStream: DataInputStream,
    segmentSize: number,
    segmentInfo: Metadata.SegmentInfo
): Promise<Segment> => {
    const compressedBytes: Uint8Array = dataInputStream.readFully(segmentSize);
    const decompressedBytes =
    await lzmaDecompress(compressedBytes);
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
 * Converts deserialized segment into log events and adds new log events to combined array for
 * all segments.
 *
 * @param segment Deserialized segment.
 */
const segmentToLogEvents = (segment: Segment, logEvents: LogEvent[], logTypeDict: Uint8Array[], varDict: Uint8Array[]) => {
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
        const logType: Uint8Array | undefined = logTypeDict[logTypeIdx];
        if (!logType) {
            throw new Error("Log type does not exist");
        }

        const [dictVars, encodedVars] = getLogEventVariables(
            logType,
            variablesIterator,
            varDict
        );

        const logLevel: LOG_LEVEL = getLogLevel(logType);

        const logEvent: LogEvent = {
            timestamp: timestamp,
            logLevel: logLevel,
            logType: logType,
            dictVars: dictVars,
            encodedVars: encodedVars,
        };

        logEvents.push(logEvent);
    }
};

/**
 * Retrieves dictionary and encoded variables for a specific log message. Traverses log type
 * until a placeholder variable is found. For each variable found, a value is popped from the
 * segment variables array. If the variable is an encoded variable, the encoded value is the
 * value from the segment. If the variable is a dictionary variable, the segment value is used
 * to index into the archive's variable dictionary, and the lookup value is the dictionary
 * variable. Dictionary and encoded variables are distinguished by different byte placeholders.
 * the function returns two arrays, one for each dictionary variables, and the other for encoded
 * variables.
 *
 * @param logType Log with placeholders for variables.
 * @param segmentVarIterator Iterator for segment variables.
 * @return Two arrays, the first for dictionary variables and the second for encoded variables.
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
            case Placeholder.Integer || Placeholder.Float:
                const encodedVar: bigint = segmentVarIterator.next().value;
                encodedVars.push(encodedVar);
                break;

            case Placeholder.Dictionary:
                const index: number = Number(segmentVarIterator.next().value);
                if (typeof varDict[index] === "undefined") {
                    throw new Error("Log type does not exist");
                }
                const dictVar: Uint8Array = varDict[index];
                dictVars.push(dictVar);
                break;
        }
    });
    return [dictVars, encodedVars];
};

/**
 * Gets log level from log type.
 *
 * @param logType Log with placeholders for variables.
 * @return The log level.
 */
const getLogLevel = (logType: Uint8Array): LOG_LEVEL => {
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
};

export {
    deserializeSegments
};


