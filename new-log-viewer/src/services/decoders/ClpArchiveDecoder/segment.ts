import {LOG_LEVEL, LOG_LEVEL_NAMES_LIST} from "../../../typings/logs";
import {Placeholder} from "../../../typings/placeholder";
import {DataInputStream} from "../../../utils/datastream";
import {lzmaDecompress} from "../../../utils/xz";
import {ArchiveLogEvent} from "./logevent";
import {SegmentFileSizes, SegmentInfo} from "./metadata";

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
 * Deserialize all segments then reconstruct into logEvents.
 *
 * @param dataInputStream Byte stream containing single file archive with offset
 * at start of segments.
 * @param segmentSize Array of byte sizes for each segment in archive.
 * @param segmentInfos Segment metadata.
 * @param logTypeDict Archive log type dictionary.
 * @param varDict Archive variable dictionary.
 * @return Array with combined log events from all segments.
 */
const deserializeSegments = async (
    dataInputStream: DataInputStream,
    segmentSizes: SegmentFileSizes,
    segmentInfos: SegmentInfo[],
    logTypeDict: Uint8Array[],
    varDict: Uint8Array[]
): Promise<ArchiveLogEvent[]> => {
  const logEvents: ArchiveLogEvent[] = [];
  for (let index = 0; index < segmentSizes.length; index++) {
    const segmentSize: number | undefined = segmentSizes[index];
    const segmentInfo: SegmentInfo | undefined = segmentInfos[index];

    if (!segmentSize || !segmentInfo) {
      throw new Error("Segment metadata was not found");
    }

    const segment: Segment = await deserializeSegment(
        dataInputStream,
        segmentSize,
        segmentInfo
    );

    console.log(
        `Retrieved ${segmentInfo.numMessages} messages from segment ${index}`
    );
    toLogEvents(segment, logEvents, logTypeDict, varDict);
  }
  return logEvents;
};

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
 * Converts segment into log events and adds new log events to combined array
 * for all segments.
 *
 * @param segment Deserialized segment.
 * @param logEvents Array with combined log events from all segments.
 * @param logTypeDict Archive log type dictionary.
 * @param varDict Archive variable dictionary.
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

    const logEvent: ArchiveLogEvent = {
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
      case Placeholder.Integer || Placeholder.Float:
        const encodedVar: bigint = segmentVarIterator.next().value;
        encodedVars.push(encodedVar);
        break;

      case Placeholder.Dictionary:
        const index: number = segmentVarIterator.next().value;
        const dictVar: Uint8Array | undefined = varDict[index];
        if (!dictVar) {
          throw new Error("Variable at index ${index} does not exist");
        }
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
  const decodedLogType: string = textDecoder.decode(logType);

  // Default log level value.
  let logLevelValue: LOG_LEVEL = LOG_LEVEL.NONE;

  // Offset from start of logType to beginning of log level. This is normally a single space.
  // Note log type should not include the timestamp.
  const logLevelPositionInLogType: number = 1;

  const logTypeSubString: string = decodedLogType.substring(
      logLevelPositionInLogType
  );

  const validLogLevelsBeginIdx: number = 1;

  // Excluded NONE as a valid log level.
  const validLevelNames: string[] = LOG_LEVEL_NAMES_LIST.slice(
      validLogLevelsBeginIdx
  );

  const logLevelNameFound: string | undefined = validLevelNames.find((level: string) =>
    logTypeSubString.startsWith(level)
  );

  if (logLevelNameFound) {
    logLevelValue = LOG_LEVEL_NAMES_LIST.indexOf(logLevelNameFound);
  }

  return logLevelValue;
};

export {deserializeSegments};
