import {Nullable} from "../../../typings/common";
import {
  Decoder,
  DecodeResultType,
  JsonlDecoderOptionsType,
  LOG_EVENT_FILE_END_IDX,
  LogEventCount
} from "../../../typings/decoders";
import {LOG_LEVEL} from "../../../typings/logs";
import {DataInputStream} from "../../../utils/datastream";
import {lzmaDecompress} from "../../../utils/xz";
import {ArchiveLogEvent, toMessage} from "./logevent";
import {
  deserializeHeaderMetadata,
  parseHeaderMetadata,
  querySegmentInfos,
  SegmentInfo
} from "./metadata";
import {deserializeSegments} from "./segment";

/**
 * A decoder for CLP single file archives. Single file archives are an
 * alternate archive format where CLP compressed output is stored in a
 * single file rather than in a directory with multiple files.
 * NOTE: Current implementation does not preserve timestamp formatting.
 */
class ClpArchiveDecoder implements Decoder {
  #dataInputStream: DataInputStream;
  #segmentSizes: number[];
  #segmentInfos: SegmentInfo[];
  #logTypeDict: Uint8Array[];
  #varDict: Uint8Array[];
  #logEvents: ArchiveLogEvent[] = [];

  /**
   * Private constructor for ClpArchiveDecoder. This is not intended to be
   * invoked publicly. Instead, use ClpArchiveDecoder.create() to create a
   * new instance of the class.
   *
   * @param dataInputStream Byte array containing single file archive with offset
   * at start of segments.
   * @param segmentSizes Byte sizes for segments.
   * @param segmentInfos Metadata for segments.
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
   * Creates a new ClpArchiveDecoder. Deserializes the single archive header
   * metadata, the CLP archive metadata, and the archive dictionaries. The
   * returned decoder state is ready to deserialize segment data.
   *
   * @param dataArray Byte array containing single file archive.
   * @return A Promise that resolves to the created ClpArchiveDecoder instance.
   */
  static async create (dataArray: Uint8Array): Promise<ClpArchiveDecoder> {
    const dataInputStream: DataInputStream = new DataInputStream(
        dataArray.buffer,
        true
    );

    const headerMetadata = deserializeHeaderMetadata(dataInputStream);

    const [nonSegmentSizes, segmentSizes] = parseHeaderMetadata(headerMetadata);

    const segmentInfos: SegmentInfo[] = await querySegmentInfos(
        dataInputStream,
        nonSegmentSizes.metadataDb
    );

    const logTypeDict: Uint8Array[] = await ClpArchiveDecoder.#deserializeDictionary(
        dataInputStream,
        nonSegmentSizes.logTypeDict
    );

    // Skip over file as not needed for decoding.
    dataInputStream.readFully(nonSegmentSizes.logTypeSegIndex);

    const varDict: Uint8Array[] = await ClpArchiveDecoder.#deserializeDictionary(
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

    this.#logEvents = await deserializeSegments(this.#dataInputStream,
        this.#segmentSizes,
        this.#segmentInfos,
        this.#logTypeDict,
        this.#varDict
    );

    return {numValidEvents: this.#logEvents.length, numInvalidEvents: 0};
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
      const logEvent : ArchiveLogEvent | undefined = this.#logEvents[logEventIdx];
      if (!logEvent) {
        throw new Error("Log event at index ${logEventIdx} does not exist");
      }

      const logLevel: LOG_LEVEL = logEvent.logLevel;
      const timestamp: bigint = logEvent.timestamp;
      const message: string = toMessage(logEvent, textDecoder);
      results.push([message, Number(timestamp), logLevel, logEventIdx + 1]);
    }

    return results;
  }

  /**
   * Decompress dictionary with xz then deserialize into an array.
   *
   * @param dataInputStream Byte stream containing single file archive with
   * offset at start of dictionary.
   * @param dictionarySize Byte size of dictionary.
   * @return Array containing dictionary entries.
   */
  static async #deserializeDictionary (
      dataInputStream: DataInputStream,
      dictionarySize: number
  ): Promise<Uint8Array[]> {
    const dictionary: Uint8Array[] = [];

    const compressedBytes: Uint8Array =
      dataInputStream.readFully(dictionarySize);

    const decompressedBytes: ArrayBuffer = await lzmaDecompress(
        compressedBytes.slice(8)
    );

    const length = decompressedBytes.byteLength;

    const dictStream: DataInputStream = new DataInputStream(
        decompressedBytes,
        true
    );

    while (dictStream.getPos() < length) {
      // Skip over ID, not used in decoding.
      dictStream.readUnsignedLong();

      const payloadSize: number = Number(dictStream.readUnsignedLong());
      const payload: Uint8Array = dictStream.readFully(payloadSize);
      dictionary.push(payload);
    }
    return dictionary;
  }
}

export default ClpArchiveDecoder;
