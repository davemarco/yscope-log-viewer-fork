import {Nullable} from "../../../typings/common";
import {
    Decoder,
    DecodeResultType,
    FilteredLogEventMap,
    LogEventCount,
} from "../../../typings/decoders";
import {LogLevelFilter} from "../../../typings/logs";
import {DataInputStream} from "../../../utils/datastream";
import {lzmaDecompress} from "../../../utils/xz";
import {
    ArchiveLogEvent,
    toMessage,
} from "./logevent";
import {
    deserializeHeaderMetadata,
    parseHeaderMetadata,
    querySegmentInfos,
    SegmentInfo,
} from "./metadata";
import {
    deserializeSegment,
    Segment,
    toLogEvents,
} from "./segment";


const SKIP_DICTIONARY_BYTES = 8;

// Parameters for the single file archive decoder constructor.
interface ClpArchiveDecoderParams {

    // Byte array containing single file archive with offset at start of segments.
    dataInputStream: DataInputStream;

    // Log type dictionary.
    logTypeDict: Uint8Array[];

    // Metadata for segments.
    segmentInfos: SegmentInfo[];

    // Byte sizes for segments.
    segmentSizes: number[];

    // Variable dictionary.
    varDict: Uint8Array[];
}

/**
 * Decompress dictionary with xz then deserialize into an array.
 *
 * @param dataInputStream Byte stream containing single file archive with
 * offset at start of dictionary.
 * @param dictionarySize Byte size of dictionary.
 * @return Array containing dictionary entries.
 */
const deserializeDictionary = async (
    dataInputStream: DataInputStream,
    dictionarySize: number
): Promise<Uint8Array[]> => {
    const dictionary: Uint8Array[] = [];

    const compressedBytes: Uint8Array =
      dataInputStream.readFully(dictionarySize);

    const decompressedBytes: ArrayBuffer = await lzmaDecompress(
        compressedBytes.slice(SKIP_DICTIONARY_BYTES)
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
};

/**
 * A decoder for CLP single file archives. Single file archives are an
 * alternate archive format where CLP compressed output is stored in a
 * single file rather than in a directory with multiple files.
 * NOTE: Current implementation does not preserve timestamp formatting.
 */
class ClpArchiveDecoder implements Decoder {
    #dataInputStream: DataInputStream;

    #filteredLogEventMap: FilteredLogEventMap = null;

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
     * @param params
     */
    constructor (params: ClpArchiveDecoderParams) {
        this.#dataInputStream = params.dataInputStream;
        this.#segmentSizes = params.segmentSizes;
        this.#segmentInfos = params.segmentInfos;
        this.#logTypeDict = params.logTypeDict;
        this.#varDict = params.varDict;
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

        const logTypeDict: Uint8Array[] = await deserializeDictionary(
            dataInputStream,
            nonSegmentSizes.logTypeDict
        );

        // Skip over file as not needed for decoding.
        dataInputStream.readFully(nonSegmentSizes.logTypeSegIndex);

        const varDict: Uint8Array[] = await deserializeDictionary(
            dataInputStream,
            nonSegmentSizes.varDict
        );

        // Skip over file as not needed for decoding.
        dataInputStream.readFully(nonSegmentSizes.varSegIndex);

        const clpArchiveDecoder: ClpArchiveDecoder = new ClpArchiveDecoder(
            {
                dataInputStream,
                logTypeDict,
                segmentInfos,
                segmentSizes,
                varDict,
            }
        );

        return clpArchiveDecoder;
    }

    getEstimatedNumEvents (): number {
        return this.#logEvents.length;
    }

    getFilteredLogEventMap (): FilteredLogEventMap {
        return this.#filteredLogEventMap;
    }

    setLogLevelFilter (logLevelFilter: LogLevelFilter): boolean {
        if (null === logLevelFilter) {
            this.#filteredLogEventMap = null;

            return true;
        }

        const filteredLogEventMap: number[] = [];
        this.#logEvents.forEach((logEvent, index) => {
            if (logLevelFilter.includes(logEvent.level)) {
                filteredLogEventMap.push(index);
            }
        });
        this.#filteredLogEventMap = filteredLogEventMap;

        return true;
    }


    async build (): Promise<LogEventCount> {
        this.#logEvents = await this.#deserializeSegments();

        return {numValidEvents: this.#logEvents.length, numInvalidEvents: 0};
    }

    // eslint-disable-next-line class-methods-use-this
    setFormatterOptions (): boolean {
        return true;
    }

    decodeRange (
        beginIdx: number,
        endIdx: number,
        useFilter: boolean
    ): Nullable<DecodeResultType[]> {
        if (useFilter && null === this.#filteredLogEventMap) {
            return null;
        }

        const length: number = (useFilter && null !== this.#filteredLogEventMap) ?
            this.#filteredLogEventMap.length :
            this.#logEvents.length;

        if (0 > beginIdx || length < endIdx) {
            return null;
        }

        const textDecoder: TextDecoder = new TextDecoder();
        const results: DecodeResultType[] = [];

        for (let i = beginIdx; i < endIdx; i++) {
            const logEventIdx: number = (useFilter && null !== this.#filteredLogEventMap) ?
                (this.#filteredLogEventMap[i] as number) :
                i;

            const logEvent : ArchiveLogEvent | undefined = this.#logEvents[logEventIdx];

            if ("undefined" === typeof logEvent) {
                throw new Error(`Log event at index ${logEventIdx} does not exist`);
            }

            const {level} = logEvent;
            const {timestamp} = logEvent;
            const message: string = toMessage(logEvent, textDecoder);
            results.push([message,
                Number(timestamp),
                level,
                logEventIdx + 1]);
        }

        return results;
    }

    /**
     * Deserialize all segments then reconstruct into logEvents.
     *
     * @return Array with combined log events from all segments.
     */
    #deserializeSegments = async (): Promise<ArchiveLogEvent[]> => {
        const logEvents: ArchiveLogEvent[] = [];
        for (let index = 0; index < this.#segmentSizes.length; index++) {
            const segmentSize: number | undefined = this.#segmentSizes[index];
            const segmentInfo: SegmentInfo | undefined = this.#segmentInfos[index];

            if (!segmentSize || !segmentInfo) {
                throw new Error("Segment metadata was not found");
            }

            const segment: Segment = await deserializeSegment(
                this.#dataInputStream,
                segmentSize,
                segmentInfo
            );

            console.log(
                `Retrieved ${segmentInfo.numMessages} messages from segment ${index}`
            );
            toLogEvents(segment, logEvents, this.#logTypeDict, this.#varDict);
        }

        return logEvents;
    };
}

export default ClpArchiveDecoder;
