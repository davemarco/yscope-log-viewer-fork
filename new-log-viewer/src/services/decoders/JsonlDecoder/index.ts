import {Nullable} from "../../../typings/common";
import {
    Decoder,
    DecodeResultType,
    FilteredLogEventMap,
    JsonlDecoderOptionsType,
    LogEventCount,
} from "../../../typings/decoders";
import {Formatter} from "../../../typings/formatters";
import {JsonValue} from "../../../typings/js";
import {
    INVALID_TIMESTAMP_VALUE,
    LOG_LEVEL,
    LogLevelFilter,
} from "../../../typings/logs";
import LogbackFormatter from "../../formatters/LogbackFormatter";
import {
    convertToDayjsTimestamp,
    convertToLogLevelValue,
    isJsonObject,
    JsonLogEvent,
} from "./utils";


/**
 * A decoder for JSONL (JSON lines) files that contain log events. See `JsonlDecoderOptionsType` for
 * properties that are specific to log events (compared to generic JSON records).
 */
class JsonlDecoder implements Decoder {
    static #textDecoder = new TextDecoder();

    #dataArray: Nullable<Uint8Array>;

    #logLevelKey: string;

    #timestampKey: string;

    #logEvents: JsonLogEvent[] = [];

    #filteredLogEventMap: FilteredLogEventMap = null;

    #invalidLogEventIdxToRawLine: Map<number, string> = new Map();

    #formatter: Formatter;

    /**
     * @param dataArray
     * @param decoderOptions
     */
    constructor (dataArray: Uint8Array, decoderOptions: JsonlDecoderOptionsType) {
        this.#dataArray = dataArray;
        this.#logLevelKey = decoderOptions.logLevelKey;
        this.#timestampKey = decoderOptions.timestampKey;
        this.#formatter = new LogbackFormatter({formatString: decoderOptions.formatString});
    }

    getEstimatedNumEvents (): number {
        return this.#logEvents.length;
    }

    getFilteredLogEventMap (): FilteredLogEventMap {
        return this.#filteredLogEventMap;
    }

    setLogLevelFilter (logLevelFilter: LogLevelFilter): boolean {
        this.#filterLogs(logLevelFilter);

        return true;
    }

    build (): LogEventCount {
        this.#deserialize();

        const numInvalidEvents = Array.from(this.#invalidLogEventIdxToRawLine.keys()).length;

        return {
            numValidEvents: this.#logEvents.length - numInvalidEvents,
            numInvalidEvents: numInvalidEvents,
        };
    }

    setFormatterOptions (options: JsonlDecoderOptionsType): boolean {
        this.#formatter = new LogbackFormatter({formatString: options.formatString});

        return true;
    }

    decodeRange (
        beginIdx: number,
        endIdx: number,
        useFilter: boolean,
    ): Nullable<DecodeResultType[]> {
        if (useFilter && null === this.#filteredLogEventMap) {
            return null;
        }

        // Prevents typescript potential null warning.
        const filteredLogEventIndices: number[] = this.#filteredLogEventMap as number[];

        const length: number = useFilter ?
            filteredLogEventIndices.length :
            this.#logEvents.length;

        if (0 > beginIdx || length < endIdx) {
            return null;
        }

        const results: DecodeResultType[] = [];
        for (let i = beginIdx; i < endIdx; i++) {
            // Explicit cast since typescript thinks `#filteredLogEventIndices[filteredLogEventIdx]`
            // can be undefined, but it shouldn't be since we performed a bounds check at the
            // beginning of the method.
            const logEventIdx: number = useFilter ?
                (filteredLogEventIndices[i] as number) :
                i;

            results.push(this.#getDecodeResult(logEventIdx));
        }

        return results;
    }

    /**
     * Decodes a log event into a `DecodeResultType`.
     *
     * @param logEventIdx
     * @return The decoded log event.
     */
    #getDecodeResult = (logEventIdx: number): DecodeResultType => {
        let timestamp: number;
        let message: string;
        let logLevel: LOG_LEVEL;

        // eslint-disable-next-line no-warning-comments
        // TODO We could probably optimize this to avoid checking `#invalidLogEventIdxToRawLine` on
        // every iteration.
        if (this.#invalidLogEventIdxToRawLine.has(logEventIdx)) {
            timestamp = INVALID_TIMESTAMP_VALUE;
            message = `${this.#invalidLogEventIdxToRawLine.get(logEventIdx)}\n`;
            logLevel = LOG_LEVEL.NONE;
        } else {
            // Explicit cast since typescript thinks `#logEvents[logEventIdx]` can be undefined,
            // but it shouldn't be since the index comes from a class-internal filter.
            const logEvent = this.#logEvents[logEventIdx] as JsonLogEvent;
            logLevel = logEvent.level;
            message = this.#formatter.formatLogEvent(logEvent);
            timestamp = logEvent.timestamp.valueOf();
        }

        return [
            message,
            timestamp,
            logLevel,
            logEventIdx + 1,
        ];
    };

    /**
     * Parses each line from the data array and buffers it internally.
     *
     * NOTE: `#dataArray` is freed after the very first run of this method.
     */
    #deserialize () {
        if (null === this.#dataArray) {
            return;
        }

        const text = JsonlDecoder.#textDecoder.decode(this.#dataArray);
        let beginIdx = 0;
        while (beginIdx < text.length) {
            const endIdx = text.indexOf("\n", beginIdx);
            const line = (-1 === endIdx) ?
                text.substring(beginIdx) :
                text.substring(beginIdx, endIdx);

            beginIdx = (-1 === endIdx) ?
                text.length :
                endIdx + 1;

            this.#parseJson(line);
        }

        this.#dataArray = null;
    }

    /**
     * Parses a JSON line into a log event and buffers it internally. If the line isn't valid JSON,
     * a default log event is buffered and the line is added to `#invalidLogEventIdxToRawLine`.
     *
     * @param line
     */
    #parseJson (line: string) {
        try {
            const fields = JSON.parse(line) as JsonValue;
            if (false === isJsonObject(fields)) {
                throw new Error("Unexpected non-object.");
            }
            this.#logEvents.push({
                fields: fields,
                level: convertToLogLevelValue(fields[this.#logLevelKey]),
                timestamp: convertToDayjsTimestamp(fields[this.#timestampKey]),
            });
        } catch (e) {
            if (0 === line.length) {
                return;
            }
            console.error(e, line);
            const currentLogEventIdx = this.#logEvents.length;
            this.#invalidLogEventIdxToRawLine.set(currentLogEventIdx, line);
            this.#logEvents.push({
                fields: {},
                level: LOG_LEVEL.NONE,
                timestamp: convertToDayjsTimestamp(INVALID_TIMESTAMP_VALUE),
            });
        }
    }

    /**
     * Filters log events and generates `#filteredLogEventMap`. If `logLevelFilter` is `null`,
     * `#filteredLogEventMap` will be set to `null`.
     *
     * @param logLevelFilter
     */
    #filterLogs (logLevelFilter: LogLevelFilter) {
        if (null === logLevelFilter) {
            this.#filteredLogEventMap = null;
            return;
        }

        this.#filteredLogEventMap = [];
        this.#logEvents.forEach((logEvent, index) => {
            if (logLevelFilter.includes(logEvent.level)) {
                (this.#filteredLogEventMap as number[]).push(index);
            }
        });
    }
}

export default JsonlDecoder;
