import {Nullable} from "../../typings/common";
import {
    Decoder,
    DecodeResultType,
    JsonlDecoderOptionsType,
    LogEventCount,
    FULL_RANGE_END_IDX,
} from "../../typings/decoders";
import {Formatter} from "../../typings/formatters";
import {
    JsonObject,
    JsonValue,
} from "../../typings/js";
import {
    INVALID_TIMESTAMP_VALUE,
    LOG_LEVEL,
} from "../../typings/logs";
import LogbackFormatter from "../formatters/LogbackFormatter";

/**
 * Parsed JSONL.
 * TODO: Add timestamp to interface by parsing in buildIdx and remove functionality from decode.
 */
interface JsonLogEvent {
    level: LOG_LEVEL
    jsonLog: JsonObject
}

/**
 * A decoder for JSONL (JSON lines) files that contain log events. See `JsonlDecodeOptionsType` for
 * properties that are specific to log events (compared to generic JSON records).
 */
class JsonlDecoder implements Decoder {
    static #textDecoder = new TextDecoder();

    #dataArray: Uint8Array;

    #logLevelKey: string = "level";

    #logEvents: JsonLogEvent[] = [];

    #filteredLogEvents: JsonLogEvent[] = [];

    #invalidLogEventIdxToRawLine: Map<number, string> = new Map();
    #filteredInvalidLogEventIdxToRawLine: Map<number, string> = new Map();

    // @ts-expect-error #formatter is set in the constructor by `setDecoderOptions()`
    #formatter: Formatter;

    /**
     * @param dataArray
     * @param decoderOptions
     * @throws {Error} if the initial decoder options are erroneous.
     */
    constructor (dataArray: Uint8Array, decoderOptions: JsonlDecoderOptionsType) {
        const isOptionSet = this.setDecoderOptions(decoderOptions);
        if (false === isOptionSet) {
            throw new Error(
                `Initial decoder options are erroneous: ${JSON.stringify(decoderOptions)}`
            );
        }
        this.#dataArray = dataArray;
    }

    getEstimatedNumEvents (): number {
        return this.#filteredLogEvents.length;
    }

    buildIdx (beginIdx: number, endIdx: number): Nullable<LogEventCount> {

        if (beginIdx !== 0 || endIdx !== FULL_RANGE_END_IDX) {
            throw new Error("Partial range deserialization is not yet supported.");
        }
        let numInvalidEvents: number = 0

        const text = JsonlDecoder.#textDecoder.decode(this.#dataArray);

        while (beginIdx < text.length) {
            const endIdx = text.indexOf("\n", beginIdx);
            const line = (-1 === endIdx) ?
                text.substring(beginIdx) :
                text.substring(beginIdx, endIdx);

            beginIdx = (-1 === endIdx) ?
                text.length :
                endIdx + 1;

            try {
                const jsonLog = JSON.parse(line) as JsonValue;
                if ("object" !== typeof jsonLog) {
                    throw new Error("Unexpected non-object.");
                }
                let level: LOG_LEVEL = this.#parseLogLevel(jsonLog as JsonObject)
                const logEvent : JsonLogEvent = {
                    level: level,
                    jsonLog: jsonLog as JsonObject
                }
                this.#logEvents.push(logEvent);

            } catch (e) {
                if (0 === line.length) {
                    continue;
                }
                numInvalidEvents++
                console.error(e, line);
                const currentLogEventIdx = this.#logEvents.length;
                this.#invalidLogEventIdxToRawLine.set(currentLogEventIdx, line);
                const logEvent : JsonLogEvent = {
                    level: LOG_LEVEL.NONE,
                    jsonLog: {}
                }
                this.#logEvents.push(logEvent);
            }
        }
        this.#filteredLogEvents = new Array(...this.#logEvents)
        this.#filteredInvalidLogEventIdxToRawLine = new Map(this.#invalidLogEventIdxToRawLine)

        return {
            numValidEvents: endIdx - beginIdx - numInvalidEvents,
            numInvalidEvents: numInvalidEvents,
        };
    }

    updateVerbosity (verbosity: number): boolean {
        this.#filteredLogEvents = [];
        this.#filteredInvalidLogEventIdxToRawLine.clear

        console.log("is this being called")

        for (let index = 0; index < this.#logEvents.length; index++) {
            let logEvent = this.#logEvents[index] as JsonLogEvent;
            console.log(logEvent.level)
            console.log(1 << logEvent.level)
            console.log(verbosity)

            if ((1 << logEvent.level) & verbosity) {
                if (this.#invalidLogEventIdxToRawLine.has(index)) {
                    this.#filteredInvalidLogEventIdxToRawLine.set(index, this.#invalidLogEventIdxToRawLine.get(index) as string);
                }
                console.log("do i get here")

                this.#filteredLogEvents.push(logEvent)
            }
        }
        return true
    }

    setDecoderOptions (options: JsonlDecoderOptionsType): boolean {
        // If options changed then parse log events again.
        if (this.#logLevelKey != options.logLevelKey) {
            // Note this will not run if there are no events, for example on decoder initialization
            for (let logEvent of this.#logEvents) {
                let level: LOG_LEVEL = this.#parseLogLevel(logEvent.jsonLog as JsonObject)
                logEvent.level = level
            }
        }
        this.#formatter = new LogbackFormatter(options);
        this.#logLevelKey = options.logLevelKey;
        return true;
    }

    decode (beginIdx: number, endIdx: number): Nullable<DecodeResultType[]> {
        if (0 > beginIdx || this.#logEvents.length < endIdx) {
            return null;
        }

        // TODO We could probably optimize this to avoid checking `#invalidLogEventIdxToRawLine` on
        // every iteration.
        const results: DecodeResultType[] = [];
        for (let logEventIdx = beginIdx; logEventIdx < endIdx; logEventIdx++) {
            let timestamp: number;
            let message: string;
            let logLevel: LOG_LEVEL;
            if (this.#filteredInvalidLogEventIdxToRawLine.has(logEventIdx)) {
                timestamp = INVALID_TIMESTAMP_VALUE;
                message = `${this.#filteredInvalidLogEventIdxToRawLine.get(logEventIdx)}\n`;
                logLevel = LOG_LEVEL.NONE;
            } else {
                // Explicit cast since typescript thinks `#logEvents[logEventIdx]` can be undefined,
                // but it shouldn't be since we performed a bounds check at the beginning of the
                // method.
                console.log(logEventIdx)
                console.log(`This is end ${endIdx}`)
                console.log("am i here")
                const logEvent: JsonLogEvent = this.#filteredLogEvents[logEventIdx] as JsonLogEvent;
                let jsonLog: JsonObject = logEvent.jsonLog;
                (
                    {timestamp, message} = this.#formatter.formatLogEvent(jsonLog)
                );
                logLevel = logEvent.level;
            }

            results.push([
                message,
                timestamp,
                logLevel,
                logEventIdx + 1,
            ]);
        }

        return results;
    }

    /**
     * Parses the log level from the given log event.
     *
     * @param logEvent
     * @return The parsed log level.
     */
    #parseLogLevel (logEvent: JsonObject): number {
        let logLevel = LOG_LEVEL.NONE;

        const parsedLogLevel = logEvent[this.#logLevelKey];
        if ("undefined" === typeof parsedLogLevel) {
            return logLevel;
        }

        const logLevelStr = "object" === typeof parsedLogLevel ?
            JSON.stringify(parsedLogLevel) :
            String(parsedLogLevel);

        if (false === (logLevelStr.toUpperCase() in LOG_LEVEL)) {
            console.error(`${logLevelStr} doesn't match any known log level.`);
        } else {
            logLevel = LOG_LEVEL[logLevelStr as (keyof typeof LOG_LEVEL)];
        }

        return logLevel;
    }
}

export default JsonlDecoder;
