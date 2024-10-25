import dayjs from "dayjs";
import bigIntSupport from "dayjs/plugin/bigIntSupport";

import {LOG_LEVEL} from "../../../typings/logs";
import {
    DICTIONARY_PLACEHOLDER,
    FLOAT_PLACEHOLDER,
    INTEGER_PLACEHOLDER,
} from "../../../typings/placeholder";


/* eslint-disable import/no-named-as-default-member */
dayjs.extend(bigIntSupport);
/* eslint-enable import/no-named-as-default-member */

/* eslint-disable no-magic-numbers */
const EIGHT_BYTE_ENCODED_FLOAT_DIGITS_BIT_MASK = (1n << 54n) - 1n;
const HYPHEN_CHAR_CODE = "-".charCodeAt(0);
const PERIOD_CHAR_CODE = ".".charCodeAt(0);
const ZERO_CHAR_CODE = "0".charCodeAt(0);
/* eslint-enable no-magic-numbers */

/**
 * IR-like log event retrieved from CLP archive.
 */
interface ArchiveLogEvent {
  timestamp: bigint;
  level: LOG_LEVEL;
  logType: Uint8Array;
  dictVars: Uint8Array[];
  encodedVars: bigint[];
}

/* eslint-disable no-magic-numbers */
/**
 * Decodes CLP float into byte array. See CLP EncodedVariableInterpreter.hpp for
 * more information.
 *
 * @param encodedFloat
 * @return Decoded float as a byte array
 */
const decodeClpFloat = (encodedFloat: bigint): Uint8Array => {
    // Decode according to the format
    const decimalPos = Number(encodedFloat & BigInt(0x0f)) + 1;
    encodedFloat >>= BigInt(4);
    const numDigits = Number(encodedFloat & BigInt(0x0f)) + 1;
    encodedFloat >>= BigInt(4);
    let digits = encodedFloat & EIGHT_BYTE_ENCODED_FLOAT_DIGITS_BIT_MASK;
    encodedFloat >>= BigInt(55);
    const isNegative = 0n < encodedFloat;

    const valueLength = numDigits + 1 + (isNegative ?
        1 :
        0);
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
/* eslint-enable no-magic-numbers */


/**
 * Creates a message from a logEvent. Iterates through the log type bytes and
 * replaces placeholders with actual values. Formats the timestamp and prepends to message.
 *
 * @param logEvent
 * @param textDecoder
 * @return The message as a string
 */
const toMessage = (
    logEvent: ArchiveLogEvent,
    textDecoder: TextDecoder
): string => {
    let message: string = "";
    const {timestamp} = logEvent;

    const encodedVarsIterator: Iterator<bigint> =
    logEvent.encodedVars[Symbol.iterator]();
    const dictVarsIterator: Iterator<Uint8Array> =
    logEvent.dictVars[Symbol.iterator]();

    let integer: bigint;
    let float: Uint8Array;
    let dictVar: Uint8Array;

    logEvent.logType.forEach((logTypeCharByte) => {
        switch (logTypeCharByte) {
            case INTEGER_PLACEHOLDER: {
                const result = encodedVarsIterator.next();
                if (result.done) {
                    throw new Error("Attempted out-of-bounds access for encoded variable");
                }
                integer = result.value;
                message += integer.toString();
                break;
            }
            case FLOAT_PLACEHOLDER: {
                const result = encodedVarsIterator.next();
                if (result.done) {
                    throw new Error("Attempted out-of-bounds access for encoded variable");
                }
                float = decodeClpFloat(result.value);
                message += textDecoder.decode(float);
                break;
            }
            case DICTIONARY_PLACEHOLDER: {
                const result = dictVarsIterator.next();
                if (result.done) {
                    throw new Error("Attempted out-of-bounds access for dictionary variable");
                }

                dictVar = result.value;
                message += textDecoder.decode(dictVar);
                break;
            }
            default:
                // For each on logType returns a number and not a Uint8Array. Thus using
                // fromCharCode instead of textDecoder.
                message += String.fromCharCode(logTypeCharByte);
                break;
        }
    });

    const formattedTimestamp: string =
    timestamp === BigInt(0) ?
        "" :
        dayjs(timestamp).format();

    // No space is needed since the logType should already start with space.
    return formattedTimestamp + message;
};


export {toMessage};
export type {ArchiveLogEvent};
