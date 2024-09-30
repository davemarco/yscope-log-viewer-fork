import dayjs, {Dayjs} from "dayjs";
import utc from "dayjs/plugin/utc";

import {
    JsonObject,
    JsonValue,
} from "../../../typings/js";
import {
    INVALID_TIMESTAMP_VALUE,
    LOG_LEVEL,
} from "../../../typings/logs";


// eslint-disable-next-line import/no-named-as-default-member
dayjs.extend(utc);

/**
 * Determines whether the given value is a `JsonObject` and applies a TypeScript narrowing
 * conversion if so.
 *
 * Reference: https://www.typescriptlang.org/docs/handbook/2/narrowing.html#using-type-predicates
 *
 * @param value
 * @return A TypeScript type predicate indicating whether `value` is a `JsonObject`.
 */
const isJsonObject = (value: JsonValue): value is JsonObject => {
    return "object" === typeof value && null !== value;
};

/**
 * Converts a field into a log level if possible.
 *
 * @param field
 * @return The log level or `LOG_LEVEL.NONE` if the field couldn't be converted.
 */
const convertToLogLevelValue = (field: JsonValue | undefined): LOG_LEVEL => {
    let logLevelValue = LOG_LEVEL.NONE;

    if ("undefined" === typeof field) {
        return logLevelValue;
    }

    const logLevelName = "object" === typeof field ?
        JSON.stringify(field) :
        String(field);

    const uppercaseLogLevelName = logLevelName.toUpperCase();
    if (uppercaseLogLevelName in LOG_LEVEL) {
        logLevelValue = LOG_LEVEL[uppercaseLogLevelName as keyof typeof LOG_LEVEL];
    }

    return logLevelValue;
};

/**
 * Converts a field into a dayjs timestamp if possible.
 *
 * @param field
 * @return The field as a dayjs timestamp or `dayjs.utc(INVALID_TIMESTAMP_VALUE)` if:
 * - the timestamp key doesn't exist in the log.
 * - the timestamp's value is an unsupported type.
 * - the timestamp's value is not a valid dayjs timestamp.
 */
const convertToDayjsTimestamp = (field: JsonValue | undefined): dayjs.Dayjs => {
    // If the field is an invalid type, then set the timestamp to `INVALID_TIMESTAMP_VALUE`.
    if (("string" !== typeof field &&
        "number" !== typeof field) ||

        // dayjs surprisingly thinks `undefined` is a valid date:
        // https://day.js.org/docs/en/parse/now#docsNav
        "undefined" === typeof field
    ) {
        // `INVALID_TIMESTAMP_VALUE` is a valid dayjs date. Another potential option is
        // `dayjs(null)` to show "Invalid Date" in the UI.
        field = INVALID_TIMESTAMP_VALUE;
    }

    let dayjsTimestamp: Dayjs = dayjs.utc(field);

    // Sanitize invalid (e.g., "deadbeef") timestamps to `INVALID_TIMESTAMP_VALUE`; otherwise
    // they'll show up in UI as "Invalid Date".
    if (false === dayjsTimestamp.isValid()) {
        dayjsTimestamp = dayjs.utc(INVALID_TIMESTAMP_VALUE);
    }

    return dayjsTimestamp;
};
export {
    convertToDayjsTimestamp,
    convertToLogLevelValue,
    isJsonObject,
};
