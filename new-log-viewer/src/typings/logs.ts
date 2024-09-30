import {Nullable} from "./common";

import {Dayjs} from "dayjs";

import {
    JsonObject,
} from "./js";

enum LOG_LEVEL {
    NONE = 0,
    TRACE,
    DEBUG,
    INFO,
    WARN,
    ERROR,
    FATAL
}

type LogLevelFilter = Nullable<LOG_LEVEL[]>;

const INVALID_TIMESTAMP_VALUE = 0;

interface JsonLogEvent {
    timestamp: Dayjs,
    level: LOG_LEVEL,
    fields: JsonObject
}

export type {
    JsonLogEvent,
    LogLevelFilter};
export {
    INVALID_TIMESTAMP_VALUE,
    LOG_LEVEL,
};
