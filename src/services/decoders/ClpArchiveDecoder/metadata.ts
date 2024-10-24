import {decode as msgpackDecode} from "@msgpack/msgpack";
import {lt} from "semver";
import initSqlJs from "sql.js";

import {DataInputStream} from "../../../utils/datastream";

/**
 * Array of byte sizes for each segment in archive. Index corresponds to
 * segment number.
 */
type SegmentFileSizes = number[];

/**
 * Byte sizes for non-segment archive files.
 */
interface NonSegmentFileSizes {
  metadataDb: number;
  logTypeDict: number;
  logTypeSegIndex: number;
  varDict: number;
  varSegIndex: number;
}

/**
 * Segment metadata.
 */
interface SegmentInfo {
  numMessages: number;
  numVariables: number;
}

/**
 * Minimum version of CLP single file archive currently supported. If the
 * archive header contains a lower version, an error will be thrown.
 */
const minSupportedVersion: string = "0.1.0";

/**
 * Retrieves Msgpack metadata from single file archive header and deserialize
 * into javascript object.
 *
 * @param dataInputStream Byte stream containing single file archive.
 * @return Metadata required to deserialize single file archive.
 */
const deserializeHeaderMetadata = (
    dataInputStream: DataInputStream
) => {
  // Skip over magic number, which is not currently used in decoding.
  dataInputStream.readFully(4);

  const patchVersion: number = dataInputStream.readUnsignedShort();
  const minorVersion: number = dataInputStream.readUnsignedByte();
  const majorVersion: number = dataInputStream.readUnsignedByte();
  const version: string = `${majorVersion}.${minorVersion}.${patchVersion}`;

  console.log(`CLP single archive version is ${version}`);

  // Ensure version is supported.
  if (lt(version, minSupportedVersion)) {
    throw new Error(
        `CLP single archive version ${version} is not supported.
            Minimum required version is ${minSupportedVersion}.`
    );
  }
  const msgPackSize: number = Number(dataInputStream.readUnsignedLong());

  // Skip over reserved section, which is not currently used in decoding.
  dataInputStream.readFully(6 * 8);

  const msgPack: Uint8Array = dataInputStream.readFully(msgPackSize);
  return msgpackDecode(msgPack);
};

/**
 * Parse header metadata to retrieve byte sizes of all files in the archive.
 * The sizes are needed to accurately decode individual files.
 *
 * @param headerMetadata Metadata containing archived file sizes.
 * @return Array with two elements. First element contains sizes of non-segment
 * files. Second element contains the size for each segment.
 */
const parseHeaderMetadata = (
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    headerMetadata: any
): [NonSegmentFileSizes, SegmentFileSizes] => {
  if (!headerMetadata.archive_files) {
    throw new Error("Archive file metadata not found");
  }

  // Array of files in the archive each containing a name (fileInfo.n) and an
  // offset (fileInfo.o).
  const fileInfos = headerMetadata.archive_files;

  // Create null instances to fill in afterwards.
  const nonSegmentSizes: NonSegmentFileSizes = {
    metadataDb: 0,
    logTypeDict: 0,
    logTypeSegIndex: 0,
    varDict: 0,
    varSegIndex: 0,
  };
  const segmentSizes: SegmentFileSizes = [];

  for (let i = 0; i < fileInfos.length - 1; i++) {
    const fileInfo = fileInfos[i];
    const nextFileInfo = fileInfos[i + 1];

    const name: string = fileInfo.n;

    // Calculate size of each file by comparing its offset to the next file's offset.
    const size: number = nextFileInfo.o - fileInfo.o;

    // Retrieve size from metadata and populate file size types with data.
    if (false === isSegment(name)) {
      switch (name) {
        case "metadata.db":
          nonSegmentSizes.metadataDb = size;
          break;
        case "logtype.dict":
          nonSegmentSizes.logTypeDict = size;
          break;
        case "logtype.segindex":
          nonSegmentSizes.logTypeSegIndex = size;
          break;
        case "var.dict":
          nonSegmentSizes.varDict = size;
          break;
        case "var.segindex":
          nonSegmentSizes.varSegIndex = size;
          break;
      }
    } else {
      segmentSizes.push(size);
    }
  }
  return [nonSegmentSizes, segmentSizes];
};

/**
 * Checks if the file name corresponds to a segment.
 *
 * @param name
 * @return Boolean whether is a segment
 */
const isSegment = (name: string) => {
  // Convert the string to a number.
  const num = Number(name);
  // Check exact match.
  return Number.isInteger(num) && String(num) === name;
};

/**
 * Queries CLP archive database for segment metadata. Segment metadata is required to decode
 * segments.
 *
 * @param dataInputStream Byte stream containing single file archive with
 * offset at start of database.
 * @param metadataDbSize Byte size of database.
 * @return Array containing metadata for each segment.
 */
const querySegmentInfos = async (
    dataInputStream: DataInputStream,
    metadataDbSize: number
): Promise<SegmentInfo[]> => {
  // Required to load the sqljs wasm binary asynchronously.
  const SQL: initSqlJs.SqlJsStatic = await initSqlJs({
    locateFile: (file) => `static/js/${file}`,
  });

  const dbBytes: Uint8Array = dataInputStream.readFully(metadataDbSize);

  const db = new SQL.Database(dbBytes);
  const queryResult: initSqlJs.QueryExecResult[] = db.exec(`
          SELECT num_messages, num_variables
          FROM files
      `);

  if (!queryResult[0]) {
    throw new Error("Segments not found in sql database.");
  }

  // Each row from query result corresponds to one segment. Transform query result by mapping
  // each row to a segment metadata object.
  const segmentInfos: SegmentInfo[] = queryResult[0].values.map((row) => {
    const [numMessages, numVariables] = row;

    if (typeof numMessages !== "number" || typeof numVariables !== "number") {
      throw new Error("Error retrieving data from archive database");
    }
    return {numMessages, numVariables};
  });

  return segmentInfos;
};

export {
  deserializeHeaderMetadata,
  parseHeaderMetadata,
  querySegmentInfos
};
export type {
  NonSegmentFileSizes,
  SegmentFileSizes,
  SegmentInfo
};


