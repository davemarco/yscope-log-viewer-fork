// @ts-expect-error Type missing from js library.
import {XzReadableStream} from "xzwasm";

/**
 * Decompress LZMA byte array using xz wasm.
 *
 * @param dataArray LZMA compressed byte array.
 * @return Decompressed buffer.
 */
const lzmaDecompress = async (dataArray: Uint8Array): Promise<ArrayBuffer> => {
    // Wrapper to create a readable stream from the byte array.
    const stream = new ReadableStream<Uint8Array>({
        start (controller: ReadableStreamDefaultController<Uint8Array>) {
            // Enqueue all the data into the stream.
            controller.enqueue(dataArray);
            // Terminate the stream.
            controller.close();
        },
    });

    const decompressedResponse: Response = new Response(
        new XzReadableStream(stream)
    );
    const arrayBuffer: ArrayBuffer = await decompressedResponse.arrayBuffer();
    return arrayBuffer;
};

export {lzmaDecompress};
