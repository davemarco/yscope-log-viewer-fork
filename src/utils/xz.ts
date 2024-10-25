// @ts-expect-error Type missing from js library.
import {XzReadableStream} from "xzwasm";


/**
 * Decompress LZMA byte array using xz wasm.
 *
 * @param dataArray LZMA compressed byte array.
 * @return Decompressed buffer.
 */
const lzmaDecompress = async (dataArray: Uint8Array): Promise<ArrayBuffer> => {
    const stream = new ReadableStream<Uint8Array>({
        /**
         * Wrapper to create a readable stream from the byte array
         *
         * @param controller
         */
        start (controller: ReadableStreamDefaultController<Uint8Array>) {
            // Enqueue all the data into the stream.
            controller.enqueue(dataArray);

            // Terminate the stream.
            controller.close();
        },
    });


    // Library is not typed.
    // eslint-disable-next-line
    const xzStream = new XzReadableStream(stream);
    const decompressedResponse: Response = new Response(xzStream as ReadableStream);

    const arrayBuffer: ArrayBuffer = await decompressedResponse.arrayBuffer();
    return arrayBuffer;
};

export {lzmaDecompress};
