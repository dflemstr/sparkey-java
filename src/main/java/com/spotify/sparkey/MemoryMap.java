package com.spotify.sparkey;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * A memory map that can offer views into a large area of memory.
 */
public interface MemoryMap {

  /**
   * Reads the data at the specified offset in the memory map.  This is an extremely cheap
   * operation.
   *
   * @param offset The offset at which data should be read.
   * @param reuse  If non-null, this must be a buffer previously returned by this method.  The
   *               buffer will be modified to contain the requested data.
   * @return A byte buffer that points to the data at the specified offset.  The capacity and limit
   * of the buffer will be set to either the end of the file, or {@link Integer#MAX_VALUE}, whatever
   * comes first.
   */
  ByteBuffer read(long offset, @Nullable ByteBuffer reuse);

  /**
   * Reads the data at the specified offset in the memory map.  This is an extremely cheap
   * operation.
   *
   * @param offset The offset at which data should be read.
   * @return A byte buffer that points to the data at the specified offset.  The capacity and limit
   * of the buffer will be set to either the end of the file, or {@link Integer#MAX_VALUE}, whatever
   * comes first.
   */
  default ByteBuffer read(long offset) {
    return read(offset, null);
  }
}
