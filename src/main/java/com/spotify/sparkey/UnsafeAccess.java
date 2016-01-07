package com.spotify.sparkey;

import org.apache.commons.lang3.SystemUtils;

import sun.misc.Unsafe;
import sun.nio.ch.FileChannelImpl;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.annotation.Nullable;

/**
 * Encapsulates all of the unsafe operations that Sparkey performs.
 */
abstract class UnsafeAccess {

  // The platform-specific unsafe implementation
  private static final UnsafeAccess IMPLEMENTATION;

  static {
    if (Sun.probe()) {
      IMPLEMENTATION = Sun.create();
      // TODO: add other implementations here
    } else {
      IMPLEMENTATION = null;
    }
  }

  private UnsafeAccess() {
    // Prevent outside extension.
    // XXX(dflemstr): I would make this class be an interface but I don't want to encourage
    // alternative implementations of this code.
  }

  /**
   * Returns the UNIX file descriptor for an open file channel.
   *
   * @param channel The channel to inspect.
   * @return The file descriptor of the channel.
   * @throws IllegalArgumentException      If the provided channel is not backed by a file
   *                                       descriptor.
   * @throws UnsupportedOperationException If the current operating system does not support file
   *                                       descriptors.
   */
  public abstract int getFileDescriptor(FileChannel channel)
      throws IllegalArgumentException, UnsupportedOperationException;

  /**
   * Returns a read-only byte buffer that points to the specified location in memory.
   *
   * @param address The address to point to.
   * @param length  The length of the buffer; if this is greater than {@link Integer#MAX_VALUE}, it
   *                will be clamped to that value -- the parameter is a {@code long} for
   *                convenience.
   * @param reuse   If non-null, this must be a buffer previously returned by this method.  The
   *                buffer will be modified to point to the new address.
   * @return A byte buffer that points to the specified address.
   * @throws IllegalArgumentException If the specified {@code reuse} buffer did not originate from a
   *                                  call to this method.
   */
  public abstract ByteBuffer accessMemory(long address, long length, @Nullable ByteBuffer reuse)
      throws IllegalArgumentException;

  /**
   * Returns a read-only byte buffer that points to the specified location in memory.
   *
   * @param address The address to point to.
   * @param length  The length of the buffer; if this is greater than {@link Integer#MAX_VALUE}, it
   *                will be clamped to that value -- the parameter is a {@code long} for
   *                convenience.
   * @return A byte buffer that points to the specified address.
   */
  public ByteBuffer accessMemory(long address, long length) {
    return accessMemory(address, length, null);
  }

  /**
   * Returns the unsafe operations implementation most suitable for the current environment.
   *
   * @throws UnsupportedOperationException If unsafe operations are not supported in the current
   *                                       environment.
   */
  public static UnsafeAccess get() throws UnsupportedOperationException {
    if (IMPLEMENTATION == null) {
      throw new UnsupportedOperationException("Unsafe operations not supported on this platform");
    } else {
      return IMPLEMENTATION;
    }
  }

  private static boolean classExists(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private static final class Sun extends UnsafeAccess {

    // The infamous Sun unsafe
    private final Unsafe unsafe;
    // A read only direct byte buffer that points to zero bytes.  It's only useful for
    // duplicate()ing
    private final ByteBuffer bufferTemplate;
    // The offset of the "long address" field in the "java.nio.Buffer" class
    private final long bufferAddressOffset;
    // The offset of the "int capacity" field in the "java.nio.Buffer" class
    private final long bufferCapacityOffset;
    // The offset of the "java.io.FileDescriptor fd" field in the "sun.nio.ch.FileChannelImpl" class
    private final long channelFdFieldOffset;
    // The offset of the "int fd" field in the "java.io.FileDescriptor" class
    private final long descriptorFdFieldOffset;

    private Sun(
        Unsafe unsafe,
        ByteBuffer bufferTemplate,
        long bufferAddressOffset,
        long bufferCapacityOffset,
        long channelFdFieldOffset,
        long descriptorFdFieldOffset) {
      this.unsafe = unsafe;
      this.bufferTemplate = bufferTemplate;
      this.bufferAddressOffset = bufferAddressOffset;
      this.bufferCapacityOffset = bufferCapacityOffset;
      this.channelFdFieldOffset = channelFdFieldOffset;
      this.descriptorFdFieldOffset = descriptorFdFieldOffset;
    }

    public static boolean probe() {
      return classExists("sun.misc.Unsafe");
    }

    public static Sun create() {
      final Unsafe unsafe = getUnsafe();

      final ByteBuffer bufferTemplate = ByteBuffer.allocateDirect(0).asReadOnlyBuffer();

      final long bufferAddressOffset = objectFieldOffset(unsafe, Buffer.class, "address");
      final long bufferCapacityOffset = objectFieldOffset(unsafe, Buffer.class, "capacity");

      final long channelFdFieldOffset = objectFieldOffset(unsafe, FileChannelImpl.class, "fd");
      final long descriptorFdFieldOffset = objectFieldOffset(unsafe, FileDescriptor.class, "fd");

      return new Sun(unsafe, bufferTemplate, bufferAddressOffset, bufferCapacityOffset,
                     channelFdFieldOffset, descriptorFdFieldOffset);
    }

    @Override
    public int getFileDescriptor(FileChannel channel) {
      if (!SystemUtils.IS_OS_UNIX) {
        throw new UnsupportedOperationException(
            "File descriptors only exist on UNIX-like operating systems");
      }

      if (channel instanceof FileChannelImpl) {
        final Object fd = unsafe.getObject(channel, channelFdFieldOffset);
        return unsafe.getInt(fd, descriptorFdFieldOffset);
      } else {
        throw new IllegalArgumentException(
            "The specified channel is not backed by a file descriptor");
      }
    }

    @Override
    public ByteBuffer accessMemory(long address, long length, @Nullable ByteBuffer reuse) {
      final ByteBuffer result;
      if (reuse == null) {
        result = bufferTemplate.duplicate();
      } else {
        if (!(reuse.isDirect() & reuse.isReadOnly())) {
          throw new IllegalArgumentException("The specified buffer cannot be re-used");
        }

        result = reuse;
      }

      unsafe.putLong(result, bufferAddressOffset, address);
      unsafe.putInt(result, bufferCapacityOffset, (int) Math.min(Integer.MAX_VALUE, length));

      // This resets all of the other buffer pointers; note that this method does NOT touch the
      // underlying memory (which it can't anyway since it's a read-only buffer)
      result.clear();

      return result;
    }

    private static Unsafe getUnsafe() {
      final Field theUnsafe;

      try {
        theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
      } catch (NoSuchFieldException e) {
        throw new IllegalStateException("The Sun unsafe field is gone!", e);
      }

      theUnsafe.setAccessible(true);

      try {
        return (Unsafe) theUnsafe.get(null);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException(
            "Could not access the unsafe object even though it is accessible", e);
      }
    }

    private static long objectFieldOffset(Unsafe unsafe, Class<?> cls, String fieldName) {
      try {
        return unsafe.objectFieldOffset(cls.getDeclaredField(fieldName));
      } catch (NoSuchFieldException e) {
        throw new IllegalArgumentException("The specified field does not exist", e);
      }
    }
  }
}
