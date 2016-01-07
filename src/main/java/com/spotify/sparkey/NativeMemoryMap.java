package com.spotify.sparkey;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import org.apache.commons.lang3.SystemUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import javax.annotation.WillNotClose;

import static java.nio.file.StandardOpenOption.READ;

/**
 * A memory map that uses platform-specific code to read memory efficiently.
 */
abstract class NativeMemoryMap implements MemoryMap, Closeable {

  /**
   * Opens a new memory map for the file at the specified path.
   *
   * @param path The path to the file to open.
   * @return A memory map that contains the contents of the file.
   * @throws IOException If the specified path could not be opened for reading.
   */
  public static NativeMemoryMap open(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, READ)) {
      return open(channel);
    }
    // Yes, the channel is closed here, and that's fine; the memory map is independent
  }

  /**
   * Opens a new memory map for the file backing the specified file channel.
   *
   * @param channel A file channel that is backed by a (OS-level) file.
   * @return A memory map that contains the contents of the file.
   * @throws IOException If the specified channel could not be opened as a memory map.
   */
  public static NativeMemoryMap open(@WillNotClose FileChannel channel) throws IOException {
    if (PosixLibC.probe()) {
      final UnsafeAccess unsafeAccess = UnsafeAccess.get();
      final int fd = unsafeAccess.getFileDescriptor(channel);
      final long size = channel.size();

      return PosixLibC.create(unsafeAccess, fd, size);
    } else {
      throw new UnsupportedOperationException("Cannot open native memory maps on this platform");
    }
  }

  private static final class PosixLibC extends NativeMemoryMap {

    // Page can be read
    private static final int PROT_READ = 0x1;
    // Memory map is shared between processes
    private static final int MAP_SHARED = 0x01;
    // Resource temporarily unavailable
    private static final int EAGAIN = 11;
    // No memory
    private static final int ENOMEM = 12;

    // Unsafe access utilities
    private final UnsafeAccess unsafeAccess;
    // The beginning of the memory map region
    private final Pointer address;
    // The length of the memory map region
    private final NativeLong length;
    // Whether the memory region is locked into physical memory
    private final boolean mlocked;
    // Whether this memory map is closed
    private final AtomicBoolean closed;

    static {
      Native.register(PosixLibC.class, Platform.C_LIBRARY_NAME);
    }

    private PosixLibC(
        UnsafeAccess unsafeAccess,
        Pointer address,
        NativeLong length,
        boolean mlocked,
        AtomicBoolean closed) {
      this.unsafeAccess = unsafeAccess;
      this.address = address;
      this.length = length;
      this.mlocked = mlocked;
      this.closed = closed;
    }

    public static boolean probe() {
      return SystemUtils.IS_OS_UNIX;
    }

    public static PosixLibC create(UnsafeAccess unsafeAccess, int fd, long length)
        throws IOException {
      return create(unsafeAccess, fd, nativeLong(length));
    }

    private static PosixLibC create(UnsafeAccess unsafeAccess, int fd, NativeLong length)
        throws IOException {
      final Pointer address;

      if (length.longValue() > 0) {
        address = mmap(Pointer.NULL, length, PROT_READ, MAP_SHARED, fd, nativeLong(0L));

        if (Pointer.nativeValue(address) == -1) {
          throw new IOException("Could not map memory map: " + strerror());
        }
      } else {
        address = Pointer.NULL;
      }

      final AtomicBoolean closed = new AtomicBoolean();
      return new PosixLibC(unsafeAccess, address, length, tryMlock(address, length), closed);
    }

    private static boolean tryMlock(Pointer address, NativeLong length) throws IOException {
      if (Pointer.nativeValue(address) == 0) {
        return false;
      }

      final int ret = mlock(address, length);

      if (ret == -1) {
        final int errno = errno();

        switch (errno) {
          case EAGAIN:
          case ENOMEM:
            return false;
          default:
            throw new IOException("Could not lock memory map memory: " + strerror(errno));
        }
      } else {
        return true;
      }
    }

    private static NativeLong nativeLong(long length) {
      return new NativeLong(length);
    }

    private static String strerror() {
      return strerror(errno());
    }

    @Override
    public ByteBuffer read(long offset, @Nullable ByteBuffer reuse) {
      final long a = Pointer.nativeValue(address);
      final long l = length.longValue();

      if (offset > l || offset < 0) {
        throw new IndexOutOfBoundsException(
            MessageFormat.format("The offset {0} is not within the bounds [0, {1})", offset, l));
      }

      return unsafeAccess.accessMemory(a + offset, l - offset, reuse);
    }

    @Override
    public void close() throws IOException {
      if (closed.compareAndSet(false, true)) {
        if (mlocked) {
          final int ret = munlock(address, length);
          if (ret == -1) {
            throw new IOException("Could not unlock memory map memory: " + strerror());
          }
        }

        if (Pointer.nativeValue(address) != 0) {
          final int ret = munmap(address, length);
          if (ret == -1) {
            throw new IOException("Could not unmap memory map: " + strerror());
          }
        }
      }
    }

    private static native Pointer mmap(
        Pointer addr, NativeLong length, int prot, int flags, int fd, NativeLong offset);

    private static native int munmap(Pointer addr, NativeLong length);

    private static native int mlock(Pointer addr, NativeLong length);

    private static native int munlock(Pointer addr, NativeLong size);

    private static int errno() {
      return Native.getLastError();
    }

    private static native String strerror(int errnum);
  }
}
