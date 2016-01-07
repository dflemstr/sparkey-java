package com.spotify.sparkey;

import com.sun.jna.Native;
import com.sun.jna.NativeLong;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

public class UnsafeAccessTest {
  // If a test fails here, it probably segfaults the JVM.  Don't expect nice behavior.

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testGetFileDescriptor() throws Exception {
    final UnsafeAccess sut = UnsafeAccess.get();

    final Path path = temporaryFolder.newFile().toPath();

    final ByteBuffer buffer = ByteBuffer.allocateDirect(4);
    buffer.put("abcd".getBytes(UTF_8)).flip();

    final long written;
    try (FileChannel channel = FileChannel.open(path, WRITE)) {
      final int fd = sut.getFileDescriptor(channel);

      written = MiniLibC.write(fd, buffer);
    }

    assertThat(written, is(4L));
    assertThat(Files.readAllLines(path), contains("abcd"));
  }

  @Test
  @SuppressWarnings("PointlessArithmeticExpression")
  public void testAccessMemory() throws Exception {
    final UnsafeAccess sut = UnsafeAccess.get();

    final long address = MiniLibC.malloc(4);
    try {
      MiniLibC.memset(address + 0, 'a', 1);
      MiniLibC.memset(address + 1, 'b', 1);
      MiniLibC.memset(address + 2, 'c', 1);
      MiniLibC.memset(address + 3, 'd', 1);

      final ByteBuffer data = sut.accessMemory(address, 4);
      assertThat(data.position(), is(0));
      assertThat(data.capacity(), is(4));
      assertThat(data.remaining(), is(4));

      final byte[] bytes = new byte[4];
      data.get(bytes);
      assertThat(bytes[0], is((byte) 'a'));
      assertThat(bytes[1], is((byte) 'b'));
      assertThat(bytes[2], is((byte) 'c'));
      assertThat(bytes[3], is((byte) 'd'));
    } finally {
      MiniLibC.free(address);
    }
  }

  @Test
  @SuppressWarnings("PointlessArithmeticExpression")
  public void testAccessMemoryReuse() throws Exception {
    final UnsafeAccess sut = UnsafeAccess.get();

    final long a = MiniLibC.malloc(4);
    try {
      final long b = MiniLibC.malloc(8);
      try {
        MiniLibC.memset(a + 0, 'a', 1);
        MiniLibC.memset(a + 1, 'b', 1);
        MiniLibC.memset(a + 2, 'c', 1);
        MiniLibC.memset(a + 3, 'd', 1);

        MiniLibC.memset(b + 0, 'e', 1);
        MiniLibC.memset(b + 1, 'f', 1);
        MiniLibC.memset(b + 2, 'g', 1);
        MiniLibC.memset(b + 3, 'h', 1);
        MiniLibC.memset(b + 4, 'i', 1);
        MiniLibC.memset(b + 5, 'j', 1);
        MiniLibC.memset(b + 6, 'k', 1);

        final ByteBuffer dataA1 = sut.accessMemory(a, 4);
        assertThat(dataA1.position(), is(0));
        assertThat(dataA1.capacity(), is(4));
        assertThat(dataA1.remaining(), is(4));

        final byte[] bytesA1 = new byte[4];
        dataA1.get(bytesA1);
        assertThat(bytesA1[0], is((byte) 'a'));
        assertThat(bytesA1[1], is((byte) 'b'));
        assertThat(bytesA1[2], is((byte) 'c'));
        assertThat(bytesA1[3], is((byte) 'd'));

        final ByteBuffer dataB = sut.accessMemory(b, 7, dataA1);
        assertThat(dataB.position(), is(0));
        assertThat(dataB.capacity(), is(7));
        assertThat(dataB.remaining(), is(7));

        final byte[] bytesB = new byte[7];
        dataB.get(bytesB);
        assertThat(bytesB[0], is((byte) 'e'));
        assertThat(bytesB[1], is((byte) 'f'));
        assertThat(bytesB[2], is((byte) 'g'));
        assertThat(bytesB[3], is((byte) 'h'));
        assertThat(bytesB[4], is((byte) 'i'));
        assertThat(bytesB[5], is((byte) 'j'));
        assertThat(bytesB[6], is((byte) 'k'));

        final ByteBuffer dataA2 = sut.accessMemory(a, 4, dataB);
        assertThat(dataA2.position(), is(0));
        assertThat(dataA2.capacity(), is(4));
        assertThat(dataA2.remaining(), is(4));

        final byte[] bytesA2 = new byte[4];
        dataA2.get(bytesA2);
        assertThat(bytesA2[0], is((byte) 'a'));
        assertThat(bytesA2[1], is((byte) 'b'));
        assertThat(bytesA2[2], is((byte) 'c'));
        assertThat(bytesA2[3], is((byte) 'd'));
      } finally {
        MiniLibC.free(b);
      }
    } finally {
      MiniLibC.free(a);
    }
  }

  @Test
  public void testAccessMemoryHeapBuffer() throws Exception {
    final UnsafeAccess sut = UnsafeAccess.get();

    // A buffer pointing to managed memory cannot be reused
    expectedException.expect(IllegalArgumentException.class);

    sut.accessMemory(0, 0, ByteBuffer.allocate(4));
  }

  @Test
  public void testAccessMemoryWritableBuffer() throws Exception {
    final UnsafeAccess sut = UnsafeAccess.get();

    // A buffer that allows writing cannot be reused
    expectedException.expect(IllegalArgumentException.class);

    sut.accessMemory(0, 0, ByteBuffer.allocateDirect(4));
  }

  private static class MiniLibC {

    static {
      Native.register(MiniLibC.class, Platform.C_LIBRARY_NAME);
    }

    private static native Pointer malloc(NativeLong size);

    private static native void free(Pointer pointer);

    private static native Pointer memset(Pointer pointer, int value, NativeLong size);

    private static native NativeLong write(int fd, Pointer buf, NativeLong count);

    public static long malloc(long size) {
      return Pointer.nativeValue(malloc(new NativeLong(size)));
    }

    public static void free(long address) {
      free(new Pointer(address));
    }

    public static void memset(long address, int value, long size) {
      memset(new Pointer(address), value, new NativeLong(size));
    }

    public static long write(int fd, ByteBuffer buffer) {
      final NativeLong result =
          write(fd, Native.getDirectBufferPointer(buffer), new NativeLong(buffer.remaining()));
      return result.longValue();
    }
  }
}