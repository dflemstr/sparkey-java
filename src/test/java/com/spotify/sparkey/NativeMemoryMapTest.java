package com.spotify.sparkey;

import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.SPARSE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NativeMemoryMapTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testRead() throws Exception {
    final String expected = "abcdefghijk";
    final int length = expected.getBytes(UTF_8).length;

    final Path path = temporaryFolder.newFile().toPath();
    Files.write(path, expected.getBytes(UTF_8));

    final String actual;
    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      final ByteBuffer data = memoryMap.read(0);
      assertThat(data.position(), is(0));
      assertThat(data.capacity(), is(length));
      assertThat(data.remaining(), is(length));

      actual = UTF_8.decode(data).toString();
    }

    assertThat(actual, is(expected));
  }

  @Test
  public void testReadReuse() throws Exception {
    final String expected = "abcdefghijk";
    final int length = expected.getBytes(UTF_8).length;

    final Path path = temporaryFolder.newFile().toPath();
    Files.write(path, (expected + expected).getBytes(UTF_8));

    final String actual1;
    final String actual2;
    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      final ByteBuffer data1 = memoryMap.read(0);
      assertThat(data1.position(), is(0));
      assertThat(data1.capacity(), is(2 * length));
      assertThat(data1.remaining(), is(2 * length));

      actual1 = UTF_8.decode(data1).toString();

      final ByteBuffer data2 = memoryMap.read(length, data1);
      assertThat(data2.position(), is(0));
      assertThat(data2.capacity(), is(length));
      assertThat(data2.remaining(), is(length));

      actual2 = UTF_8.decode(data2).toString();
    }

    assertThat(actual1, is(expected + expected));
    assertThat(actual2, is(expected));
  }

  @Test
  public void testReadAcrossIntBoundary() throws Exception {
    final String expected = "abcdefghijk";
    final long position = Integer.MAX_VALUE - 4;

    final Path path = temporaryFolder.newFile().toPath();

    // We need to delete the file and re-create it as sparse
    Files.delete(path);

    // SPARSE lets us run this test a bit quicker maybe... We are after all creating a 2GiB file
    try (FileChannel channel = FileChannel.open(path, CREATE_NEW, WRITE, SPARSE)) {
      channel.position(position).write(UTF_8.encode(expected));
    }

    final String actual;
    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      final ByteBuffer data = memoryMap.read(position);
      actual = UTF_8.decode(data).toString();
    }

    assertThat(actual, is(expected));
  }

  @Test
  public void testReadOutisdeFile() throws Exception {
    final Path path = temporaryFolder.newFile().toPath();
    Files.write(path, ImmutableList.of("abc123"), UTF_8);

    // Read outside of file
    expectedException.expect(IndexOutOfBoundsException.class);

    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      memoryMap.read(1234);
    }
  }

  @Test
  public void testReadNegative() throws Exception {
    final Path path = temporaryFolder.newFile().toPath();
    Files.write(path, ImmutableList.of("abc123"), UTF_8);

    // Read memory before file
    expectedException.expect(IndexOutOfBoundsException.class);

    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      memoryMap.read(-52);
    }
  }

  @Test
  public void testReadOutsideEmptyFile() throws Exception {
    final Path path = temporaryFolder.newFile().toPath();

    // Read outside of empty file
    expectedException.expect(IndexOutOfBoundsException.class);

    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      memoryMap.read(1);
    }
  }

  @Test
  public void testReadEmptyFile() throws Exception {
    final Path path = temporaryFolder.newFile().toPath();

    try (NativeMemoryMap memoryMap = NativeMemoryMap.open(path)) {
      final ByteBuffer data = memoryMap.read(0);
      assertThat(data.position(), is(0));
      assertThat(data.capacity(), is(0));
      assertThat(data.remaining(), is(0));
    }
  }

  @Test
  public void testDoubleClose() throws Exception {
    final Path path = temporaryFolder.newFile().toPath();

    final NativeMemoryMap memoryMap = NativeMemoryMap.open(path);
    memoryMap.close();
    memoryMap.close();
  }
}