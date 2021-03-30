package com.taobao.timetunnel.swap;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ByteBufferFreezer}
 * 
 * @author <a href=mailto:jushi@taobao.com>jushi</a>
 * @created 2010-12-9
 * 
 */
final class ByteBufferFreezer implements Freezer<ByteBuffer> {

  ByteBufferFreezer(final File home, final int chunkCapacity, final int chunkBuffer) {
    this.home = home;
    this.chunkCapacity = chunkCapacity;
    this.chunkBuffer = chunkBuffer;
    chunk = newChunk();
    if (!home.exists()) home.mkdirs();
  }

  @Override
  public synchronized void dispose() {
    chunk.fix();
  }

  @Override
  public synchronized Point<ByteBuffer> freeze(final ByteBuffer buffer) {
    if (!chunk.hasRemainingFor(buffer.remaining())) {
      chunk.fix();
      chunk = newChunk();
    }
    return chunk.freeze(buffer);
  }

  private Chunk newChunk() {
    return new Chunk(new File(home, seq() + ""), chunkCapacity, chunkBuffer);
  }

  private long seq() {
    return (seq == Long.MAX_VALUE) ? (seq = 0) : seq++;
  }

  private final int chunkCapacity;
  private final int chunkBuffer;
  private final File home;

  private Chunk chunk;
  private long seq = 0;

  /**
   * {@link Chunk} .
   * 
   */
  private static final class Chunk {

    Chunk(final File path, final int capacity, final int buffer) {
      this.path = path;
      this.capacity = capacity;
      this.buffer = ByteBuffer.allocate(buffer);
      LOGGER.info("{} created.", this);
    }

    public synchronized void fix() {
      if (fixed) return;
      fixed = true;
      flush();
    }

    public synchronized Point<ByteBuffer> freeze(final ByteBuffer byteBuffer) {
      if (fixed) throw new IllegalStateException("Can freeze buffer to a fixed chunk");

      final int length = byteBuffer.remaining();
      if (bufferRemainingLessThan(length)) {
        flush();
        buffer.clear();
      }

      buffer.putInt(length);

      final ByteBufferPoint bufferPoint = new ByteBufferPoint(buffer, buffer.position(), length);

      buffer.put(byteBuffer);

      final ReplaceablePoint point = new ReplaceablePoint(bufferPoint, position, length);
      pendings.add(point);
      forward(length);
      return point;
    }

    public synchronized boolean hasRemainingFor(final int size) {
      if (fixed) throw new IllegalStateException("Can't freeze buffer to a fixed chunk");
      return position + size + DATA_SIZE_LENGTH <= capacity;
    }

    @Override
    public String toString() {
      final StringBuilder builder = new StringBuilder();
      builder.append("Chunk [buffer=")
             .append(buffer)
             .append(", capacity=")
             .append(capacity)
             .append(", fixed=")
             .append(fixed)
             .append(", path=")
             .append(path)
             .append(", pendings=")
             .append(pendings)
             .append(", position=")
             .append(position)
             .append("]");
      return builder.toString();
    }

    private boolean bufferRemainingLessThan(final int length) {
      return buffer.position() + length + DATA_SIZE_LENGTH > buffer.capacity();
    }

    private void doFlush() throws FileNotFoundException, IOException {
      final FileOutputStream fos = new FileOutputStream(path, true);
      try {
        final FileChannel channel = fos.getChannel();
        channel.write((ByteBuffer) buffer.flip());
        channel.close();
      } finally {
        fos.close();
      }
    }

    private void flush() {
      if (pendings.isEmpty()) return;

      try {
        doFlush();
        replacePendings();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    private void forward(final int length) {
      position += length + DATA_SIZE_LENGTH;
    }

    private void replacePendings() throws IOException {
      Slicer slicer = null;
      while (!pendings.isEmpty()) {
        if (fileBufferReader == null) fileBufferReader = new FileBufferReader(path);
        if (slicer == null) slicer = new Slicer(fileBufferReader, pendings.size(), fixed);
        final ReplaceablePoint point = pendings.remove();
        point.replace(slicer.slice(point.position, point.length));
      }
    }

    private boolean fixed;
    private int position;
    private FileBufferReader fileBufferReader;

    private static final int DATA_SIZE_LENGTH = 4;
    private static final Logger LOGGER = LoggerFactory.getLogger(Chunk.class);

    private final File path;
    private final ByteBuffer buffer;
    private final int capacity;
    private final Queue<ReplaceablePoint> pendings = new LinkedList<ReplaceablePoint>();

    /**
     * {@link ByteBufferPoint}
     */
    private static final class ByteBufferPoint implements Point<ByteBuffer> {

      public ByteBufferPoint(final ByteBuffer byteBuffer, final int position, final int length) {
        this.position = position;
        this.length = length;
        this.byteBuffer = byteBuffer;
      }

      @Override
      public void clear() {
        byteBuffer = null;
      }

      @Override
      public ByteBuffer get() {
        return (ByteBuffer) byteBuffer.duplicate().position(position).limit(position + length);
      }

      private final int position;
      private final int length;
      private volatile ByteBuffer byteBuffer;

    }

    /**
     * {@link FileBufferReader}
     */
    private static final class FileBufferReader {

      public FileBufferReader(final File file) throws FileNotFoundException {
        this.file = file;
        final FileInputStream fis = new FileInputStream(file);
        channel = fis.getChannel();
      }

      public void closeAndDeleteFile() {
        try {
          channel.close();
        } catch (final Exception e) {
          LOGGER.error("Can't close file channel of  " + file, e);
        }
        if (file.delete()) LOGGER.info("{} deleted", file);
        else LOGGER.warn("{} delete failed", file);
      }

      public void read(final ByteBuffer byteBuffer, final long position) {
        try {
          channel.read(byteBuffer, position);
        } catch (final Exception e) {
          LOGGER.error("Can't read from " + file, e);
        }
      }

      @Override
      public String toString() {
        return file.toString();
      }

      private final File file;
      private final FileChannel channel;
    }

    /**
     * {@link ReplaceablePoint}
     */
    private final static class ReplaceablePoint implements Point<ByteBuffer> {

      public ReplaceablePoint(final Point<ByteBuffer> delegatee,
                              final int position,
                              final int length) {
        this.position = position;
        this.length = length;
        reference = new AtomicReference<Point<ByteBuffer>>(delegatee);
      }

      @Override
      public final void clear() {
        reference.getAndSet(null).clear();
      }

      @Override
      public final ByteBuffer get() {
        return reference.get().get();
      }

      public final void replace(final Point<ByteBuffer> delegatee) {
        final Point<ByteBuffer> pre = reference.getAndSet(delegatee);
        if (pre == null) delegatee.clear(); // point to cache has already
                                            // cleared.
      }

      private final int position;
      private final int length;
      private final AtomicReference<Point<ByteBuffer>> reference;

    }

    /**
     * {@link Slicer}
     */
    private final static class Slicer {

      public Slicer(final FileBufferReader fileBufferReader, final int size, final boolean last) throws IOException {
        this.fileBufferReader = fileBufferReader;
        this.last = last;
        referenceCount = new AtomicInteger(size);
      }

      public Point<ByteBuffer> slice(final int position, final int length) {
        return new Point<ByteBuffer>() {

          @Override
          public void clear() {
            if (last && referenceCount.decrementAndGet() == 0)
              fileBufferReader.closeAndDeleteFile();
          }

          @Override
          public ByteBuffer get() {
            final ByteBuffer byteBuffer = ByteBuffer.allocate(length);
            fileBufferReader.read(byteBuffer, position + DATA_SIZE_LENGTH);
            return (ByteBuffer) byteBuffer.flip();
          }
        };
      }

      private final boolean last;
      private final AtomicInteger referenceCount;
      private final FileBufferReader fileBufferReader;
    }
  }

}
