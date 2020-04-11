/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.creator.impl.inv;

import com.google.common.base.Preconditions;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.creator.InvertedIndexCreator;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.index.readers.BaseImmutableDictionary;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import static org.apache.pinot.core.segment.creator.impl.V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;


/**
 * Implementation of {@link InvertedIndexCreator} that uses off-heap memory.
 * <p>We use 2 passes to create the inverted index.
 * <ul>
 *
 *   <li>
 *     In the constructor compute the number of ranges needed. We can decide this based on the following
 *     - number of buckets based on dictionary (use the user provided config for number for buckets or number of dictIds per bucket or pick something a default)
 *   </li>
 *   <li>
 *     In the first pass (adding values phase), when add() method is called, store the dictIds into the forward index
 *     value buffer (for multi-valued column also store number of values for each docId into forward index length
 *     buffer). We also compute the inverted index length for each dictId while adding values.
 *   </li>
 *   <li>
 *     In the second pass (processing values phase), when seal() method is called, all the dictIds should already been
 *     added. We first reorder the values into the inverted index buffers by going over the dictIds in forward index
 *     value buffer (for multi-valued column we also need forward index length buffer to get the docId for each dictId).
 *     <p>Once we have the inverted index buffers, we simply go over them and create the bitmap for each dictId and
 *     serialize them into a file.
 *   </li>
 * </ul>
 * <p>Based on the number of values we need to store, we use direct memory or MMap file to allocate the buffer.
 */
public final class DictionaryBasedRangeIndexCreator implements InvertedIndexCreator {
  // Use MMapBuffer if the value buffer size is larger than 2G
  private static final int NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER = 500_000_000;
  private static final int DEFAULT_NUM_RANGES = 20;

  private static final String FORWARD_INDEX_VALUE_BUFFER_SUFFIX = ".fwd.idx.val.buf";
  private static final String FORWARD_INDEX_LENGTH_BUFFER_SUFFIX = ".fwd.idx.len.buf";
  private static final String INVERTED_INDEX_VALUE_BUFFER_SUFFIX = ".inv.idx.val.buf";
  private static final String INVERTED_INDEX_LENGTH_BUFFER_SUFFIX = ".inv.idx.len.buf";

  private final File _invertedIndexFile;
  private final File _forwardIndexValueBufferFile;
  private final File _forwardIndexLengthBufferFile;
  private final File _invertedIndexValueBufferFile;
  private final File _invertedIndexLengthBufferFile;
  private final boolean _singleValue;
  private final int _cardinality;
  private final int _numDocs;
  private final int _numValues;
  private final boolean _useMMapBuffer;

  private final int[] _values;

  // Forward index buffers (from docId to dictId)
  private int _nextDocId;
  private PinotDataBuffer _forwardIndexValueBuffer;
  // For multi-valued column only because each docId can have multiple dictIds
  private int _nextValueId;
  private PinotDataBuffer _forwardIndexLengthBuffer;

  // Inverted index buffers (from dictId to docId)
  private PinotDataBuffer _invertedIndexValueBuffer;
  private PinotDataBuffer _invertedIndexLengthBuffer;
  private int _numRanges;
  private int _numDocsPerRange;

  public DictionaryBasedRangeIndexCreator(File indexDir, FieldSpec fieldSpec, int cardinality, int numDocs,
      int numValues)
      throws IOException {
    String columnName = fieldSpec.getName();
    _invertedIndexFile = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    _forwardIndexValueBufferFile = new File(indexDir, columnName + FORWARD_INDEX_VALUE_BUFFER_SUFFIX);
    _forwardIndexLengthBufferFile = new File(indexDir, columnName + FORWARD_INDEX_LENGTH_BUFFER_SUFFIX);
    _invertedIndexValueBufferFile = new File(indexDir, columnName + INVERTED_INDEX_VALUE_BUFFER_SUFFIX);
    _invertedIndexLengthBufferFile = new File(indexDir, columnName + INVERTED_INDEX_LENGTH_BUFFER_SUFFIX);
    _singleValue = fieldSpec.isSingleValueField();
    _cardinality = cardinality;
    _numDocs = numDocs;
    _numValues = _singleValue ? numDocs : numValues;
    _useMMapBuffer = _numValues > NUM_VALUES_THRESHOLD_FOR_MMAP_BUFFER;

    try {
      _values = new int[_numValues];
      _numRanges = DEFAULT_NUM_RANGES;
      _numDocsPerRange = (int) Math.ceil(_numValues / _numRanges);
      _forwardIndexValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _forwardIndexValueBufferFile);
      if (!_singleValue) {
        _forwardIndexLengthBuffer = createTempBuffer((long) _numDocs * Integer.BYTES, _forwardIndexLengthBufferFile);
      }

      // We need to clear the inverted index length buffer because we rely on the initial value of 0, and keep updating
      // the value instead of directly setting the value
      _invertedIndexLengthBuffer =
          createTempBuffer((long) _cardinality * Integer.BYTES, _invertedIndexLengthBufferFile);
      for (int i = 0; i < _cardinality; i++) {
        _invertedIndexLengthBuffer.putInt((long) i * Integer.BYTES, 0);
      }
    } catch (Exception e) {
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      destroyBuffer(_invertedIndexLengthBuffer, _invertedIndexLengthBufferFile);
      throw e;
    }
  }

  @Override
  public void add(int dictId) {
    putInt(_forwardIndexValueBuffer, _nextDocId, dictId);
    putInt(_invertedIndexLengthBuffer, dictId, getInt(_invertedIndexLengthBuffer, dictId) + 1);
    _values[_nextDocId] = dictId;
    _nextDocId = _nextDocId + 1;
  }

  @Override
  public void add(int[] dictIds, int length) {
    for (int i = 0; i < length; i++) {
      int dictId = dictIds[i];
      putInt(_forwardIndexValueBuffer, _nextValueId, dictId);
      putInt(_invertedIndexLengthBuffer, dictId, getInt(_invertedIndexLengthBuffer, dictId) + 1);
      _values[_nextValueId] = dictId;
      _nextValueId = _nextValueId + 1;
    }
    putInt(_forwardIndexLengthBuffer, _nextDocId++, length);
  }

  @Override
  public void addDoc(Object document, int docIdCounter) {
    throw new IllegalStateException("Bitmap inverted index creator does not support Object type currently");
  }

  @Override
  public void seal()
      throws IOException {
    //copy forward index, sort the forward index, create a copy
    //go over the sorted value to compute ranges
    Integer[] positions = new Integer[_values.length];
    for (int i = 0; i < _values.length; i++) {
      positions[i] = i;
    }
    Arrays.sort(positions, Comparator.comparingInt(pos -> _values[pos]));

//    List<Integer> ranges = new ArrayList<>();
//    for (int i = 0; i < _values.length; i++) {
//      if (i % _numDocsPerRange == 0) {
//        ranges.add(_values[i]);
//      }
//    }

    List<Integer> rangeStartList = new ArrayList<>();
    int numDocsInCurrentRange = 0;
    // Calculate value index for each dictId in the inverted index value buffer
    // Re-use inverted index length buffer to store the value index for each dictId, where value index is the index in
    // the inverted index value buffer where we should put next docId for the dictId
    int invertedValueIndex = 0;
    rangeStartList.add(0);
    int prevEndDict = 0;
    for (int dictId = 0; dictId < _cardinality; dictId++) {
      int length = getInt(_invertedIndexLengthBuffer, dictId);
      putInt(_invertedIndexLengthBuffer, dictId, invertedValueIndex);
      invertedValueIndex += length;
      if (prevEndDict == dictId || (numDocsInCurrentRange + length <= _numDocsPerRange)) {
        numDocsInCurrentRange += length;
      } else {
        rangeStartList.add(dictId);
        prevEndDict = dictId;
        numDocsInCurrentRange = length;
      }
    }

    // Put values into inverted index value buffer
    _invertedIndexValueBuffer = createTempBuffer((long) _numValues * Integer.BYTES, _invertedIndexValueBufferFile);
    if (_singleValue) {
      for (int docId = 0; docId < _numDocs; docId++) {
        int dictId = getInt(_forwardIndexValueBuffer, docId);
        int index = getInt(_invertedIndexLengthBuffer, dictId);
        putInt(_invertedIndexValueBuffer, index, docId);
        putInt(_invertedIndexLengthBuffer, dictId, index + 1);
      }

      // Destroy buffer no longer needed
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      _forwardIndexValueBuffer = null;
    } else {
      int valueId = 0;
      for (int docId = 0; docId < _numDocs; docId++) {
        int length = getInt(_forwardIndexLengthBuffer, docId);
        for (int i = 0; i < length; i++) {
          int dictId = getInt(_forwardIndexValueBuffer, valueId++);
          int index = getInt(_invertedIndexLengthBuffer, dictId);
          putInt(_invertedIndexValueBuffer, index, docId);
          putInt(_invertedIndexLengthBuffer, dictId, index + 1);
        }
      }

      // Destroy buffers no longer needed
      destroyBuffer(_forwardIndexValueBuffer, _forwardIndexValueBufferFile);
      _forwardIndexValueBuffer = null;
      destroyBuffer(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile);
      _forwardIndexLengthBuffer = null;
    }

    // Create bitmaps from inverted index buffers and serialize them to file
    //HEADER
    //# OF RANGES
    //   Range Start 0
    //    .........
    //   Range Start R - 1
    //   Bitmap for Range 0 Start Offset
    //      .....
    //   Bitmap for Range R Start Offset
    //BODY
    //   Bitmap for range 0
    //   Bitmap for range 2
    //    ......
    //   Bitmap for range R - 1
    long length = 0;
    try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(_invertedIndexFile));
        DataOutputStream header = new DataOutputStream(bos);
        FileOutputStream fos = new FileOutputStream(_invertedIndexFile);
        DataOutputStream dataOutputStream = new DataOutputStream(new BufferedOutputStream(fos))) {

      //Write the Range values
      header.writeInt(rangeStartList.size());
//      System.out.println("rangeStartList = " + rangeStartList);
      for (int rangeStart : rangeStartList) {
        header.writeInt(rangeStart);
      }
      //compute the offset where the bitmap for the first range would be written
      int bitmapOffset =
          Integer.BYTES + (rangeStartList.size()) * Integer.BYTES + (rangeStartList.size() + 1) * Integer.BYTES;
      length += bitmapOffset;
      header.writeInt(bitmapOffset);
//      System.out.println("bitmapOffset = " + bitmapOffset);
      //set the starting position where the actual bitmaps will be written
      fos.getChannel().position(bitmapOffset);

      int startIndex = 0;
      int[] bitmapOffsets = new int[_numRanges];
      for (int i = 0; i < rangeStartList.size(); i++) {
        int rangeStartDictId = rangeStartList.get(i);
        int rangeEndDictId = (i == rangeStartList.size() - 1) ? _cardinality : rangeStartList.get(i + 1);
        MutableRoaringBitmap bitmap = new MutableRoaringBitmap();
        for (int dictId = rangeStartDictId; dictId < rangeEndDictId; dictId++) {
          int endIndex = getInt(_invertedIndexLengthBuffer, dictId);
          for (int index = startIndex; index < endIndex; index++) {
            bitmap.add(getInt(_invertedIndexValueBuffer, index));
          }
          startIndex = endIndex;
        }
        // Write offset and bitmap into file
        int sizeInBytes = bitmap.serializedSizeInBytes();
//        System.out.println("sizeInBytes = " + sizeInBytes);
        bitmapOffset += sizeInBytes;
        length += sizeInBytes;
        // Check for int overflow
        Preconditions.checkState(bitmapOffset > 0, "Inverted index file: %s exceeds 2GB limit", _invertedIndexFile);

        header.writeInt(bitmapOffset);
        byte[] bytes = new byte[sizeInBytes];
        bitmap.serialize(ByteBuffer.wrap(bytes));
//        System.out.println("Arrays.toString(bytes) = " + Arrays.toString(bytes));
        dataOutputStream.write(bytes);
      }
//      System.out.println("bitmapOffsets = " + Arrays.toString(bitmapOffsets));
//      System.out.println("length = " + length);
    } catch (Exception e) {
      FileUtils.deleteQuietly(_invertedIndexFile);
      throw e;
    }
//    System.out.println("_invertedIndexFile = " + _invertedIndexFile.length());
  }

  @Override
  public void close()
      throws IOException {
    org.apache.pinot.common.utils.FileUtils
        .close(new DataBufferAndFile(_forwardIndexValueBuffer, _forwardIndexValueBufferFile),
            new DataBufferAndFile(_forwardIndexLengthBuffer, _forwardIndexLengthBufferFile),
            new DataBufferAndFile(_invertedIndexValueBuffer, _invertedIndexValueBufferFile),
            new DataBufferAndFile(_invertedIndexLengthBuffer, _invertedIndexLengthBufferFile));
  }

  private class DataBufferAndFile implements Closeable {
    private final PinotDataBuffer _dataBuffer;
    private final File _file;

    DataBufferAndFile(final PinotDataBuffer buffer, final File file) {
      _dataBuffer = buffer;
      _file = file;
    }

    @Override
    public void close()
        throws IOException {
      destroyBuffer(_dataBuffer, _file);
    }
  }

  private static void putInt(PinotDataBuffer buffer, long index, int value) {
    buffer.putInt(index << 2, value);
  }

  private static int getInt(PinotDataBuffer buffer, long index) {
    return buffer.getInt(index << 2);
  }

  private PinotDataBuffer createTempBuffer(long size, File mmapFile)
      throws IOException {
    if (_useMMapBuffer) {
      return PinotDataBuffer.mapFile(mmapFile, false, 0, size, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapBitmapInvertedIndexCreator: temp buffer");
    } else {
      return PinotDataBuffer.allocateDirect(size, PinotDataBuffer.NATIVE_ORDER,
          "OffHeapBitmapInvertedIndexCreator: temp buffer for " + mmapFile.getName());
    }
  }

  private void destroyBuffer(PinotDataBuffer buffer, File mmapFile)
      throws IOException {
    if (buffer != null) {
      buffer.close();
      if (mmapFile.exists()) {
        FileUtils.forceDelete(mmapFile);
      }
    }
  }

  public static void main(String[] args)
      throws IOException {
    File indexDir = new File("/tmp/testRangeIndex");
    indexDir.mkdirs();
    FieldSpec fieldSpec = new MetricFieldSpec();
    fieldSpec.setDataType(FieldSpec.DataType.INT);
    String columnName = "latency";
    fieldSpec.setName(columnName);
    int cardinality = 1000;
    int numDocs = 10000;
    int numValues = 10000;
    DictionaryBasedRangeIndexCreator creator =
        new DictionaryBasedRangeIndexCreator(indexDir, fieldSpec, cardinality, numDocs, numValues);
    Random r = new Random();
    for (int i = 0; i < numDocs; i++) {
      creator.add(r.nextInt(cardinality));
    }
    creator.seal();

    File file = new File(indexDir, columnName + BITMAP_RANGE_INDEX_FILE_EXTENSION);
    DataInputStream dis = new DataInputStream(new FileInputStream(file));
    int numRanges = dis.readInt();
    System.out.println("numRanges = " + numRanges);
    int[] rangeStart = new int[numRanges];
    for (int i = 0; i < numRanges; i++) {
      rangeStart[i] = dis.readInt();
    }
    System.out.println("Arrays.toString(rangeStart) = " + Arrays.toString(rangeStart));
    int[] rangeBitmapOffsets = new int[numRanges + 1];
    for (int i = 0; i <= numRanges; i++) {
      rangeBitmapOffsets[i] = dis.readInt();
    }
    System.out.println("Arrays.toString(rangeBitmapOffsets) = " + Arrays.toString(rangeBitmapOffsets));
    ImmutableRoaringBitmap[] bitmaps = new ImmutableRoaringBitmap[numRanges];
    for (int i = 0; i < numRanges; i++) {
      long serializedBitmapLength;
      serializedBitmapLength = rangeBitmapOffsets[i + 1] - rangeBitmapOffsets[i];
      System.out.println("serializedBitmapLength = " + serializedBitmapLength);
      byte[] bytes = new byte[(int) serializedBitmapLength];
      dis.read(bytes, 0, (int) serializedBitmapLength);
      System.out.println("bytes = " + Arrays.toString(bytes));
      bitmaps[i] = new ImmutableRoaringBitmap(ByteBuffer.wrap(bytes));
      System.out.println(bitmaps[i]);
    }
  }
}
