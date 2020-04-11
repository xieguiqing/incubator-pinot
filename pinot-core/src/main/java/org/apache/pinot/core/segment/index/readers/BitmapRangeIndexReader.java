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
package org.apache.pinot.core.segment.index.readers;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BitmapRangeIndexReader implements InvertedIndexReader<ImmutableRoaringBitmap> {
  public static final Logger LOGGER = LoggerFactory.getLogger(BitmapRangeIndexReader.class);

  private final PinotDataBuffer _buffer;
  private final int _numRanges;
  final int _bitmapIndexOffset;
  private final int[] _rangeStartArray;

  private volatile SoftReference<SoftReference<ImmutableRoaringBitmap>[]> _bitmaps = null;

  /**
   * Constructs an inverted index with the specified size.
   * @param indexDataBuffer data buffer for the inverted index.
   */
  public BitmapRangeIndexReader(PinotDataBuffer indexDataBuffer) {
    _buffer = indexDataBuffer;
    _numRanges = _buffer.getInt(0);
    _rangeStartArray = new int[_numRanges];
    _bitmapIndexOffset = Integer.BYTES + _numRanges * Integer.BYTES;
    final int lastOffset = _buffer.getInt(_numRanges * Integer.BYTES + (_numRanges + 1) * Integer.BYTES);

    Preconditions.checkState(lastOffset == _buffer.size(),
        "The last offset should be equal to buffer size! Current lastOffset: " + lastOffset + ", buffer size: "
            + _buffer.size());
    for (int i = 0; i < _numRanges; i++) {
      _rangeStartArray[i] = _buffer.getInt(Integer.BYTES + i * Integer.BYTES);
    }
    System.out.println("_rangeStartArray = " + _rangeStartArray);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ImmutableRoaringBitmap getDocIds(int rangeId) {
    SoftReference<ImmutableRoaringBitmap>[] bitmapArrayReference = null;
    // Return the bitmap if it's still on heap
    if (_bitmaps != null) {
      bitmapArrayReference = _bitmaps.get();
      if (bitmapArrayReference != null) {
        SoftReference<ImmutableRoaringBitmap> bitmapReference = bitmapArrayReference[rangeId];
        if (bitmapReference != null) {
          ImmutableRoaringBitmap value = bitmapReference.get();
          if (value != null) {
            return value;
          }
        }
      } else {
        bitmapArrayReference = new SoftReference[_numRanges];
        _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
      }
    } else {
      bitmapArrayReference = new SoftReference[_numRanges];
      _bitmaps = new SoftReference<SoftReference<ImmutableRoaringBitmap>[]>(bitmapArrayReference);
    }
    synchronized (this) {
      ImmutableRoaringBitmap value;
      if (bitmapArrayReference[rangeId] == null || bitmapArrayReference[rangeId].get() == null) {
        value = buildRoaringBitmapForIndex(rangeId);
        bitmapArrayReference[rangeId] = new SoftReference<ImmutableRoaringBitmap>(value);
      } else {
        value = bitmapArrayReference[rangeId].get();
      }
      return value;
    }
  }

  @Override
  public ImmutableRoaringBitmap getDocIds(Object value) {
    // This should not be called from anywhere. If it happens, there is a bug
    // and that's why we throw illegal state exception
    throw new IllegalStateException("bitmap inverted index reader supports lookup only on dictionary id");
  }

  private synchronized ImmutableRoaringBitmap buildRoaringBitmapForIndex(final int rangeId) {
    final int currentOffset = getOffset(rangeId);
    final int nextOffset = getOffset(rangeId + 1);
    final int bufferLength = nextOffset - currentOffset;

    // Slice the buffer appropriately for Roaring Bitmap
    ByteBuffer bb = _buffer.toDirectByteBuffer(currentOffset, bufferLength);
    return new ImmutableRoaringBitmap(bb);
  }

  private int getOffset(final int rangeId) {
    return _buffer.getInt(_bitmapIndexOffset + rangeId * Integer.BYTES);
  }

  @Override
  public void close()
      throws IOException {
    _buffer.close();
  }

  public int findRangeId(int dictId) {
    for (int i = 0; i < _rangeStartArray.length - 1; i++) {
      if (dictId >= _rangeStartArray[i] && dictId < _rangeStartArray[i + 1]) {
        return i;
      }
    }
    return _rangeStartArray.length - 1;
  }
}
