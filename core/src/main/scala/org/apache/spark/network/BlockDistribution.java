package org.apache.spark.network;

public class BlockDistribution {
  private long blockSizeMin;
  private long remainder;

  public static class Range {
    public final long leftInclusive;
    public final long rightExclusive;
    public Range(long leftInclusive, long rightExclusive) {
      this.leftInclusive = leftInclusive;
      this.rightExclusive = rightExclusive;
    }
    public long size() {
      return rightExclusive-leftInclusive;
    }
  }

  /**
   * Create a block distribution of elements.
   * Each block contains contiguous elements.
   * 
   * @param numBlocks number of blocks to distribute across
   * @param numElements number of elements
   */
  public BlockDistribution(long numBlocks, long numElements) {
    this.blockSizeMin = numElements/numBlocks;
    this.remainder = numElements%numBlocks;
  }

  public Range getRangeForBlock(long blockId) {
    // first remainder blocks get +1 elements
    if (blockId < remainder) {
      long size = blockSizeMin + 1;
      long left = (blockSizeMin+1)*blockId;
      return new Range(left, left+size);
    } else { 
      // after remainder, blocks get +0 elements
      long size = blockSizeMin;
      long left = (blockSizeMin+1)*remainder+blockSizeMin*(blockId-remainder);
      return new Range(left, left+size);
    }
  }

  public long getBlockIdForIndex(long index) {
    if (index/(blockSizeMin+1) < remainder) {
      return index/(blockSizeMin+1);
    } else {
      return (index-remainder) / blockSizeMin;
    }
  }
}
