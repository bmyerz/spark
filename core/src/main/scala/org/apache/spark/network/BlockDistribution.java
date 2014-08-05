package org.apache.spark.network;

public class BlockDistribution {
  private int blockSizeMin;
  private int remainder;

  public static class Range {
    public final int leftInclusive;
    public final int rightExclusive;
    public Range(int leftInclusive, int rightExclusive) {
      this.leftInclusive = leftInclusive;
      this.rightExclusive = rightExclusive;
    }
    public int size() {
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
  public BlockDistribution(int numBlocks, int numElements) {
    this.blockSizeMin = numElements/numBlocks;
    this.remainder = numElements%numBlocks;
  }
  
  public Range getRangeForBlock(int blockId) {
    // first remainder blocks get +1 elements
    if (blockId < remainder) {
      int size = blockSizeMin + 1;
      int left = (blockSizeMin+1)*blockId;
      return new Range(left, left+size);
    } else { 
      // after remainder, blocks get +0 elements
      int size = blockSizeMin;
      int left = (blockSizeMin+1)*remainder+blockSizeMin*(blockId-remainder);
      return new Range(left, left+size);
    }
  }

  public int getBlockIdForIndex(int index) {
    if (index/(blockSizeMin+1) < remainder) {
      return index/(blockSizeMin+1);
    } else {
      return (index-remainder) / blockSizeMin;
    }
  }

}
