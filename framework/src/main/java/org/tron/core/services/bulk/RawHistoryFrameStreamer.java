/*
 * java-tron is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * java-tron is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.tron.core.services.bulk;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import org.tron.api.GrpcAPI.BytesMessage;
import org.tron.core.ChainBaseManager;

public final class RawHistoryFrameStreamer {

  private static final int WIRE_VERSION = 1;
  private static final int TARGET_FRAME_BYTES = 16 * 1024 * 1024;
  private static final int BLOCK_TRANSACTIONS_FIELD_NUMBER = 1;

  private RawHistoryFrameStreamer() {
  }

  public static void streamByLimitNext(
      ChainBaseManager chainBaseManager,
      long startNum,
      long limit,
      StreamObserver<BytesMessage> responseObserver) {
    if (limit <= 0) {
      return;
    }

    List<byte[]> blocks = chainBaseManager.getBlockStore()
        .getLimitNumberRawInOrder(startNum, limit);
    FrameWriter frame = newFrame(startNum);
    for (int index = 0; index < blocks.size(); index++) {
      long blockNum = startNum + index;
      byte[] blockBytes = blocks.get(index);
      byte[] transactionRetBytes = chainBaseManager.getTransactionRetStore()
          .getTransactionInfoByBlockNumRaw(blockNum);
      int transactionCount = countTransactions(blockBytes);
      int entrySize = estimateBlockEntrySize(blockBytes, transactionRetBytes);
      if (frame.hasBlocks() && frame.estimatedBytes() + entrySize > TARGET_FRAME_BYTES) {
        responseObserver.onNext(frame.toMessage());
        frame = newFrame(blockNum);
      }
      frame.writeBlock(blockNum, transactionCount, blockBytes, transactionRetBytes);
    }
    if (frame.hasBlocks()) {
      responseObserver.onNext(frame.toMessage());
    }
  }

  private static FrameWriter newFrame(long startBlock) {
    try {
      return new FrameWriter(startBlock);
    } catch (IOException e) {
      throw new IllegalStateException("failed to start raw history frame", e);
    }
  }

  private static int countTransactions(byte[] blockBytes) {
    try {
      CodedInputStream input = CodedInputStream.newInstance(blockBytes);
      int count = 0;
      int tag;
      while ((tag = input.readTag()) != 0) {
        if (WireFormat.getTagFieldNumber(tag) == BLOCK_TRANSACTIONS_FIELD_NUMBER) {
          count++;
        }
        input.skipField(tag);
      }
      return count;
    } catch (IOException e) {
      throw new IllegalStateException("failed to count raw block transactions", e);
    }
  }

  private static int estimateBlockEntrySize(byte[] blockBytes, byte[] transactionRetBytes) {
    int transactionRetLength = transactionRetBytes == null ? 0 : transactionRetBytes.length;
    return CodedOutputStream.computeInt64SizeNoTag(0)
        + CodedOutputStream.computeUInt32SizeNoTag(0)
        + CodedOutputStream.computeUInt32SizeNoTag(blockBytes.length)
        + blockBytes.length
        + CodedOutputStream.computeUInt32SizeNoTag(transactionRetLength)
        + transactionRetLength;
  }

  private static final class FrameWriter {

    private final ByteString.Output output = ByteString.newOutput();
    private final CodedOutputStream codedOutput = CodedOutputStream.newInstance(output);
    private int blockCount;
    private int estimatedBytes;

    private FrameWriter(long startBlock) throws IOException {
      codedOutput.writeUInt32NoTag(WIRE_VERSION);
      codedOutput.writeInt64NoTag(startBlock);
      estimatedBytes = CodedOutputStream.computeUInt32SizeNoTag(WIRE_VERSION)
          + CodedOutputStream.computeInt64SizeNoTag(startBlock);
    }

    private boolean hasBlocks() {
      return blockCount > 0;
    }

    private int estimatedBytes() {
      return estimatedBytes;
    }

    private void writeBlock(
        long blockNum,
        int transactionCount,
        byte[] blockBytes,
        byte[] transactionRetBytes) {
      try {
        codedOutput.writeInt64NoTag(blockNum);
        codedOutput.writeUInt32NoTag(transactionCount);
        codedOutput.writeUInt32NoTag(blockBytes.length);
        codedOutput.writeRawBytes(blockBytes);
        if (transactionRetBytes == null) {
          codedOutput.writeUInt32NoTag(0);
        } else {
          codedOutput.writeUInt32NoTag(transactionRetBytes.length);
          codedOutput.writeRawBytes(transactionRetBytes);
        }
        blockCount++;
        estimatedBytes += estimateBlockEntrySize(blockBytes, transactionRetBytes);
      } catch (IOException e) {
        throw new IllegalStateException("failed to write raw history block", e);
      }
    }

    private BytesMessage toMessage() {
      try {
        codedOutput.flush();
        return BytesMessage.newBuilder().setValue(output.toByteString()).build();
      } catch (IOException e) {
        throw new IllegalStateException("failed to finish raw history frame", e);
      }
    }
  }
}
