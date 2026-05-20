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
import com.google.protobuf.CodedOutputStream;
import java.io.IOException;
import java.util.List;
import org.tron.api.GrpcAPI.BytesMessage;
import org.tron.core.ChainBaseManager;

public final class CallTargetHistoryWireBuilder {

  private static final int WIRE_VERSION = 1;
  private static final int BLOCK_TRANSACTIONS_FIELD_NUMBER = 1;
  private static final int TRANSACTION_RAW_DATA_FIELD_NUMBER = 1;
  private static final int RAW_CONTRACT_FIELD_NUMBER = 11;
  private static final int CONTRACT_TYPE_FIELD_NUMBER = 1;
  private static final int CONTRACT_PARAMETER_FIELD_NUMBER = 2;
  private static final int ANY_VALUE_FIELD_NUMBER = 2;
  private static final int TRIGGER_CONTRACT_ADDRESS_FIELD_NUMBER = 2;
  private static final int TRIGGER_SMART_CONTRACT_TYPE = 31;
  private static final int TRON_ADDRESS_BYTES = 21;
  private static final int EVM_ADDRESS_BYTES = 20;
  private static final byte TRON_PREFIX = 0x41;

  private CallTargetHistoryWireBuilder() {
  }

  public static BytesMessage encodeByLimitNext(
      ChainBaseManager chainBaseManager,
      long startNum,
      long limit) {
    if (limit <= 0) {
      return BytesMessage.getDefaultInstance();
    }

    try {
      List<byte[]> blocks = chainBaseManager.getBlockStore()
          .getLimitNumberRawInOrder(startNum, limit);
      ByteString.Output output = ByteString.newOutput();
      CodedOutputStream codedOutput = CodedOutputStream.newInstance(output);
      codedOutput.writeUInt32NoTag(WIRE_VERSION);
      codedOutput.writeUInt32NoTag(blocks.size());
      for (int index = 0; index < blocks.size(); index++) {
        writeBlock(codedOutput, startNum + index, blocks.get(index));
      }
      codedOutput.flush();
      return BytesMessage.newBuilder().setValue(output.toByteString()).build();
    } catch (IOException e) {
      throw new IllegalStateException("failed to encode call target history wire range", e);
    }
  }

  private static void writeBlock(CodedOutputStream output, long blockNum, byte[] blockBytes)
      throws IOException {
    BlockTargets targets = scanBlock(blockBytes);
    output.writeInt64NoTag(blockNum);
    output.writeUInt32NoTag(targets.transactionCount);
    output.writeUInt32NoTag(targets.targetCount);
    output.writeRawBytes(targets.rows.toByteString());
  }

  private static BlockTargets scanBlock(byte[] blockBytes) throws IOException {
    Cursor block = new Cursor(blockBytes, 0, blockBytes.length);
    ByteString.Output rowsOutput = ByteString.newOutput();
    CodedOutputStream rows = CodedOutputStream.newInstance(rowsOutput);
    int transactionCount = 0;
    int targetCount = 0;
    int tag;
    while ((tag = block.readTag()) != 0) {
      if (fieldNumber(tag) == BLOCK_TRANSACTIONS_FIELD_NUMBER && wireType(tag) == 2) {
        ByteSlice transaction = block.readBytes();
        ByteSlice contractAddress = extractTriggerContractAddress(transaction);
        if (isValidAddress(contractAddress)) {
          rows.writeUInt32NoTag(transactionCount);
          rows.writeUInt32NoTag(contractAddress.length);
          rows.writeRawBytes(contractAddress.data, contractAddress.offset, contractAddress.length);
          targetCount++;
        }
        transactionCount++;
      } else {
        block.skipField(tag);
      }
    }
    rows.flush();
    return new BlockTargets(transactionCount, targetCount, rowsOutput);
  }

  private static ByteSlice extractTriggerContractAddress(ByteSlice transactionBytes)
      throws IOException {
    Cursor transaction = new Cursor(transactionBytes);
    int tag;
    while ((tag = transaction.readTag()) != 0) {
      if (fieldNumber(tag) == TRANSACTION_RAW_DATA_FIELD_NUMBER && wireType(tag) == 2) {
        return extractRawDataContractAddress(transaction.readBytes());
      }
      transaction.skipField(tag);
    }
    return ByteSlice.EMPTY;
  }

  private static ByteSlice extractRawDataContractAddress(ByteSlice rawDataBytes)
      throws IOException {
    Cursor rawData = new Cursor(rawDataBytes);
    int tag;
    while ((tag = rawData.readTag()) != 0) {
      if (fieldNumber(tag) == RAW_CONTRACT_FIELD_NUMBER && wireType(tag) == 2) {
        return extractContractAddress(rawData.readBytes());
      }
      rawData.skipField(tag);
    }
    return ByteSlice.EMPTY;
  }

  private static ByteSlice extractContractAddress(ByteSlice contractBytes) throws IOException {
    Cursor contract = new Cursor(contractBytes);
    int contractType = -1;
    ByteSlice parameterValue = ByteSlice.EMPTY;
    int tag;
    while ((tag = contract.readTag()) != 0) {
      if (fieldNumber(tag) == CONTRACT_TYPE_FIELD_NUMBER && wireType(tag) == 0) {
        contractType = contract.readVarint32();
      } else if (fieldNumber(tag) == CONTRACT_PARAMETER_FIELD_NUMBER && wireType(tag) == 2) {
        parameterValue = extractAnyValue(contract.readBytes());
      } else {
        contract.skipField(tag);
      }
    }
    if (contractType != TRIGGER_SMART_CONTRACT_TYPE || parameterValue.isEmpty()) {
      return ByteSlice.EMPTY;
    }
    return extractTriggerContractAddressFromParameter(parameterValue);
  }

  private static ByteSlice extractAnyValue(ByteSlice anyBytes) throws IOException {
    Cursor any = new Cursor(anyBytes);
    int tag;
    while ((tag = any.readTag()) != 0) {
      if (fieldNumber(tag) == ANY_VALUE_FIELD_NUMBER && wireType(tag) == 2) {
        return any.readBytes();
      }
      any.skipField(tag);
    }
    return ByteSlice.EMPTY;
  }

  private static ByteSlice extractTriggerContractAddressFromParameter(ByteSlice parameterBytes)
      throws IOException {
    Cursor parameter = new Cursor(parameterBytes);
    int tag;
    while ((tag = parameter.readTag()) != 0) {
      if (fieldNumber(tag) == TRIGGER_CONTRACT_ADDRESS_FIELD_NUMBER && wireType(tag) == 2) {
        return parameter.readBytes();
      }
      parameter.skipField(tag);
    }
    return ByteSlice.EMPTY;
  }

  private static int fieldNumber(int tag) {
    return tag >>> 3;
  }

  private static int wireType(int tag) {
    return tag & 7;
  }

  private static boolean isValidAddress(ByteSlice address) {
    if (address == null) {
      return false;
    }
    if (address.length == EVM_ADDRESS_BYTES) {
      return true;
    }
    return address.length == TRON_ADDRESS_BYTES && address.data[address.offset] == TRON_PREFIX;
  }

  private static final class BlockTargets {

    private final int transactionCount;
    private final int targetCount;
    private final ByteString.Output rows;

    private BlockTargets(int transactionCount, int targetCount, ByteString.Output rows) {
      this.transactionCount = transactionCount;
      this.targetCount = targetCount;
      this.rows = rows;
    }
  }

  private static final class ByteSlice {

    private static final ByteSlice EMPTY = new ByteSlice(new byte[0], 0, 0);

    private final byte[] data;
    private final int offset;
    private final int length;

    private ByteSlice(byte[] data, int offset, int length) {
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    private boolean isEmpty() {
      return length <= 0;
    }
  }

  private static final class Cursor {

    private final byte[] data;
    private final int limit;
    private int position;

    private Cursor(ByteSlice slice) {
      this(slice.data, slice.offset, slice.length);
    }

    private Cursor(byte[] data, int offset, int length) {
      if (offset < 0 || length < 0 || offset > data.length || length > data.length - offset) {
        throw new IllegalArgumentException("cursor range is outside the backing array");
      }
      this.data = data;
      this.position = offset;
      this.limit = offset + length;
    }

    private boolean isAtEnd() {
      return position >= limit;
    }

    private int readTag() throws IOException {
      if (isAtEnd()) {
        return 0;
      }
      return readVarint32();
    }

    private int readVarint32() throws IOException {
      long value = readVarint64();
      if (value > Integer.MAX_VALUE) {
        throw new IOException("varint32 value out of range");
      }
      return (int) value;
    }

    private long readVarint64() throws IOException {
      long result = 0L;
      for (int shift = 0; shift < 64; shift += 7) {
        if (position >= limit) {
          throw new IOException("truncated varint");
        }
        int b = data[position++] & 0xff;
        result |= (long) (b & 0x7f) << shift;
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      throw new IOException("malformed varint");
    }

    private ByteSlice readBytes() throws IOException {
      int length = readVarint32();
      if (length < 0 || position + length > limit) {
        throw new IOException("invalid length-delimited field length=" + length);
      }
      ByteSlice slice = new ByteSlice(data, position, length);
      position += length;
      return slice;
    }

    private void skipBytes() throws IOException {
      int length = readVarint32();
      if (length < 0 || position + length > limit) {
        throw new IOException("invalid length-delimited field length=" + length);
      }
      position += length;
    }

    private void skipRaw(int count) throws IOException {
      if (count < 0 || position + count > limit) {
        throw new IOException("truncated raw field");
      }
      position += count;
    }

    private void skipField(int tag) throws IOException {
      switch (wireType(tag)) {
        case 0:
          readVarint64();
          return;
        case 1:
          skipRaw(8);
          return;
        case 2:
          skipBytes();
          return;
        case 5:
          skipRaw(4);
          return;
        default:
          throw new IOException("unsupported wire type " + wireType(tag));
      }
    }
  }
}
