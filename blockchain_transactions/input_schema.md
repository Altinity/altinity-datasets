### source

s3://aws-public-blockchain/v1.0/btc/transactions/date=*/part-00000-*.parquet

### input schema 

```
CREATE TABLE transactions
(
    hash Nullable(String),
    version Nullable(Int64),
    size Nullable(Int64),
    block_hash Nullable(String),
    block_number Nullable(Int64),
    index Nullable(Int64),
    virtual_size Nullable(Int64),
    lock_time Nullable(Int64),
    input_count Nullable(Int64),
    output_count Nullable(Int64),
    is_coinbase Nullable(Bool),
    output_value Nullable(Float64),
    outputs Array(
        Tuple(
            address Nullable(String),
            index Nullable(Int64),
            required_signatures Nullable(Int64),
            script_asm Nullable(String),
            script_hex Nullable(String),
            type Nullable(String),
            value Nullable(Float64)
        )
    ),
    block_timestamp Nullable(DateTime64(9)),
    date Nullable(String),
    last_modified Nullable(DateTime64(9)),
    fee Nullable(Float64),
    input_value Nullable(Float64),
    inputs Array(
        Tuple(
            address Nullable(String),
            index Nullable(Int64),
            required_signatures Nullable(Int64),
            script_asm Nullable(String),
            script_hex Nullable(String),
            sequence Nullable(Int64),
            spent_output_index Nullable(Int64),
            spent_transaction_hash Nullable(String),
            txinwitness Array(Nullable(String)),
            type Nullable(String),
            value Nullable(Float64)
        )
    )
)
```

## Explanations

### Top-Level Columns

1. **`hash`** (Nullable(String)):
   - Unique identifier (hash) of the transaction.
   - Nullable means it may be absent or contain null values.

2. **`version`** (Nullable(Int64)):
   - Version of the transaction format.
   - Used for backward compatibility.

3. **`size`** (Nullable(Int64)):
   - Total size of the transaction in bytes, including inputs and outputs.

4. **`block_hash`** (Nullable(String)):
   - Hash of the block that contains this transaction.

5. **`block_number`** (Nullable(Int64)):
   - Height of the block in the blockchain that contains this transaction.

6. **`index`** (Nullable(Int64)):
   - Index position of the transaction within the block.

7. **`virtual_size`** (Nullable(Int64)):
   - A metric used to calculate transaction fees. It represents the size of the transaction in weight units.

8. **`lock_time`** (Nullable(Int64)):
   - The earliest time or block height at which the transaction can be added to the blockchain.

9. **`input_count`** (Nullable(Int64)):
   - Number of inputs in the transaction.

10. **`output_count`** (Nullable(Int64)):
    - Number of outputs in the transaction.

11. **`is_coinbase`** (Nullable(Bool)):
    - Indicates whether the transaction is a coinbase transaction (true/false).

12. **`output_value`** (Nullable(Float64)):
    - Total value of all outputs in the transaction, in BTC or satoshis.

13. **`outputs`** (Array(Tuple(...))):
    - Array of output details (one tuple per output). Explanation below.

14. **`block_timestamp`** (Nullable(DateTime64(9))):
    - Timestamp of the block that contains the transaction, with nanosecond precision.

15. **`date`** (Nullable(String)):
    - String representation of the date associated with the transaction.

16. **`last_modified`** (Nullable(DateTime64(9))):
    - Timestamp indicating when the transaction data was last modified.

17. **`fee`** (Nullable(Float64)):
    - Fee paid for the transaction, in BTC or satoshis.

18. **`input_value`** (Nullable(Float64)):
    - Total value of all inputs in the transaction, in BTC or satoshis.

19. **`inputs`** (Array(Tuple(...))):
    - Array of input details (one tuple per input). Explanation below.

---

### Nested Tuples

#### **`outputs`** (Array(Tuple)):
Each element in this array represents an individual output. Fields in the tuple:

1. **`address`** (Nullable(String)):
   - Destination address for this output.

2. **`index`** (Nullable(Int64)):
   - Index position of the output in the transaction.

3. **`required_signatures`** (Nullable(Int64)):
   - Number of signatures required to spend this output.

4. **`script_asm`** (Nullable(String)):
   - Script in human-readable assembly format for this output.

5. **`script_hex`** (Nullable(String)):
   - Script in raw hexadecimal format for this output.

6. **`type`** (Nullable(String)):
   - Type of the output (e.g., P2PKH, P2SH).

7. **`value`** (Nullable(Float64)):
   - Value of this output in BTC or satoshis.

#### **`inputs`** (Array(Tuple)):
Each element in this array represents an individual input. Fields in the tuple:

1. **`address`** (Nullable(String)):
   - Source address for this input.

2. **`index`** (Nullable(Int64)):
   - Index of the input in the transaction.

3. **`required_signatures`** (Nullable(Int64)):
   - Number of signatures required to spend the input.

4. **`script_asm`** (Nullable(String)):
   - Script in human-readable assembly format for this input.

5. **`script_hex`** (Nullable(String)):
   - Script in raw hexadecimal format for this input.

6. **`sequence`** (Nullable(Int64)):
   - Sequence number of the input, used for relative timelock.

7. **`spent_output_index`** (Nullable(Int64)):
   - Index of the output in the previous transaction that this input spends.

8. **`spent_transaction_hash`** (Nullable(String)):
   - Hash of the previous transaction whose output is being spent.

9. **`txinwitness`** (Array(Nullable(String))):
   - Array of witness data for SegWit inputs.

10. **`type`** (Nullable(String)):
    - Type of the input (e.g., P2PKH, P2SH).

11. **`value`** (Nullable(Float64)):
    - Value of the input in BTC or satoshis.
