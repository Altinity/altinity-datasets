-- btc opcode decoding - https://en.bitcoin.it/wiki/Script
create or replace function  btcOpcodeName AS (op) ->
    if(op between 1 and 75,'',
       dictGetOrDefault('btc_codes_dict','code',op,'OP_UNKNOWN_' || toString(op)));

create table btc.opcodes (code String, hex String) engine = Log;
create or replace dictionary btc.opcodes_dict (hex UInt64, code String)
primary key hex
layout ( HASHED() ) LIFETIME(0)
source (clickhouse (query 'select reinterpretAsUInt8(unhex(hex)), opcode from btc.opcodes '))
;

-- https://bsv.brc.dev/scripts/0014
create or replace function  btcOpcodeName1 AS (op) ->
    if(op between 1 and 75,'', transform(op,
[
    -- Constants
    0x00, 0x4C, 0x4D, 0x4E, 0x4F, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5A, 0x5B, 0x5C, 0x5D, 0x5E, 0x5F, 0x60,
    -- Flow control
    0x61, 0x63, 0x64, 0x67, 0x68, 0x69, 0x6A,
    -- Stack operations
    0x6B, 0x6C, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7A, 0x7B, 0x7C, 0x7D,
    -- Arithmetic
    0x8F, 0x90, 0x91, 0x92, 0x93, 0x94, 0x95, 0x96, 0x97,
    -- Cryptographic operations
    0xA6, 0xA7, 0xA8, 0xA9, 0xAA, 0xAC, 0xAD, 0xAE, 0xAF,
    -- Reserved/Disabled
    0x7E, 0x7F, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86,
    -- Equality and verification
    0x87, 0x88
],
[
    -- Constants
    'OP_0', 'OP_PUSHDATA1', 'OP_PUSHDATA2', 'OP_PUSHDATA4', 'OP_1NEGATE', 'OP_1', 'OP_2', 'OP_3', 'OP_4', 'OP_5',
    'OP_6', 'OP_7', 'OP_8', 'OP_9', 'OP_10', 'OP_11', 'OP_12', 'OP_13', 'OP_14', 'OP_15', 'OP_16',
    -- Flow control
    'OP_NOP', 'OP_IF', 'OP_NOTIF', 'OP_ELSE', 'OP_ENDIF', 'OP_VERIFY', 'OP_RETURN',
    -- Stack operations
    'OP_TOALTSTACK', 'OP_FROMALTSTACK', 'OP_IFDUP', 'OP_DEPTH', 'OP_DROP', 'OP_DUP', 'OP_NIP', 'OP_OVER', 'OP_PICK',
    'OP_ROLL', 'OP_ROT', 'OP_SWAP', 'OP_TUCK',
    -- Arithmetic
    'OP_ADD', 'OP_SUB', 'OP_MUL (disabled)', 'OP_NEGATE', 'OP_ABS', 'OP_NOT', 'OP_0NOTEQUAL', 'OP_EQUAL', 'OP_EQUALVERIFY',
    -- Cryptographic operations
    'OP_RIPEMD160', 'OP_SHA1', 'OP_SHA256', 'OP_HASH160', 'OP_HASH256', 'OP_CHECKSIG', 'OP_CHECKSIGVERIFY',
    'OP_CHECKMULTISIG', 'OP_CHECKMULTISIGVERIFY',
    -- Reserved/Disabled
    'OP_CAT (disabled)', 'OP_SUBSTR (disabled)', 'OP_LEFT (disabled)', 'OP_RIGHT (disabled)',
    'OP_AND (disabled)', 'OP_OR (disabled)', 'OP_XOR (disabled)', 'OP_2MUL (disabled)', 'OP_2DIV (disabled)',
     -- Equality and verification
    'OP_EQUAL', 'OP_EQUALVERIFY'
],

    'OP_UNKNOWN_' || toString(op)
   ) )
;

-- stored procedures emulation for running recusive CTEs
-- {condition} contains something like (`script_hex`) IN (('aaa'),('bbb')). Let's extract it
create or replace function  directDictParams AS (par) -> extractAll(par,$$\('([^']+)'\)(?:,)?|\('([^']+)'\)$$ );
CREATE OR REPLACE DICTIONARY btc.DisAsmDict (
    script_bin String,
    decoded Array(Tuple(UInt8,String))
--    decoded String
) PRIMARY KEY script_bin LAYOUT(Direct)
SOURCE(CLICKHOUSE(QUERY 'select bytes,decoded from btcDisAsm(scripts=directDictParams($${condition}$$))'))
--SOURCE(CLICKHOUSE(QUERY 'select untuple(arrayJoin(arrayMap(x->(x, x||''--''),directDictParams($${condition}$$))))'))
;
select dictGet('btc.DisAsmDict','decoded',c1) from values ('a','b','c');
select arrayMap(op-> btcOpcodeName(op.1) || lower(hex(op.2)), dictGet('btcDisAsmDict','decoded',c1))
from values ('76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac',
 '5214abcdefabbaabbaabbaabbaabbaabbaabbaabba0014abcdefabbaabbaabbaabbaabbaabbaabbaabba0014abcdefabbaabbaabbaabbaabbaabbaabbaabba0053ae')
;

create or replace view btc.DisAsm as
WITH RECURSIVE disassembly AS (

-- initial state
    SELECT  bytes,
            0                                                AS pos,
            cast([],'Array(Tuple(UInt8,String))')            AS decoded
    from (select arrayJoin({scripts:Array(String)}) as bytes)

    UNION ALL WITH

    -- Recursive step
    reinterpretAsUInt8(substr(unhex(bytes),pos+1,1)) as opcode,
    multiIf( -- Compute bytes_consumed
            opcode between 1 and 75, -- number of bytes
                (0,toUInt32(opcode)),
            opcode = 76,             -- OP_PUSHDATA1: next byte is len
                (1,toUInt32(reinterpretAsUInt8(substr(unhex(bytes),pos+2,1)))),
            opcode = 77,             -- OP_PUSHDATA2: next 2 bytes is len
                (2,toUInt32(reinterpretAsUInt16(substr(unhex(bytes),pos+2,2)))),
            opcode = 78,             -- OP_PUSHDATA4: next 4 bytes is len
                (4,toUInt32(reinterpretAsUInt32(substr(unhex(bytes),pos+2,4)))),
            (0,0::UInt32)  -- just opcode
    ) AS consumed
    SELECT  bytes,
            pos+1+consumed.1+consumed.2,
            arrayPushBack(decoded, (opcode, (substr(unhex(bytes),pos+2+consumed.1,consumed.2)) )) as decoded
    FROM disassembly
    WHERE pos < length(unhex(bytes))
) select bytes, decoded from disassembly order by pos desc limit 1 by bytes
;

-- usage
select bytes,arrayMap(op-> btcOpcodeName(op.1) || lower(hex(op.2)), decoded ) from btc.DisAsm(
        scripts = ['76a91489abcdefabbaabbaabbaabbaabbaabbaabbaabba88ac',
 '5214abcdefabbaabbaabbaabbaabbaabbaabbaabba0014abcdefabbaabbaabbaabbaabbaabbaabbaabba0014abcdefabbaabbaabbaabbaabbaabbaabbaabba0053ae']
);

-- test suite
-- https://chatgpt.com/c/6738a304-06b8-8008-9469-5b795329542c
with arrayMap(op-> btcOpcodeName(op.1) || lower(hex(op.2)), dictGet('btc.DisAsmDict','decoded',c2)) as c4
select  c1, c4 = c3 as a, c3 as test, c4 as result
from  values (
-- P2PKH Full Example Locking Script (ScriptPubKey)
     (0, '76a91489abcdef89abcdef89abcdef89abcdef89abcdef88ac', ['OP_DUP', 'OP_HASH160', '89abcdef89abcdef89abcdef89abcdef89abcdef', 'OP_EQUALVERIFY', 'OP_CHECKSIG'] ),
 -- OP_CHECKMULTISIG (2-of-3 Multisig)  Locking Script (ScriptPubKey)
 (1, '522103b4630a3b2f1c45d3e5c7b1f9a7c5e3d1f0c4e8d6a5b7c9e1a3d5c7b9e0f4a62103c4a6b2e1f3d5c7b8e9a0f2b5c6d8a3e7c9a4b2d6e3f1a7e9b0c8a5d7e6c9a053ae', ['OP_2', '03b4630a3b2f1c45d3e5c7b1f9a7c5e3d1f0c4e8d6a5b7c9e1a3d5c7b9e0f4a6', '03c4a6b2e1f3d5c7b8e9a0f2b5c6d8a3e7c9a4b2d6e3f1a7e9b0c8a5d7e6c9a0', 'OP_3', 'OP_CHECKMULTISIG'] ),
-- OP_RETURN with Metadata
      (2,'6a1048656c6c6f2c20576f726c6421',['OP_RETURN', '48656c6c6f2c20576f726c6421']),
-- OP_IF-ELSE Condition Locking Script (ScriptPubKey)
      (3,'635a91489abcdef1234567890abcdef1234567890abcdef88ac675b68',['OP_IF', 'OP_HASH160', '89abcdef1234567890abcdef1234567890abcdef', 'OP_EQUALVERIFY', 'OP_CHECKSIG', 'OP_ELSE', 'OP_RETURN', 'OP_ENDIF']),
-- OP_VERIFY for Conditional Verification
      (10,'a914aabbccddeeff00112233445566778899aabbccdd88ac',['OP_HASH160', 'aabbccddeeff00112233445566778899aabbccdd', 'OP_EQUALVERIFY', 'OP_CHECKSIG'])
)
;

select hex(hex),arrayMap(op-> btcOpcodeName(op.1) || lower(hex(op.2)), dictGet('btc.DisAsmDict','decoded',hex(hex)))
from btc_scripts limit 100;
/*
arrayMap(lambda(tuple(op), concat(btcOpcodeName(tupleElement(op, 1)), lower(hex(tupleElement(op, 2))))), dictGet('btc.DisAsmDict', 'decoded', hex(hex)))
['3045022100f5580e4adddf701f129e59bc9c1353d0dfbae11b96361c5b0b4fe2f8ac0a8b7c022077a575574f91e899abddc08dfa67fbf0f22c141c551f92c3e1b68ffb68fd35fe01', '039dcc00b95139c80a3e3bab73254b39e1b75ca4283664970ed030998bb3318824']
['304502204ea79a34c8471440bcb9fd7fc0d1122163e609c9d363be66335714d9c436d77d022100df562823b603f4743902e07a10653b94d4cc0838720707300a62c3ff0a8ee37501', '04ccab149271a4bf13059fb08676d2ce874b1062f08534d740965d8a7d09bebcd73fbd1d2b8e2a9279b09802fcf328e3f264645ed29d04b2b44d435f4cd6e9d713']
['3045022100c68aa7a77a4faae21cffc90652118e1d153b13f44e29d06337f7ffda6ffc3555022058dfd011f887f80398f6f242e013f1f592327495dd789f7a9a3e2f6ae034f3bc01', '03904537b3767116bc454f5acc7c7c37eb910b17df7052bc83422a8b5fb57e3115']
['30440220327f7ac6485d19acf94d95b4dfabc69919be9452151f56c80c8c005b4fb1ee860220465bb22f9d5431d0108fc9f5f78996fcfe5c21788bace290450ff9a8ea948e6001', '03f8be03b753d92ca1cd0b3316e59b3c8c7ba508524df27c547d558dc65d7fc469']
['3045022100bb27c527a319f2df8620f386eb2666165ee59cb337b77328ada06f96a5a569e50220497ea4c07a92b564b2e661b1f4abd1ff4ae04355c601d4d64d35c8141f44f6fb01', '02a0667a5e639812351828cfa60cd764e7ba52bace7f0e9e7f56efce30841de429']
['3045022100d4b83853b1a0fbb744950f29fc5192670c2214cfd123f75beb6ae6484ff31f9b02201b3912d0725d38470d79bde2ac9f0352a3856a9c9931d6b8ad11250c318c56e901', '02a175da0b3ada5ab6ee97c5b4b71527867ee13055aff980b50cc7a30e1097fb98']
['3045022100e28d992d3727d23066da09fe7c7871758dcd8f4f0caaec46e3dcf2d01902c32f02205d6a130f897a43534a0f376fd74e1d3387b09fc85902c4ce49bb547c00c8444701', '039faf41cb8c42efbbfa4c7d6e934d0c140052712d840f0674a13adde98266123e']
 */