{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Bytecoin
  ( Bytecoin(..)
  , BytecoinBlock(..)
  , BytecoinTransaction(..)
  , BytecoinTransactionInput(..)
  , BytecoinTransactionOutput(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.Text.Encoding as T
import qualified Data.HashMap.Lazy as HML
import qualified Data.Vector as V
import Data.Time.Clock.POSIX
import Data.Maybe
import Data.Int
import Data.Proxy
import Hanalytics.Schema

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util

newtype Bytecoin = Bytecoin JsonRpc

data BytecoinBlock = BytecoinBlock
  { bc_difficulty :: {-# UNPACK #-} !Int64
  , bc_hash :: {-# UNPACK #-} !HexString
  , bc_height :: {-# UNPACK #-} !Int64
  , bc_major_version :: {-# UNPACK #-} !Int64
  , bc_minor_version :: {-# UNPACK #-} !Int64
  , bc_nonce :: {-# UNPACK #-} !Int64
  , bc_reward :: {-# UNPACK #-} !Int64
  , bc_size :: {-# UNPACK #-} !Int64
  , bc_timestamp :: {-# UNPACK #-} !Int64
  , bc_coinbase_transaction :: !BytecoinTransaction
  , bc_transactions :: !(V.Vector BytecoinTransaction)
  }

instance IsBlock BytecoinBlock where
  getBlockHeight = bc_height
  getBlockTimestamp = posixSecondsToUTCTime . fromIntegral . bc_timestamp

newtype BytecoinBlockWrapper = BytecoinBlockWrapper
  { unwrapBytecoinBlock :: BytecoinBlock
  }

instance J.FromJSON BytecoinBlockWrapper where
  parseJSON = J.withObject "bytecoin block" $ \fields -> fmap BytecoinBlockWrapper $ BytecoinBlock
    <$> (fields J..: "difficulty")
    <*> (fields J..: "hash")
    <*> (fields J..: "height")
    <*> (fields J..: "major_version")
    <*> (fields J..: "minor_version")
    <*> (fields J..: "nonce")
    <*> (fields J..: "reward")
    <*> (fields J..: "size")
    <*> (fields J..: "timestamp")
    <*> (unwrapBytecoinTransaction <$> fields J..: "coinbase_transaction")
    <*> (V.map unwrapBytecoinTransaction <$> fields J..: "transactions")

data BytecoinTransaction = BytecoinTransaction
  { mt_extra :: {-# UNPACK #-} !HexString
  , mt_fee :: !(Maybe Int64)
  , mt_hash :: !(Maybe HexString)
  , mt_inputs :: !(V.Vector BytecoinTransactionInput)
  , mt_outputs :: !(V.Vector BytecoinTransactionOutput)
  , mt_unlock_block_or_timestamp :: {-# UNPACK #-} !Int64
  , mt_version :: {-# UNPACK #-} !Int64
  }

newtype BytecoinTransactionWrapper = BytecoinTransactionWrapper
  { unwrapBytecoinTransaction :: BytecoinTransaction
  }

instance J.FromJSON BytecoinTransactionWrapper where
  parseJSON = J.withObject "bytecoin transaction" $ \fields -> fmap BytecoinTransactionWrapper $ BytecoinTransaction
    <$> (HexString . BS.toShort . B.pack <$> fields J..: "extra")
    <*> (fields J..:? "fee")
    <*> (fields J..:? "hash")
    <*> (V.map unwrapBytecoinTransactionInput <$> fields J..: "inputs")
    <*> (V.map unwrapBytecoinTransactionOutput <$> fields J..: "outputs")
    <*> (fields J..: "unlock_block_or_timestamp")
    <*> (fields J..: "version")

data BytecoinTransactionInput = BytecoinTransactionInput
  { mti_amount :: !(Maybe Int64)
  , mti_height :: !(Maybe Int64)
  , mti_key_image :: !(Maybe HexString)
  , mti_output_indexes :: !(V.Vector Int64)
  }

newtype BytecoinTransactionInputWrapper = BytecoinTransactionInputWrapper
  { unwrapBytecoinTransactionInput :: BytecoinTransactionInput
  }

instance J.FromJSON BytecoinTransactionInputWrapper where
  parseJSON = J.withObject "bytecoin transaction input" $ \fields -> do
    maybeKey <- fields J..:? "key"
    maybeGen <- fields J..:? "gen"
    fmap BytecoinTransactionInputWrapper $ BytecoinTransactionInput
      <$> traverse (J..: "amount") maybeKey
      <*> traverse (J..: "height") maybeGen
      <*> traverse (J..: "key_image") maybeKey
      <*> (fromMaybe V.empty <$> traverse (J..: "output_indexes") maybeKey)

data BytecoinTransactionOutput = BytecoinTransactionOutput
  { mto_amount :: !Int64
  , mto_key :: {-# UNPACK #-} !HexString
  }

newtype BytecoinTransactionOutputWrapper = BytecoinTransactionOutputWrapper
  { unwrapBytecoinTransactionOutput :: BytecoinTransactionOutput
  }

instance J.FromJSON BytecoinTransactionOutputWrapper where
  parseJSON = J.withObject "bytecoin transaction output" $ \fields -> fmap BytecoinTransactionOutputWrapper $ BytecoinTransactionOutput
    <$> (fields J..: "amount")
    <*> ((J..: "key") =<< fields J..: "target")

genSchemaInstances [''BytecoinBlock, ''BytecoinTransaction, ''BytecoinTransactionInput, ''BytecoinTransactionOutput]
genFlattenedTypes "height" [| bc_height |] [("block", ''BytecoinBlock), ("transaction", ''BytecoinTransaction), ("input", ''BytecoinTransactionInput), ("output", ''BytecoinTransactionOutput)]

instance BlockChain Bytecoin where
  type Block Bytecoin = BytecoinBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return $ Bytecoin $ newJsonRpc httpManager httpRequest Nothing
    , bci_defaultApiUrl = "http://127.0.0.1:8081/json_rpc"
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -60 -- conservative rewrite limit
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy BytecoinBlock))
      [ schemaOf (Proxy :: Proxy BytecoinTransactionInput)
      , schemaOf (Proxy :: Proxy BytecoinTransactionOutput)
      , schemaOf (Proxy :: Proxy BytecoinTransaction)
      ]
      "CREATE TABLE \"bytecoin\" OF \"BytecoinBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "inputs", "outputs"]
    , bci_flattenPack = let
      f (blocks, (transactions, inputs, outputs)) =
        [ SomeBlocks (blocks :: [BytecoinBlock_flattened])
        , SomeBlocks (transactions :: [BytecoinTransaction_flattened])
        , SomeBlocks (inputs :: [BytecoinTransactionInput_flattened])
        , SomeBlocks (outputs :: [BytecoinTransactionOutput_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight (Bytecoin jsonRpc) =
    either fail (return . (+ (-1))) . J.parseEither (J..: "count") =<< jsonRpcRequest jsonRpc "getblockcount" J.Null

  getBlockByHeight (Bytecoin jsonRpc) blockHeight = do
    blockInfo <- jsonRpcRequest jsonRpc "getrawblock" (J.Object [("height", J.toJSON blockHeight)])
    J.Object blockHeaderFields <- either fail return $ J.parseEither (J..: "block_header") blockInfo
    Just (J.Bool False) <- return $ HML.lookup "orphan_status" blockHeaderFields
    Just blockHash <- return $ HML.lookup "hash" blockHeaderFields
    Just blockDifficulty <- return $ HML.lookup "difficulty" blockHeaderFields
    Just blockReward <- return $ HML.lookup "reward" blockHeaderFields
    Just blockSize <- return $ HML.lookup "block_size" blockHeaderFields
    blockJsonFields <- either fail return $ ((J.eitherDecode' . BL.fromStrict . T.encodeUtf8) =<<) $ J.parseEither (J..: "json") blockInfo
    transactionHashes <- either fail return $ J.parseEither (J..: "transaction_hashes") blockJsonFields
    transactions <- if V.null transactionHashes
      then return V.empty
      else do
        transactionsJsons <- either fail return . J.parseEither (J..: "txs_as_json") =<< nonJsonRpcRequest jsonRpc "/gettransactions" (J.Object [("transaction_hashes", J.Array transactionHashes), ("decode_as_json", J.Bool True)])
        V.forM (V.zip transactionHashes transactionsJsons) $ \(txHash, transactionJson) -> do
          transactionFields <- either fail return $ J.eitherDecode' $ BL.fromStrict $ T.encodeUtf8 transactionJson
          return $ J.Object $ HML.insert "hash" txHash transactionFields
    let
      jsonBlock = J.Object
        $ HML.insert "height" (J.toJSON blockHeight)
        $ HML.insert "hash" blockHash
        $ HML.insert "difficulty" blockDifficulty
        $ HML.insert "reward" blockReward
        $ HML.insert "size" blockSize
        $ HML.insert "transactions" (J.Array transactions)
        blockJsonFields
    case J.fromJSON jsonBlock of
      J.Success block -> return $ unwrapBytecoinBlock block
      J.Error err -> fail err

  blockHeightFieldName _ = "height"
