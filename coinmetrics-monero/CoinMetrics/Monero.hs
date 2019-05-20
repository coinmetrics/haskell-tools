{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Monero
  ( Monero(..)
  , MoneroBlock(..)
  , MoneroTransaction(..)
  , MoneroTransactionInput(..)
  , MoneroTransactionOutput(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Short as BS
import qualified Data.HashMap.Lazy as HML
import Data.Maybe
import Data.Int
import Data.Proxy
import qualified Data.Text.Encoding as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

newtype Monero = Monero JsonRpc

data MoneroBlock = MoneroBlock
  { mb_height :: {-# UNPACK #-} !Int64
  , mb_hash :: {-# UNPACK #-} !HexString
  , mb_major_version :: {-# UNPACK #-} !Int64
  , mb_minor_version :: {-# UNPACK #-} !Int64
  , mb_difficulty :: {-# UNPACK #-} !Int64
  , mb_reward :: {-# UNPACK #-} !Int64
  , mb_timestamp :: {-# UNPACK #-} !Int64
  , mb_nonce :: {-# UNPACK #-} !Int64
  , mb_size :: {-# UNPACK #-} !Int64
  , mb_miner_tx :: !MoneroTransaction
  , mb_transactions :: !(V.Vector MoneroTransaction)
  }

instance HasBlockHeader MoneroBlock where
  getBlockHeader MoneroBlock
    { mb_height = height
    , mb_hash = hash
    , mb_timestamp = timestamp
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp
    }

newtype MoneroBlockWrapper = MoneroBlockWrapper
  { unwrapMoneroBlock :: MoneroBlock
  }

instance J.FromJSON MoneroBlockWrapper where
  parseJSON = J.withObject "monero block" $ \fields -> fmap MoneroBlockWrapper $ MoneroBlock
    <$> (fields J..: "height")
    <*> (fields J..: "hash")
    <*> (fields J..: "major_version")
    <*> (fields J..: "minor_version")
    <*> (fields J..: "difficulty")
    <*> (fields J..: "reward")
    <*> (fields J..: "timestamp")
    <*> (fields J..: "nonce")
    <*> (fields J..: "size")
    <*> (unwrapMoneroTransaction <$> fields J..: "miner_tx")
    <*> (V.map unwrapMoneroTransaction <$> fields J..: "transactions")

data MoneroTransaction = MoneroTransaction
  { mt_hash :: !(Maybe HexString)
  , mt_version :: {-# UNPACK #-} !Int64
  , mt_unlock_time :: {-# UNPACK #-} !Int64
  , mt_vin :: !(V.Vector MoneroTransactionInput)
  , mt_vout :: !(V.Vector MoneroTransactionOutput)
  , mt_extra :: {-# UNPACK #-} !HexString
  , mt_fee :: !(Maybe Int64)
  }

newtype MoneroTransactionWrapper = MoneroTransactionWrapper
  { unwrapMoneroTransaction :: MoneroTransaction
  }

instance J.FromJSON MoneroTransactionWrapper where
  parseJSON = J.withObject "monero transaction" $ \fields -> fmap MoneroTransactionWrapper $ MoneroTransaction
    <$> (fields J..:? "hash")
    <*> (fields J..: "version")
    <*> (fields J..: "unlock_time")
    <*> (V.map unwrapMoneroTransactionInput <$> fields J..: "vin")
    <*> (V.map unwrapMoneroTransactionOutput <$> fields J..: "vout")
    <*> (HexString . BS.toShort . B.pack <$> fields J..: "extra")
    <*> parseFee fields
    where
      parseFee fields = do
        maybeRctSignatures <- fields J..:? "rct_signatures"
        case maybeRctSignatures of
          Just rctSignatures -> rctSignatures J..:? "txnFee"
          Nothing -> return Nothing

data MoneroTransactionInput = MoneroTransactionInput
  { mti_amount :: !(Maybe Int64)
  , mti_k_image :: !(Maybe HexString)
  , mti_key_offsets :: !(V.Vector Int64)
  , mti_height :: !(Maybe Int64)
  }

newtype MoneroTransactionInputWrapper = MoneroTransactionInputWrapper
  { unwrapMoneroTransactionInput :: MoneroTransactionInput
  }

instance J.FromJSON MoneroTransactionInputWrapper where
  parseJSON = J.withObject "monero transaction input" $ \fields -> do
    maybeKey <- fields J..:? "key"
    maybeGen <- fields J..:? "gen"
    fmap MoneroTransactionInputWrapper $ MoneroTransactionInput
      <$> traverse (J..: "amount") maybeKey
      <*> traverse (J..: "k_image") maybeKey
      <*> (fromMaybe V.empty <$> traverse (J..: "key_offsets") maybeKey)
      <*> traverse (J..: "height") maybeGen

data MoneroTransactionOutput = MoneroTransactionOutput
  { mto_amount :: !Int64
  , mto_key :: {-# UNPACK #-} !HexString
  }

newtype MoneroTransactionOutputWrapper = MoneroTransactionOutputWrapper
  { unwrapMoneroTransactionOutput :: MoneroTransactionOutput
  }

instance J.FromJSON MoneroTransactionOutputWrapper where
  parseJSON = J.withObject "monero transaction output" $ \fields -> fmap MoneroTransactionOutputWrapper $ MoneroTransactionOutput
    <$> (fields J..: "amount")
    <*> ((J..: "key") =<< fields J..: "target")

genSchemaInstances [''MoneroBlock, ''MoneroTransaction, ''MoneroTransactionInput, ''MoneroTransactionOutput]
genFlattenedTypes "height" [| mb_height |] [("block", ''MoneroBlock), ("transaction", ''MoneroTransaction), ("input", ''MoneroTransactionInput), ("output", ''MoneroTransactionOutput)]

instance BlockChain Monero where
  type Block Monero = MoneroBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return $ Monero $ newJsonRpc httpManager httpRequest Nothing
    , bci_defaultApiUrls = ["http://127.0.0.1:18081/json_rpc"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -60 -- conservative rewrite limit
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy MoneroBlock))
      [ schemaOf (Proxy :: Proxy MoneroTransactionInput)
      , schemaOf (Proxy :: Proxy MoneroTransactionOutput)
      , schemaOf (Proxy :: Proxy MoneroTransaction)
      ]
      "CREATE TABLE \"monero\" OF \"MoneroBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "inputs", "outputs"]
    , bci_flattenPack = let
      f (blocks, (transactions, inputs, outputs)) =
        [ SomeBlocks (blocks :: [MoneroBlock_flattened])
        , SomeBlocks (transactions :: [MoneroTransaction_flattened])
        , SomeBlocks (inputs :: [MoneroTransactionInput_flattened])
        , SomeBlocks (outputs :: [MoneroTransactionOutput_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight (Monero jsonRpc) =
    either fail (return . (+ (-1))) . J.parseEither (J..: "count") =<< jsonRpcRequest jsonRpc "getblockcount" J.Null

  getBlockByHeight (Monero jsonRpc) blockHeight = do
    blockInfo <- jsonRpcRequest jsonRpc "getblock" (J.Object [("height", J.toJSON blockHeight)])
    J.Object blockHeaderFields <- either fail return $ J.parseEither (J..: "block_header") blockInfo
    Just (J.Bool False) <- return $ HML.lookup "orphan_status" blockHeaderFields
    Just blockHash <- return $ HML.lookup "hash" blockHeaderFields
    Just blockDifficulty <- return $ HML.lookup "difficulty" blockHeaderFields
    Just blockReward <- return $ HML.lookup "reward" blockHeaderFields
    Just blockSize <- return $ HML.lookup "block_size" blockHeaderFields
    blockJsonFields <- either fail return $ ((J.eitherDecode' . BL.fromStrict . T.encodeUtf8) =<<) $ J.parseEither (J..: "json") blockInfo
    txHashes <- either fail return $ J.parseEither (J..: "tx_hashes") blockJsonFields
    transactions <- if V.null txHashes
      then return V.empty
      else do
        transactionsJsons <- either fail return . J.parseEither (J..: "txs_as_json") =<< nonJsonRpcRequest jsonRpc "/gettransactions" (J.Object [("txs_hashes", J.Array txHashes), ("decode_as_json", J.Bool True)])
        V.forM (V.zip txHashes transactionsJsons) $ \(txHash, transactionJson) -> do
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
      J.Success block -> return $ unwrapMoneroBlock block
      J.Error err -> fail err
