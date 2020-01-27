{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Bitcoin
  ( Bitcoin(..)
  , BitcoinBlock(..)
  , BitcoinTransaction(..)
  , BitcoinVin(..)
  , BitcoinVout(..)
  ) where

import Control.Exception
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.Scientific
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import Numeric

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

newtype Bitcoin = Bitcoin JsonRpc

data BitcoinBlock = BitcoinBlock
  { bb_hash :: {-# UNPACK #-} !HexString
  , bb_size :: {-# UNPACK #-} !Int64
  , bb_strippedsize :: !(Maybe Int64)
  , bb_weight :: !(Maybe Int64)
  , bb_height :: {-# UNPACK #-} !Int64
  , bb_version :: {-# UNPACK #-} !Int64
  , bb_tx :: !(V.Vector BitcoinTransaction)
  , bb_time :: {-# UNPACK #-} !Int64
  , bb_nonce :: {-# UNPACK #-} !Int64
  , bb_difficulty :: {-# UNPACK #-} !Double
  , bb_prevHash :: {-# UNPACK #-} !HexString
  }

instance HasBlockHeader BitcoinBlock where
  getBlockHeader BitcoinBlock
    { bb_hash = hash
    , bb_height = height
    , bb_time = time
    , bb_prevHash = prevHash
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Just prevHash
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral time
    }

newtype BitcoinBlockWrapper = BitcoinBlockWrapper
  { unwrapBitcoinBlock :: BitcoinBlock
  }

instance J.FromJSON BitcoinBlockWrapper where
  parseJSON = J.withObject "bitcoin block" $ \fields -> fmap BitcoinBlockWrapper $ BitcoinBlock
    <$> (fields J..: "hash")
    <*> (fields J..: "size")
    <*> (fields J..:? "strippedsize")
    <*> (fields J..:? "weight")
    <*> (fields J..: "height")
    <*> (fields J..: "version")
    <*> (V.map unwrapBitcoinTransaction <$> fields J..: "tx")
    <*> (fields J..: "time")
    <*> (parseNonce =<< fields J..: "nonce")
    <*> (fields J..: "difficulty")
    <*> (fields J..: "previousblockhash")

data BitcoinBlockHeader = BitcoinBlockHeader
  { bbh_hash :: {-# UNPACK #-} !HexString
  , bbh_height :: {-# UNPACK #-} !Int64
  , bbh_time :: {-# UNPACK #-} !Int64
  , bbh_prevHash :: {-# UNPACK #-} !HexString
  }

instance HasBlockHeader BitcoinBlockHeader where
  getBlockHeader BitcoinBlockHeader
    { bbh_hash = hash
    , bbh_height = height
    , bbh_time = time
    , bbh_prevHash = prevHash
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Just prevHash
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral time
    }

newtype BitcoinBlockHeaderWrapper = BitcoinBlockHeaderWrapper
  { unwrapBitcoinBlockHeader :: BitcoinBlockHeader
  }

instance J.FromJSON BitcoinBlockHeaderWrapper where
  parseJSON = J.withObject "bitcoin block header" $ \fields -> fmap BitcoinBlockHeaderWrapper $ BitcoinBlockHeader
    <$> (fields J..: "hash")
    <*> (fields J..: "height")
    <*> (fields J..: "time")
    <*> (fields J..: "previousblockhash")

data BitcoinTransaction = BitcoinTransaction
  { bt_txid :: {-# UNPACK #-} !HexString
  , bt_hash :: !(Maybe HexString)
  , bt_size :: !(Maybe Int64)
  , bt_vsize :: !(Maybe Int64)
  , bt_version :: {-# UNPACK #-} !Int64
  , bt_locktime :: {-# UNPACK #-} !Int64
  , bt_vin :: !(V.Vector BitcoinVin)
  , bt_vout :: !(V.Vector BitcoinVout)
  }

newtype BitcoinTransactionWrapper = BitcoinTransactionWrapper
  { unwrapBitcoinTransaction :: BitcoinTransaction
  }

instance J.FromJSON BitcoinTransactionWrapper where
  parseJSON = J.withObject "bitcoin transaction" $ \fields -> fmap BitcoinTransactionWrapper $ BitcoinTransaction
    <$> (fields J..: "txid")
    <*> (fields J..:? "hash")
    <*> (fields J..:? "size")
    <*> (fields J..:? "vsize")
    <*> (fields J..: "version")
    <*> (fields J..: "locktime")
    <*> (fields J..: "vin")
    <*> (V.map unwrapBitcoinVout <$> fields J..: "vout")

data BitcoinVin = BitcoinVin
  { bvi_txid :: !(Maybe HexString)
  , bvi_vout :: !(Maybe Int64)
  , bvi_coinbase :: !(Maybe HexString)
  }

data BitcoinVout = BitcoinVout
  { bvo_type :: !T.Text
  , bvo_value :: {-# UNPACK #-} !Scientific
  , bvo_addresses :: !(V.Vector T.Text)
  , bvo_asm :: !T.Text
  }

newtype BitcoinVoutWrapper = BitcoinVoutWrapper
  { unwrapBitcoinVout :: BitcoinVout
  }

instance J.FromJSON BitcoinVoutWrapper where
  parseJSON = J.withObject "bitcoin vout" $ \fields -> do
    scriptPubKey <- fields J..: "scriptPubKey"
    fmap BitcoinVoutWrapper $ BitcoinVout
      <$> (scriptPubKey J..: "type")
      <*> (fields J..: "value")
      <*> fmap (fromMaybe V.empty) (scriptPubKey J..:? "addresses")
      <*> (scriptPubKey J..: "asm")

parseNonce :: J.Value -> J.Parser Int64
parseNonce = \case
  -- Bitcoin Gold returns nonce in form of hex string
  J.String (readHex . T.unpack -> [(n, "")]) -> return n
  n -> J.parseJSON n


genSchemaInstances [''BitcoinBlock, ''BitcoinTransaction, ''BitcoinVin, ''BitcoinVout]
genFlattenedTypes "height" [| bb_height |] [("block", ''BitcoinBlock), ("transaction", ''BitcoinTransaction), ("vin", ''BitcoinVin), ("vout", ''BitcoinVout)]

instance BlockChain Bitcoin where
  type Block Bitcoin = BitcoinBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return $ Bitcoin $ newJsonRpc httpManager httpRequest Nothing
    , bci_defaultApiUrls = ["http://127.0.0.1:8332/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -100 -- conservative rewrite limit
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy BitcoinBlock))
      [ schemaOf (Proxy :: Proxy BitcoinVin)
      , schemaOf (Proxy :: Proxy BitcoinVout)
      , schemaOf (Proxy :: Proxy BitcoinTransaction)
      ]
      "CREATE TABLE \"bitcoin\" OF \"BitcoinBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "vins", "vouts"]
    , bci_flattenPack = let
      f (blocks, (transactions, vins, vouts)) =
        [ SomeBlocks (blocks :: [BitcoinBlock_flattened])
        , SomeBlocks (transactions :: [BitcoinTransaction_flattened])
        , SomeBlocks (vins :: [BitcoinVin_flattened])
        , SomeBlocks (vouts :: [BitcoinVout_flattened])
        ]
      in f . mconcat . map flatten
    }

  getBlockChainNodeInfo (Bitcoin jsonRpc) = do
    networkInfoJson <- jsonRpcRequest jsonRpc "getnetworkinfo" ([] :: V.Vector J.Value)
    codedVersion <- either fail return $ J.parseEither (J..: "version") networkInfoJson
    return BlockChainNodeInfo
      { bcni_version = getVersionString codedVersion
      }

  getCurrentBlockHeight (Bitcoin jsonRpc) = (+ (-1)) <$> jsonRpcRequest jsonRpc "getblockcount" ([] :: V.Vector J.Value)

  getBlockHeaderByHeight (Bitcoin jsonRpc) blockHeight = do
    blockHash <- jsonRpcRequest jsonRpc "getblockhash" ([J.Number $ fromIntegral blockHeight] :: V.Vector J.Value)
    getBlockHeader . unwrapBitcoinBlockHeader <$> jsonRpcRequest jsonRpc "getblock" ([blockHash] :: V.Vector J.Value)

  getBlockByHeight (Bitcoin jsonRpc) blockHeight = do
    blockHash <- jsonRpcRequest jsonRpc "getblockhash" ([J.Number $ fromIntegral blockHeight] :: V.Vector J.Value)
    -- try get everything in one RPC call
    eitherBlock <- try $ unwrapBitcoinBlock <$> jsonRpcRequest jsonRpc "getblock" ([blockHash, J.Number 2] :: V.Vector J.Value)
    case eitherBlock of
      Right block -> return block
      Left SomeException {} -> do
        -- request block with transactions' hashes
        blockJson <- jsonRpcRequest jsonRpc "getblock" ([blockHash, J.Bool True] :: V.Vector J.Value)
        transactionsHashes <- either fail return $ J.parseEither (J..: "tx") blockJson
        transactions <- forM transactionsHashes $ \case
          -- some bitcoin clones return full transaction here anyhow, because fuck you
          -- well, less work needed in that case
          transaction@(J.Object {}) -> return transaction
          transactionHash@(J.String {}) -> jsonRpcRequest jsonRpc "getrawtransaction" ([transactionHash, J.Number 1] :: V.Vector J.Value)
          _ -> fail "wrong tx hash"
        fmap unwrapBitcoinBlock $ either fail return $ J.parseEither J.parseJSON $ J.Object $ HM.insert "tx" (J.Array transactions) blockJson

getVersionString :: Int -> T.Text
getVersionString n = T.pack $ showsPrec 0 v4 $ '.' : (showsPrec 0 v3 $ '.' : (showsPrec 0 v2 $ '.' : show v1)) where
  (p1, v1) = n `quotRem` 100
  (p2, v2) = p1 `quotRem` 100
  (p3, v3) = p2 `quotRem` 100
  v4 = p3 `rem` 100
