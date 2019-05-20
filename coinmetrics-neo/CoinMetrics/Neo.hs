{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies #-}

module CoinMetrics.Neo
  ( Neo(..)
  , NeoBlock(..)
  , NeoTransaction(..)
  , NeoTransactionInput(..)
  , NeoTransactionOutput(..)
  ) where

import qualified Data.Aeson as J
import Data.Int
import Data.Proxy
import Data.Scientific
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

newtype Neo = Neo JsonRpc

data NeoBlock = NeoBlock
  { nb_hash :: {-# UNPACK #-} !HexString
  , nb_size :: {-# UNPACK #-} !Int64
  , nb_time :: {-# UNPACK #-} !Int64
  , nb_index :: {-# UNPACK #-} !Int64
  , nb_tx :: !(V.Vector NeoTransaction)
  }

instance HasBlockHeader NeoBlock where
  getBlockHeader NeoBlock
    { nb_hash = hash
    , nb_index = index
    , nb_time = time
    } = BlockHeader
    { bh_height = index
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral time
    }

newtype NeoBlockWrapper = NeoBlockWrapper
  { unwrapNeoBlock :: NeoBlock
  }

instance J.FromJSON NeoBlockWrapper where
  parseJSON = J.withObject "neo block" $ \fields -> fmap NeoBlockWrapper $ NeoBlock
    <$> (decode0xHexBytes =<< fields J..: "hash")
    <*> (fields J..: "size")
    <*> (fields J..: "time")
    <*> (fields J..: "index")
    <*> (V.map unwrapNeoTransaction <$> fields J..: "tx")

data NeoTransaction = NeoTransaction
  { et_txid :: {-# UNPACK #-} !HexString
  , et_size :: {-# UNPACK #-} !Int64
  , et_type :: !T.Text
  , et_vin :: !(V.Vector NeoTransactionInput)
  , et_vout :: !(V.Vector NeoTransactionOutput)
  , et_sys_fee :: !Scientific
  , et_net_fee :: !Scientific
  }

newtype NeoTransactionWrapper = NeoTransactionWrapper
  { unwrapNeoTransaction :: NeoTransaction
  }

instance J.FromJSON NeoTransactionWrapper where
  parseJSON = J.withObject "neo transaction" $ \fields -> fmap NeoTransactionWrapper $ NeoTransaction
    <$> (decode0xHexBytes =<< fields J..: "txid")
    <*> (fields J..: "size")
    <*> (fields J..: "type")
    <*> (V.map unwrapNeoTransactionInput <$> fields J..: "vin")
    <*> (V.map unwrapNeoTransactionOutput <$> fields J..: "vout")
    <*> (decodeReadStr =<< fields J..: "sys_fee")
    <*> (decodeReadStr =<< fields J..: "net_fee")

data NeoTransactionInput = NeoTransactionInput
  { nti_txid :: {-# UNPACK #-} !HexString
  , nti_vout :: {-# UNPACK #-} !Int64
  }

newtype NeoTransactionInputWrapper = NeoTransactionInputWrapper
  { unwrapNeoTransactionInput :: NeoTransactionInput
  }

instance J.FromJSON NeoTransactionInputWrapper where
  parseJSON = J.withObject "neo transaction input" $ \fields -> fmap NeoTransactionInputWrapper $ NeoTransactionInput
    <$> (decode0xHexBytes =<< fields J..: "txid")
    <*> (fields J..: "vout")

data NeoTransactionOutput = NeoTransactionOutput
  { nto_asset :: {-# UNPACK #-} !HexString
  , nto_value :: !Scientific
  , nto_address :: !T.Text
  }

newtype NeoTransactionOutputWrapper = NeoTransactionOutputWrapper
  { unwrapNeoTransactionOutput :: NeoTransactionOutput
  }

instance J.FromJSON NeoTransactionOutputWrapper where
  parseJSON = J.withObject "neo transaction output" $ \fields -> fmap NeoTransactionOutputWrapper $ NeoTransactionOutput
    <$> (decode0xHexBytes =<< fields J..: "asset")
    <*> (decodeReadStr =<< fields J..: "value")
    <*> (fields J..: "address")


genSchemaInstances [''NeoBlock, ''NeoTransaction, ''NeoTransactionInput, ''NeoTransactionOutput]
genFlattenedTypes "index" [| nb_index |] [("block", ''NeoBlock), ("transaction", ''NeoTransaction), ("input", ''NeoTransactionInput), ("output", ''NeoTransactionOutput)]

instance BlockChain Neo where
  type Block Neo = NeoBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return $ Neo $ newJsonRpc httpManager httpRequest Nothing
    , bci_defaultApiUrls = ["http://127.0.0.1:10332/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -1000 -- very conservative rewrite limit
    , bci_heightFieldName = "index"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy NeoBlock))
      [ schemaOf (Proxy :: Proxy NeoTransactionInput)
      , schemaOf (Proxy :: Proxy NeoTransactionOutput)
      , schemaOf (Proxy :: Proxy NeoTransaction)
      ]
      "CREATE TABLE \"neo\" OF \"NeoBlock\" (PRIMARY KEY (\"index\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "inputs", "outputs"]
    , bci_flattenPack = let
      f (blocks, (transactions, inputs, outputs)) =
        [ SomeBlocks (blocks :: [NeoBlock_flattened])
        , SomeBlocks (transactions :: [NeoTransaction_flattened])
        , SomeBlocks (inputs :: [NeoTransactionInput_flattened])
        , SomeBlocks (outputs :: [NeoTransactionOutput_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight (Neo jsonRpc) = (+ (-1)) <$> jsonRpcRequest jsonRpc "getblockcount" ([] :: V.Vector J.Value)

  getBlockByHeight (Neo jsonRpc) blockHeight = unwrapNeoBlock <$> jsonRpcRequest jsonRpc "getblock" ([J.Number $ fromIntegral blockHeight, J.Number 1] :: V.Vector J.Value)
