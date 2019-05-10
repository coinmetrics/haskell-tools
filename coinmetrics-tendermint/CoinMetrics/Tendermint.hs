{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Tendermint
  ( Tendermint(..)
  , TendermintBlock(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Int
import Data.Proxy
import Data.String
import Data.Time.Clock.POSIX
import Data.Time.ISO8601
import GHC.Generics(Generic)
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Tendermint = Tendermint
  { tendermint_httpManager :: !H.Manager
  , tendermint_httpRequest :: !H.Request
  }

data TendermintBlock = TendermintBlock
  { tb_height :: {-# UNPACK #-} !Int64
  , tb_hash :: {-# UNPACK #-} !HexString
  , tb_time :: {-# UNPACK #-} !Int64
  }

instance HasBlockHeader TendermintBlock where
  getBlockHeader TendermintBlock
    { tb_height = height
    , tb_hash = hash
    , tb_time = timestamp
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp * 0.001
    }

newtype TendermintBlockWrapper = TendermintBlockWrapper
  { unwrapTendermintBlock :: TendermintBlock
  }

instance J.FromJSON TendermintBlockWrapper where
  parseJSON = J.withObject "tendermint block" $ \fields -> do
    blockMeta <- fields J..: "block_meta"
    blockMetaHeader <- blockMeta J..: "header"
    fmap TendermintBlockWrapper $ TendermintBlock
      <$> (read <$> blockMetaHeader J..: "height")
      <*> ((J..: "hash") =<< (blockMeta J..: "block_id"))
      <*> (maybe (fail "wrong time") (return . floor . (* 1000) . utcTimeToPOSIXSeconds) . parseISO8601 =<< blockMetaHeader J..: "time")

data TendermintResult a = TendermintResult
  { tr_result :: !a
  } deriving Generic
instance J.FromJSON a => J.FromJSON (TendermintResult a) where
  parseJSON = J.genericParseJSON schemaJsonOptions

genSchemaInstances [''TendermintBlock]
genFlattenedTypes "height" [| tb_height |] [("block", ''TendermintBlock)]

instance BlockChain Tendermint where
  type Block Tendermint = TendermintBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Tendermint
        { tendermint_httpManager = httpManager
        , tendermint_httpRequest = httpRequest
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:26657/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy TendermintBlock))
      [
      ]
      "CREATE TABLE \"tendermint\" OF \"TendermintBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks"]
    , bci_flattenPack = let
      f blocks =
        [ SomeBlocks (blocks :: [TendermintBlock_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/block"
      } httpManager
    either fail (return . tb_height . unwrapTendermintBlock . tr_result) $ J.eitherDecode' $ H.responseBody response

  getBlockByHeight Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    } blockHeight = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/block?height=" <> fromString (show blockHeight)
      } httpManager
    either fail (return . unwrapTendermintBlock . tr_result) $ J.eitherDecode' $ H.responseBody response
