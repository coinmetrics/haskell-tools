{-# LANGUAGE DeriveGeneric, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Tezos
  ( Tezos(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import Data.Int
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Time.Clock.POSIX as Time
import qualified Data.Time.ISO8601 as Time
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Tezos = Tezos
  { tezos_httpManager :: !H.Manager
  , tezos_httpRequest :: !H.Request
  }

data TezosBlock = TezosBlock
  { tb_level :: {-# UNPACK #-} !Int64
  , tb_hash :: !T.Text
  , tb_timestamp :: {-# UNPACK #-} !Int64
  }

instance IsBlock TezosBlock where
  getBlockHeight = tb_level
  getBlockTimestamp = Time.posixSecondsToUTCTime . fromIntegral . tb_timestamp

newtype TezosBlockWrapper = TezosBlockWrapper
  { unwrapTezosBlock :: TezosBlock
  }

instance J.FromJSON TezosBlockWrapper where
  parseJSON = J.withObject "tezos block" $ \fields -> do
    headerData <- fields J..: "header"
    fmap TezosBlockWrapper $ TezosBlock
      <$> (headerData J..: "level")
      <*> (fields J..: "hash")
      <*> (decodeDate =<< headerData J..: "timestamp")

decodeDate :: T.Text -> J.Parser Int64
decodeDate (T.unpack -> t) = case Time.parseISO8601 t of
  Just date -> return $ floor $ Time.utcTimeToPOSIXSeconds date
  Nothing -> fail $ "wrong date: " <> t

genSchemaInstances [''TezosBlock]
genFlattenedTypes "level" [| tb_level |] [("block", ''TezosBlock)]

instance BlockChain Tezos where
  type Block Tezos = TezosBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Tezos
      { tezos_httpManager = httpManager
      , tezos_httpRequest = httpRequest
      }
    , bci_defaultApiUrl = "http://127.0.0.1:8732/"
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = 0
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy TezosBlock))
      [
      ]
      "CREATE TABLE \"tezos\" OF \"TezosBlock\" (PRIMARY KEY (\"level\"));"
    , bci_flattenSuffixes = ["blocks"]
    , bci_flattenPack = let
      f (blocks) =
        [ SomeBlocks (blocks :: [TezosBlock_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight Tezos
    { tezos_httpManager = httpManager
    , tezos_httpRequest = httpRequest
    } =
    tryWithRepeat $ either fail (return . tb_level . unwrapTezosBlock) . J.eitherDecode . H.responseBody =<< H.httpLbs httpRequest
      { H.path = "/chains/" <> mainnetChain <> "/blocks/head"
      } httpManager

  getBlockByHeight Tezos
    { tezos_httpManager = httpManager
    , tezos_httpRequest = httpRequest
    } blockHeight = do
    TezosBlockWrapper TezosBlock
      { tb_level = headBlockLevel
      , tb_hash = headBlockHash
      } <- tryWithRepeat $ either fail return . J.eitherDecode . H.responseBody =<< H.httpLbs httpRequest
      { H.path = "/chains/" <> mainnetChain <> "/blocks/head"
      } httpManager
    tryWithRepeat $ either fail (return . unwrapTezosBlock) . J.eitherDecode . H.responseBody =<< H.httpLbs httpRequest
      { H.path = "/chains/" <> mainnetChain <> "/blocks/" <> T.encodeUtf8 headBlockHash <> "~" <> (T.encodeUtf8 $ T.pack $ show $ headBlockLevel - blockHeight)
      } httpManager

  blockHeightFieldName _ = "level"

mainnetChain :: B.ByteString
mainnetChain = "NetXdQprcVkpaWU"
