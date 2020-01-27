{-|
Module: CoinMetrics.Tendermint
Description: Generic Tendermint structures useful for implementing derived blockchains, as well as Blockchain instance for generic non-decoded export.
License: MIT
-}

{-# LANGUAGE DeriveGeneric, FlexibleInstances, LambdaCase, OverloadedLists, OverloadedStrings, ScopedTypeVariables, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Tendermint
  ( Tendermint(..)
  , TendermintBlock(..)
  , TendermintBlockWrapper(..)
  , TendermintTx(..)
  ) where

import Control.Monad
import Control.Monad.Fail(MonadFail)
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.String
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import Data.Time.ISO8601
import qualified Data.Vector as V
import GHC.Generics(Generic)
import Language.Haskell.TH
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

data Tendermint tx = Tendermint
  { tendermint_httpManager :: !H.Manager
  , tendermint_httpRequest :: !H.Request
  }

data TendermintBlock tx = TendermintBlock
  { tb_height :: {-# UNPACK #-} !Int64
  , tb_hash :: {-# UNPACK #-} !HexString
  , tb_time :: {-# UNPACK #-} !Int64
  , tb_transactions :: !(V.Vector tx)
  , tb_header :: !J.Value
  }

instance HasBlockHeader (TendermintBlock tx) where
  getBlockHeader TendermintBlock
    { tb_height = height
    , tb_hash = hash
    , tb_time = timestamp
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp * 0.001
    }

newtype TendermintBlockWrapper tx = TendermintBlockWrapper
  { unwrapTendermintBlock :: TendermintBlock tx
  }

instance TendermintTx tx => J.FromJSON (TendermintBlockWrapper tx) where
  parseJSON = J.withObject "tendermint block" $ \fields -> do
    blockMeta <- fields J..: "block_meta"
    blockMetaHeader <- blockMeta J..: "header"
    fmap TendermintBlockWrapper $ TendermintBlock
      <$> (read <$> blockMetaHeader J..: "height")
      <*> ((J..: "hash") =<< (blockMeta J..: "block_id"))
      <*> (maybe (fail "wrong time") (return . floor . (* 1000) . utcTimeToPOSIXSeconds) . parseISO8601 =<< blockMetaHeader J..: "time")
      <*> (mapM decodeTendermintTx . fromMaybe V.empty =<< (J..:? "txs") =<< (J..: "data") =<< (fields J..: "block"))
      <*> (return $ J.Object blockMetaHeader)

data TendermintResult a = TendermintResult
  { tr_result :: !a
  } deriving Generic
instance J.FromJSON a => J.FromJSON (TendermintResult a) where
  parseJSON = J.genericParseJSON schemaJsonOptions

-- | Typeclass for Tendermint transaction types.
class (SchemableField tx, A.ToAvro tx, ToPostgresText tx, J.ToJSON tx) => TendermintTx tx where
  -- | Decode tendermint transaction.
  decodeTendermintTx :: MonadFail m => T.Text -> m tx

-- | Instance for Text, doing nothing.
instance TendermintTx T.Text where
  decodeTendermintTx = return

-- | Instances for tendermint block.
do
  tx <- newName "tx"
  schemaInstancesCtxDecs [varT tx] [t| TendermintBlock $(varT tx) |]

instance TendermintTx tx => BlockChain (Tendermint tx) where
  type Block (Tendermint tx) = TendermintBlock tx

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
      (schemaOf (Proxy :: Proxy (TendermintBlock tx)))
      [
      ]
      "CREATE TABLE \"tendermint\" OF \"TendermintBlock\" (PRIMARY KEY (\"height\"));"
    }

  getBlockChainNodeInfo Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/status"
      } httpManager
    result <- either fail (return . tr_result) $ J.eitherDecode' $ H.responseBody response
    version <- either fail return $ J.parseEither ((J..: "node_info") >=> (J..: "version")) result
    return BlockChainNodeInfo
      { bcni_version = version
      }

  getCurrentBlockHeight Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/block"
      } httpManager
    either fail (return . tb_height . (id :: TendermintBlock tx -> TendermintBlock tx) . unwrapTendermintBlock . tr_result) $ J.eitherDecode' $ H.responseBody response

  getBlockByHeight Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    } blockHeight = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/block?height=" <> fromString (show blockHeight)
      } httpManager
    either fail (return . unwrapTendermintBlock . tr_result) $ J.eitherDecode' $ H.responseBody response
