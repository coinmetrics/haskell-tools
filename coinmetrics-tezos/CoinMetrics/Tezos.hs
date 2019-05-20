{-# LANGUAGE DeriveGeneric, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Tezos
  ( Tezos(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Time.Clock.POSIX as Time
import qualified Data.Time.ISO8601 as Time
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
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
  , tb_baker :: !(Maybe T.Text)
  , tb_operations :: !(V.Vector TezosOperation)
  , tb_balanceUpdates :: !(V.Vector TezosBalanceUpdate)
  }

instance HasBlockHeader TezosBlock where
  getBlockHeader TezosBlock
    { tb_level = level
    , tb_hash = hash
    , tb_timestamp = timestamp
    } = BlockHeader
    { bh_height = level
    , bh_hash = HexString $ BS.toShort $ T.encodeUtf8 hash
    , bh_prevHash = Nothing
    , bh_timestamp = Time.posixSecondsToUTCTime $ fromIntegral timestamp
    }

newtype TezosBlockWrapper = TezosBlockWrapper
  { unwrapTezosBlock :: TezosBlock
  }

instance J.FromJSON TezosBlockWrapper where
  parseJSON = J.withObject "tezos block" $ \fields -> do
    headerData <- fields J..: "header"
    metadata <- fields J..: "metadata"
    fmap TezosBlockWrapper $ TezosBlock
      <$> (headerData J..: "level")
      <*> (fields J..: "hash")
      <*> (decodeDate =<< headerData J..: "timestamp")
      <*> (metadata J..:? "baker")
      <*> (V.map unwrapTezosOperation . V.concat <$> fields J..: "operations")
      <*> (V.map unwrapTezosBalanceUpdate . fromMaybe V.empty <$> metadata J..:? "balance_updates")

data TezosOperation = TezosOperation
  { to_hash :: !T.Text
  , to_items :: !(V.Vector TezosOperationItem)
  }

newtype TezosOperationWrapper = TezosOperationWrapper
  { unwrapTezosOperation :: TezosOperation
  }

instance J.FromJSON TezosOperationWrapper where
  parseJSON = J.withObject "tezos operation" $ \fields -> do
    fmap TezosOperationWrapper $ TezosOperation
      <$> (fields J..: "hash")
      <*> (V.map unwrapTezosOperationItem <$> fields J..: "contents")

data TezosOperationItem = TezosOperationItem
  { toi_kind :: !T.Text
  , toi_level :: !(Maybe Int64)
  , toi_source :: !(Maybe T.Text)
  , toi_fee :: !(Maybe Int64)
  , toi_counter :: !(Maybe Int64)
  , toi_gasLimit :: !(Maybe Int64)
  , toi_storageLimit :: !(Maybe Int64)
  , toi_amount :: !(Maybe Int64)
  , toi_destination :: !(Maybe T.Text)
  , toi_publicKey :: !(Maybe T.Text)
  , toi_managerPubkey :: !(Maybe T.Text)
  , toi_balance :: !(Maybe Int64)
  , toi_balanceUpdates :: !(V.Vector TezosBalanceUpdate)
  , toi_resultStatus :: !(Maybe T.Text)
  , toi_resultBalanceUpdates :: !(V.Vector TezosBalanceUpdate)
  , toi_resultConsumedGas :: !(Maybe Int64)
  , toi_raw :: !J.Value
  }

newtype TezosOperationItemWrapper = TezosOperationItemWrapper
  { unwrapTezosOperationItem :: TezosOperationItem
  }

instance J.FromJSON TezosOperationItemWrapper where
  parseJSON = J.withObject "tezos operation item" $ \fields -> do
    metadata <- fields J..: "metadata"
    maybeResult <- metadata J..:? "operation_result"
    fmap TezosOperationItemWrapper $ TezosOperationItem
      <$> (fields J..: "kind")
      <*> (fields J..:? "level")
      <*> (fields J..:? "source")
      <*> (traverse decodeReadStr =<< fields J..:? "fee")
      <*> (traverse decodeReadStr =<< fields J..:? "counter")
      <*> (traverse decodeReadStr =<< fields J..:? "gas_limit")
      <*> (traverse decodeReadStr =<< fields J..:? "storage_limit")
      <*> (traverse decodeReadStr =<< fields J..:? "amount")
      <*> (fields J..:? "destination")
      <*> (fields J..:? "public_key")
      <*> (fields J..:? "managerPubkey")
      <*> (traverse decodeReadStr =<< fields J..:? "balance")
      <*> (V.map unwrapTezosBalanceUpdate . fromMaybe V.empty <$> metadata J..:? "balance_updates")
      <*> (traverse (J..: "status") maybeResult)
      <*> (V.map unwrapTezosBalanceUpdate . fromMaybe V.empty . join <$> traverse (J..:? "balance_updates") maybeResult)
      <*> (traverse decodeReadStr . join =<< traverse (J..:? "consumed_gas") maybeResult)
      <*> (return $ J.Object fields)

data TezosBalanceUpdate = TezosBalanceUpdate
  { tbu_kind :: !T.Text
  , tbu_change :: {-# UNPACK #-} !Int64
  , tbu_contract :: !(Maybe T.Text)
  , tbu_category :: !(Maybe T.Text)
  , tbu_delegate :: !(Maybe T.Text)
  , tbu_level :: !(Maybe Int64)
  }

newtype TezosBalanceUpdateWrapper = TezosBalanceUpdateWrapper
  { unwrapTezosBalanceUpdate :: TezosBalanceUpdate
  }

instance J.FromJSON TezosBalanceUpdateWrapper where
  parseJSON = J.withObject "tezos balance update" $ \fields -> do
    fmap TezosBalanceUpdateWrapper $ TezosBalanceUpdate
      <$> (fields J..: "kind")
      <*> (decodeReadStr =<< fields J..: "change")
      <*> (fields J..:? "contract")
      <*> (fields J..:? "category")
      <*> (fields J..:? "delegate")
      <*> (fields J..:? "level")

decodeDate :: T.Text -> J.Parser Int64
decodeDate (T.unpack -> t) = case Time.parseISO8601 t of
  Just date -> return $ floor $ Time.utcTimeToPOSIXSeconds date
  Nothing -> fail $ "wrong date: " <> t

genSchemaInstances [''TezosBlock, ''TezosOperation, ''TezosOperationItem, ''TezosBalanceUpdate]


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
    , bci_defaultApiUrls = ["http://127.0.0.1:8732/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "level"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy TezosBlock))
      [ schemaOf (Proxy :: Proxy TezosBalanceUpdate)
      , schemaOf (Proxy :: Proxy TezosOperationItem)
      , schemaOf (Proxy :: Proxy TezosOperation)
      ]
      "CREATE TABLE \"tezos\" OF \"TezosBlock\" (PRIMARY KEY (\"level\"));"
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

mainnetChain :: B.ByteString
mainnetChain = "NetXdQprcVkpaWU"
