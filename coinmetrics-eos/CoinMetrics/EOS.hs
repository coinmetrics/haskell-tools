{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.EOS
  ( Eos(..)
  , EosBlock(..)
  , EosTransaction(..)
  , EosAction(..)
  , EosAuthorization(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString.Lazy as BL
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding.Error as T
import qualified Data.Text.Lazy.Encoding as TL
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Eos = Eos
  { eos_httpManager :: !H.Manager
  , eos_httpRequest :: !H.Request
  }

data EosBlock = EosBlock
  { eb_id :: {-# UNPACK #-} !HexString
  , eb_number :: {-# UNPACK #-} !Int64
  , eb_timestamp :: {-# UNPACK #-} !Int64
  , eb_producer :: !T.Text
  , eb_ref_block_prefix :: {-# UNPACK #-} !Int64
  , eb_transactions :: !(V.Vector EosTransaction)
  }

instance HasBlockHeader EosBlock where
  getBlockHeader EosBlock
    { eb_number = number
    , eb_timestamp = timestamp
    } = BlockHeader
    { bh_height = number
    , bh_hash = mempty
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp
    }

newtype EosBlockWrapper = EosBlockWrapper
  { unwrapEosBlock :: EosBlock
  }

instance J.FromJSON EosBlockWrapper where
  parseJSON = J.withObject "eos block" $ \fields -> fmap EosBlockWrapper $ EosBlock
    <$> (fields J..: "id")
    <*> (fields J..: "block_num")
    <*> (round . utcTimeToPOSIXSeconds . currentLocalTimeToUTC <$> fields J..: "timestamp")
    <*> (fields J..: "producer")
    <*> (fields J..: "ref_block_prefix")
    <*> (V.map unwrapEosTransaction <$> fields J..: "transactions")

data EosTransaction = EosTransaction
  { et_id :: {-# UNPACK #-} !HexString
  , et_status :: !T.Text
  , et_cpu_usage_us :: {-# UNPACK #-} !Int64
  , et_net_usage_words :: {-# UNPACK #-} !Int64
  , et_expiration :: {-# UNPACK #-} !Int64
  , et_ref_block_num :: {-# UNPACK #-} !Int64
  , et_ref_block_prefix :: {-# UNPACK #-} !Int64
  , et_max_net_usage_words :: {-# UNPACK #-} !Int64
  , et_max_cpu_usage_ms :: {-# UNPACK #-} !Int64
  , et_delay_sec :: {-# UNPACK #-} !Int64
  , et_context_free_actions :: !(V.Vector EosAction)
  , et_actions :: !(V.Vector EosAction)
  }

newtype EosTransactionWrapper = EosTransactionWrapper
  { unwrapEosTransaction :: EosTransaction
  }

instance J.FromJSON EosTransactionWrapper where
  parseJSON = J.withObject "eos transaction" $ \fields -> do
    trxVal <- fields J..: "trx"
    fmap EosTransactionWrapper $ case trxVal of
      J.Object trx -> do
        trxTrans <- trx J..: "transaction"
        EosTransaction
          <$> (trx J..: "id")
          <*> (fields J..: "status")
          <*> (fields J..: "cpu_usage_us")
          <*> (fields J..: "net_usage_words")
          <*> (round . utcTimeToPOSIXSeconds . currentLocalTimeToUTC <$> trxTrans J..: "expiration")
          <*> (trxTrans J..: "ref_block_num")
          <*> (trxTrans J..: "ref_block_prefix")
          <*> (trxTrans J..: "max_net_usage_words")
          <*> (trxTrans J..: "max_cpu_usage_ms")
          <*> (trxTrans J..: "delay_sec")
          <*> (V.map unwrapEosAction <$> trxTrans J..: "context_free_actions")
          <*> (V.map unwrapEosAction <$> trxTrans J..: "actions")
      _ -> EosTransaction
        mempty -- id
        <$> (fields J..: "status")
        <*> (fields J..: "cpu_usage_us")
        <*> (fields J..: "net_usage_words")
        <*> return 0
        <*> return 0
        <*> return 0
        <*> return 0
        <*> return 0
        <*> return 0
        <*> return []
        <*> return []

data EosAction = EosAction
  { ea_account :: !T.Text
  , ea_name :: !T.Text
  , ea_authorization :: !(V.Vector EosAuthorization)
  , ea_data :: {-# UNPACK #-} !HexString
  }

newtype EosActionWrapper = EosActionWrapper
  { unwrapEosAction :: EosAction
  }

instance J.FromJSON EosActionWrapper where
  parseJSON = J.withObject "eos action" $ \fields -> fmap EosActionWrapper $ EosAction
    <$> (fields J..: "account")
    <*> (fields J..: "name")
    <*> (fields J..: "authorization")
    <*> (maybe (fields J..: "data") return =<< fields J..:? "hex_data")

data EosAuthorization = EosAuthorization
  { eau_actor :: !T.Text
  , eau_permission :: !T.Text
  }

genSchemaInstances [''EosBlock, ''EosTransaction, ''EosAction, ''EosAuthorization]
-- genFlattenedTypes "number" [| eb_number |] [("block", ''EthereumBlock), ("transaction", ''EthereumTransaction), ("log", ''EthereumLog), ("action", ''EthereumAction), ("uncle", ''EthereumUncleBlock)]

instance BlockChain Eos where
  type Block Eos = EosBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Eos
        { eos_httpManager = httpManager
        , eos_httpRequest = httpRequest
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:8888/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0 -- no need in a gap, as it uses irreversible block number
    , bci_heightFieldName = "number"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy EosBlock))
      [ schemaOf (Proxy :: Proxy EosAuthorization)
      , schemaOf (Proxy :: Proxy EosAction)
      , schemaOf (Proxy :: Proxy EosTransaction)
      ]
      "CREATE TABLE \"eos\" OF \"EosBlock\" (PRIMARY KEY (\"number\"));"
    }

  getBlockChainNodeInfo Eos
    { eos_httpManager = httpManager
    , eos_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/v1/chain/get_info"
      } httpManager
    version <- either fail return $ J.parseEither (J..: "server_version_string") =<< J.eitherDecode' (fixUtf8 $ H.responseBody response)
    return BlockChainNodeInfo
      { bcni_version = fromMaybe version $ T.stripPrefix "v" version
      }

  getCurrentBlockHeight Eos
    { eos_httpManager = httpManager
    , eos_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/v1/chain/get_info"
      } httpManager
    either fail return $ J.parseEither (J..: "last_irreversible_block_num") =<< J.eitherDecode' (fixUtf8 $ H.responseBody response)

  getBlockByHeight Eos
    { eos_httpManager = httpManager
    , eos_httpRequest = httpRequest
    } blockHeight = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/v1/chain/get_block"
      , H.requestBody = H.RequestBodyLBS $ J.encode $ J.Object
        [ ("block_num_or_id", J.Number $ fromIntegral blockHeight)
        ]
      } httpManager
    either fail (return . unwrapEosBlock) $ J.eitherDecode' $ fixUtf8 $ H.responseBody response

fixUtf8 :: BL.ByteString -> BL.ByteString
fixUtf8 = TL.encodeUtf8 . TL.decodeUtf8With T.lenientDecode
