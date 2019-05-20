{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Waves
  ( Waves(..)
  , WavesBlock(..)
  , WavesTransaction(..)
  , WavesOrder(..)
  , WavesTransfer(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import Data.Int
import Data.Maybe
import Data.Proxy
import Data.String
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Waves = Waves
  { waves_httpManager :: !H.Manager
  , waves_httpRequest :: !H.Request
  }

data WavesBlock = WavesBlock
  { wb_height :: {-# UNPACK #-} !Int64
  , wb_version :: {-# UNPACK #-} !Int64
  , wb_timestamp :: {-# UNPACK #-} !Int64
  , wb_basetarget :: {-# UNPACK #-} !Int64
  , wb_generator :: !T.Text
  , wb_blocksize :: {-# UNPACK #-} !Int64
  , wb_transactions :: !(V.Vector WavesTransaction)
  }

instance HasBlockHeader WavesBlock where
  getBlockHeader WavesBlock
    { wb_height = height
    , wb_timestamp = timestamp
    } = BlockHeader
    { bh_height = height
    , bh_hash = mempty
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp * 0.001
    }

newtype WavesBlockWrapper = WavesBlockWrapper
  { unwrapWavesBlock :: WavesBlock
  }

instance J.FromJSON WavesBlockWrapper where
  parseJSON = J.withObject "waves block" $ \fields -> fmap WavesBlockWrapper $ WavesBlock
    <$> (fields J..: "height")
    <*> (fields J..: "version")
    <*> (fields J..: "timestamp")
    <*> ((J..: "base-target") =<< fields J..: "nxt-consensus")
    <*> (fields J..: "generator")
    <*> (fields J..: "blocksize")
    <*> (V.map unwrapWavesTransaction <$> fields J..: "transactions")

data WavesTransaction = WavesTransaction
  { wt_type :: {-# UNPACK #-} !Int64
  , wt_id :: !T.Text
  , wt_timestamp :: {-# UNPACK #-} !Int64
  , wt_fee :: {-# UNPACK #-} !Int64
  , wt_version :: !(Maybe Int64)
  , wt_sender :: !(Maybe T.Text)
  , wt_recipient :: !(Maybe T.Text)
  , wt_amount :: !(Maybe Int64)
  , wt_assetId :: !(Maybe T.Text)
  , wt_feeAssetId :: !(Maybe T.Text)
  , wt_feeAsset :: !(Maybe T.Text)
  , wt_name :: !(Maybe T.Text)
  , wt_quantity :: !(Maybe Int64)
  , wt_reissuable :: !(Maybe Bool)
  , wt_decimals :: !(Maybe Int64)
  , wt_description :: !(Maybe T.Text)
  , wt_price :: !(Maybe Int64)
  , wt_buyMatcherFee :: !(Maybe Int64)
  , wt_sellMatcherFee :: !(Maybe Int64)
  , wt_order1 :: !(Maybe WavesOrder)
  , wt_order2 :: !(Maybe WavesOrder)
  , wt_leaseId :: !(Maybe T.Text)
  , wt_alias :: !(Maybe T.Text)
  , wt_transfers :: !(V.Vector WavesTransfer)
  }

newtype WavesTransactionWrapper = WavesTransactionWrapper
  { unwrapWavesTransaction :: WavesTransaction
  }

instance J.FromJSON WavesTransactionWrapper where
  parseJSON = J.withObject "waves transaction" $ \fields -> fmap WavesTransactionWrapper $ WavesTransaction
    <$> (fields J..: "type")
    <*> (fields J..: "id")
    <*> (fields J..: "timestamp")
    <*> (fields J..: "fee")
    <*> (fields J..:? "version")
    <*> (fields J..:? "sender")
    <*> (fields J..:? "recipient")
    <*> (fields J..:? "amount")
    <*> (fields J..:? "assetId")
    <*> (fields J..:? "feeAssetId")
    <*> (fields J..:? "feeAsset")
    <*> (fields J..:? "name")
    <*> (fields J..:? "quantity")
    <*> (fields J..:? "reissuable")
    <*> (fields J..:? "decimals")
    <*> (fields J..:? "description")
    <*> (fields J..:? "price")
    <*> (fields J..:? "buyMatcherFee")
    <*> (fields J..:? "sellMatcherFee")
    <*> (fmap unwrapWavesOrder <$> fields J..:? "order1")
    <*> (fmap unwrapWavesOrder <$> fields J..:? "order2")
    <*> (fields J..:? "leaseId")
    <*> (fields J..:? "alias")
    <*> (V.map unwrapWavesTransfer . fromMaybe V.empty <$> fields J..:? "transfers")

data WavesOrder = WavesOrder
  { wo_id :: !T.Text
  , wo_sender :: !T.Text
  , wo_matcherPublicKey :: !T.Text
  , wo_amountAsset :: !(Maybe T.Text)
  , wo_priceAsset :: !(Maybe T.Text)
  , wo_orderType :: !T.Text
  , wo_price :: {-# UNPACK #-} !Int64
  , wo_amount :: {-# UNPACK #-} !Int64
  , wo_timestamp :: {-# UNPACK #-} !Int64
  , wo_expiration :: {-# UNPACK #-} !Int64
  , wo_matcherFee :: {-# UNPACK #-} !Int64
  }

newtype WavesOrderWrapper = WavesOrderWrapper
  { unwrapWavesOrder :: WavesOrder
  }

instance J.FromJSON WavesOrderWrapper where
  parseJSON = J.withObject "waves order" $ \fields -> fmap WavesOrderWrapper $ WavesOrder
    <$> (fields J..: "id")
    <*> (fields J..: "sender")
    <*> (fields J..: "matcherPublicKey")
    <*> ((J..: "amountAsset") =<< fields J..: "assetPair")
    <*> ((J..: "priceAsset") =<< fields J..: "assetPair")
    <*> (fields J..: "orderType")
    <*> (fields J..: "price")
    <*> (fields J..: "amount")
    <*> (fields J..: "timestamp")
    <*> (fields J..: "expiration")
    <*> (fields J..: "matcherFee")

data WavesTransfer = WavesTransfer
  { wtf_recipient :: !T.Text
  , wtf_amount :: {-# UNPACK #-} !Int64
  }

newtype WavesTransferWrapper = WavesTransferWrapper
  { unwrapWavesTransfer :: WavesTransfer
  }

instance J.FromJSON WavesTransferWrapper where
  parseJSON = J.withObject "waves transfer" $ \fields -> fmap WavesTransferWrapper $ WavesTransfer
    <$> (fields J..: "recipient")
    <*> (fields J..: "amount")


genSchemaInstances [''WavesBlock, ''WavesTransaction, ''WavesOrder, ''WavesTransfer]


instance BlockChain Waves where
  type Block Waves = WavesBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Waves
        { waves_httpManager = httpManager
        , waves_httpRequest = httpRequest
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:6869/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0 -- PoS
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy WavesBlock))
      [ schemaOf (Proxy :: Proxy WavesTransfer)
      , schemaOf (Proxy :: Proxy WavesOrder)
      , schemaOf (Proxy :: Proxy WavesTransaction)
      ]
      "CREATE TABLE \"waves\" OF \"WavesBlock\" (PRIMARY KEY (\"height\"));"
    }

  getCurrentBlockHeight Waves
    { waves_httpManager = httpManager
    , waves_httpRequest = httpRequest
    } = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/blocks/height"
      } httpManager
    either fail return $ J.parseEither (J..: "height") =<< J.eitherDecode' (H.responseBody response)

  getBlockByHeight Waves
    { waves_httpManager = httpManager
    , waves_httpRequest = httpRequest
    } blockHeight = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/blocks/at/" <> fromString (show blockHeight)
      } httpManager
    either fail (return . unwrapWavesBlock) $ J.eitherDecode' $ H.responseBody response
