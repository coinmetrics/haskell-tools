{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Waves
	( newWaves
	, WavesBlock(..)
	, WavesTransaction(..)
	, WavesOrder(..)
	, WavesTransfer(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import GHC.Generics(Generic)
import Data.Int
import Data.Maybe
import Data.String
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

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
	} deriving Generic

instance Schemable WavesBlock

instance J.FromJSON WavesBlock where
	parseJSON = J.withObject "waves block" $ \fields -> WavesBlock
		<$> (fields J..: "height")
		<*> (fields J..: "version")
		<*> (fields J..: "timestamp")
		<*> ((J..: "base-target") =<< fields J..: "nxt-consensus")
		<*> (fields J..: "generator")
		<*> (fields J..: "blocksize")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema WavesBlock where
	schema = genericAvroSchema
instance A.ToAvro WavesBlock where
	toAvro = genericToAvro
instance ToPostgresText WavesBlock

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
	} deriving Generic

instance Schemable WavesTransaction
instance SchemableField WavesTransaction

instance J.FromJSON WavesTransaction where
	parseJSON = J.withObject "waves transaction" $ \fields -> WavesTransaction
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
		<*> (fields J..:? "order1")
		<*> (fields J..:? "order2")
		<*> (fields J..:? "leaseId")
		<*> (fields J..:? "alias")
		<*> (fromMaybe V.empty <$> fields J..:? "transfers")

instance A.HasAvroSchema WavesTransaction where
	schema = genericAvroSchema
instance A.ToAvro WavesTransaction where
	toAvro = genericToAvro
instance ToPostgresText WavesTransaction

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
	} deriving Generic

instance Schemable WavesOrder
instance SchemableField WavesOrder

instance J.FromJSON WavesOrder where
	parseJSON = J.withObject "waves order" $ \fields -> WavesOrder
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

instance A.HasAvroSchema WavesOrder where
	schema = genericAvroSchema
instance A.ToAvro WavesOrder where
	toAvro = genericToAvro
instance ToPostgresText WavesOrder

data WavesTransfer = WavesTransfer
	{ wtf_recipient :: !T.Text
	, wtf_amount :: {-# UNPACK #-} !Int64
	} deriving Generic

instance Schemable WavesTransfer
instance SchemableField WavesTransfer

instance J.FromJSON WavesTransfer where
	parseJSON = J.withObject "waves transfer" $ \fields -> WavesTransfer
		<$> (fields J..: "recipient")
		<*> (fields J..: "amount")

instance A.HasAvroSchema WavesTransfer where
	schema = genericAvroSchema
instance A.ToAvro WavesTransfer where
	toAvro = genericToAvro
instance ToPostgresText WavesTransfer

newWaves :: H.Manager -> H.Request -> Waves
newWaves = Waves

instance BlockChain Waves where
	type Block Waves = WavesBlock

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
		either fail return $ J.eitherDecode' $ H.responseBody response

	blockHeightFieldName _ = "height"
