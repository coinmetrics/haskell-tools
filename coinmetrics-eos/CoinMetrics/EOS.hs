{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.EOS
	( newEos
	, EosBlock(..)
	, EosTransaction(..)
	, EosAction(..)
	, EosAuthorization(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import GHC.Generics(Generic)
import Data.Int
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

data Eos = Eos
	{ eos_httpManager :: !H.Manager
	, eos_httpRequest :: !H.Request
	}

data EosBlock = EosBlock
	{ eb_id :: !B.ByteString
	, eb_number :: {-# UNPACK #-} !Int64
	, eb_timestamp :: {-# UNPACK #-} !Int64
	, eb_producer :: !T.Text
	, eb_ref_block_prefix :: {-# UNPACK #-} !Int64
	, eb_transactions :: !(V.Vector EosTransaction)
	} deriving Generic

instance Schemable EosBlock

instance J.FromJSON EosBlock where
	parseJSON = J.withObject "eos block" $ \fields -> EosBlock
		<$> (decodeHexBytes =<< fields J..: "id")
		<*> (fields J..: "block_num")
		<*> (round . utcTimeToPOSIXSeconds . currentLocalTimeToUTC <$> fields J..: "timestamp")
		<*> (fields J..: "producer")
		<*> (fields J..: "ref_block_prefix")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema EosBlock where
	schema = genericAvroSchema
instance A.ToAvro EosBlock where
	toAvro = genericToAvro
instance ToPostgresText EosBlock

data EosTransaction = EosTransaction
	{ et_id :: !B.ByteString
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
	} deriving Generic

instance Schemable EosTransaction
instance SchemableField EosTransaction

instance J.FromJSON EosTransaction where
	parseJSON = J.withObject "eos transaction" $ \fields -> do
		trxVal <- fields J..: "trx"
		case trxVal of
			J.Object trx -> do
				trxTrans <- trx J..: "transaction"
				EosTransaction
					<$> (decodeHexBytes =<< trx J..: "id")
					<*> (fields J..: "status")
					<*> (fields J..: "cpu_usage_us")
					<*> (fields J..: "net_usage_words")
					<*> (round . utcTimeToPOSIXSeconds . currentLocalTimeToUTC <$> trxTrans J..: "expiration")
					<*> (trxTrans J..: "ref_block_num")
					<*> (trxTrans J..: "ref_block_prefix")
					<*> (trxTrans J..: "max_net_usage_words")
					<*> (trxTrans J..: "max_cpu_usage_ms")
					<*> (trxTrans J..: "delay_sec")
					<*> (trxTrans J..: "context_free_actions")
					<*> (trxTrans J..: "actions")
			_ -> EosTransaction
				"" -- id
				<$> (fields J..: "status")
				<*> (fields J..: "cpu_usage_us")
				<*> (fields J..: "net_usage_words")
				<*> (return 0)
				<*> (return 0)
				<*> (return 0)
				<*> (return 0)
				<*> (return 0)
				<*> (return 0)
				<*> (return [])
				<*> (return [])

instance A.HasAvroSchema EosTransaction where
	schema = genericAvroSchema
instance A.ToAvro EosTransaction where
	toAvro = genericToAvro
instance ToPostgresText EosTransaction

data EosAction = EosAction
	{ ea_account :: !T.Text
	, ea_name :: !T.Text
	, ea_authorization :: !(V.Vector EosAuthorization)
	, ea_data :: !B.ByteString
	} deriving Generic

instance Schemable EosAction
instance SchemableField EosAction

instance J.FromJSON EosAction where
	parseJSON = J.withObject "eos action" $ \fields -> EosAction
		<$> (fields J..: "account")
		<*> (fields J..: "name")
		<*> (fields J..: "authorization")
		<*> (decodeHexBytes =<< maybe (fields J..: "data") return =<< fields J..:? "hex_data")

instance A.HasAvroSchema EosAction where
	schema = genericAvroSchema
instance A.ToAvro EosAction where
	toAvro = genericToAvro
instance ToPostgresText EosAction

data EosAuthorization = EosAuthorization
	{ eau_actor :: !T.Text
	, eau_permission :: !T.Text
	} deriving Generic

instance Schemable EosAuthorization
instance SchemableField EosAuthorization

instance J.FromJSON EosAuthorization where
	parseJSON = J.withObject "eos authorization" $ \fields -> EosAuthorization
			<$> (fields J..: "actor")
			<*> (fields J..: "permission")

instance A.HasAvroSchema EosAuthorization where
	schema = genericAvroSchema
instance A.ToAvro EosAuthorization where
	toAvro = genericToAvro
instance ToPostgresText EosAuthorization

newEos :: H.Manager -> H.Request -> Eos
newEos = Eos

instance BlockChain Eos where
	type Block Eos = EosBlock

	getCurrentBlockHeight Eos
		{ eos_httpManager = httpManager
		, eos_httpRequest = httpRequest
		} = do
		response <- tryWithRepeat $ H.httpLbs httpRequest
			{ H.path = "/v1/chain/get_info"
			} httpManager
		either fail return $ J.parseEither (J..: "last_irreversible_block_num") =<< J.eitherDecode' (H.responseBody response)

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
		either fail return $ J.eitherDecode' $ H.responseBody response

	blockHeightFieldName _ = "number"
