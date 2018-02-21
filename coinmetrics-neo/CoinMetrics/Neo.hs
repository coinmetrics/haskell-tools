{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies #-}

module CoinMetrics.Neo
	( newNeo
	, NeoBlock(..)
	, NeoTransaction(..)
	, NeoTransactionInput(..)
	, NeoTransactionOutput(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import GHC.Generics(Generic)
import Data.Int
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

newtype Neo = Neo JsonRpc

data NeoBlock = NeoBlock
	{ nb_hash :: !B.ByteString
	, nb_size :: {-# UNPACK #-} !Int64
	, nb_time :: {-# UNPACK #-} !Int64
	, nb_index :: {-# UNPACK #-} !Int64
	, nb_tx :: !(V.Vector NeoTransaction)
	} deriving Generic

instance Schemable NeoBlock

instance J.FromJSON NeoBlock where
	parseJSON = J.withObject "neo block" $ \fields -> NeoBlock
		<$> (decodeHexBytes =<< fields J..: "Hash")
		<*> (fields J..: "Size")
		<*> (fields J..: "Time")
		<*> (fields J..: "Index")
		<*> (fields J..: "Tx")

instance A.HasAvroSchema NeoBlock where
	schema = genericAvroSchema
instance A.ToAvro NeoBlock where
	toAvro = genericToAvro
instance ToPostgresText NeoBlock

data NeoTransaction = NeoTransaction
	{ et_txid :: !B.ByteString
	, et_size :: {-# UNPACK #-} !Int64
	, et_type :: !T.Text
	, et_vin :: !(V.Vector NeoTransactionInput)
	, et_vout :: !(V.Vector NeoTransactionOutput)
	, et_sys_fee :: !Integer
	, et_net_fee :: !Integer
	} deriving Generic

instance Schemable NeoTransaction
instance SchemableField NeoTransaction

instance J.FromJSON NeoTransaction where
	parseJSON = J.withObject "neo transaction" $ \fields -> NeoTransaction
		<$> (decodeHexBytes =<< fields J..: "Txid")
		<*> (fields J..: "Size")
		<*> (fields J..: "Type")
		<*> (fields J..: "Vin")
		<*> (fields J..: "Vout")
		<*> (decodeDecNumberStr =<< fields J..: "Sys_fee")
		<*> (decodeDecNumberStr =<< fields J..: "Net_fee")

instance A.HasAvroSchema NeoTransaction where
	schema = genericAvroSchema
instance A.ToAvro NeoTransaction where
	toAvro = genericToAvro
instance ToPostgresText NeoTransaction

data NeoTransactionInput = NeoTransactionInput
	{ nti_txid :: !B.ByteString
	, nti_vout :: {-# UNPACK #-} !Int64
	} deriving Generic

instance Schemable NeoTransactionInput
instance SchemableField NeoTransactionInput

instance J.FromJSON NeoTransactionInput where
	parseJSON = J.withObject "neo transaction input" $ \fields -> NeoTransactionInput
		<$> (decodeHexBytes =<< fields J..: "Txid")
		<*> (fields J..: "Vout")

instance A.HasAvroSchema NeoTransactionInput where
	schema = genericAvroSchema
instance A.ToAvro NeoTransactionInput where
	toAvro = genericToAvro
instance ToPostgresText NeoTransactionInput

data NeoTransactionOutput = NeoTransactionOutput
	{ nti_asset :: !B.ByteString
	, nti_value :: !Integer
	, nti_address :: !T.Text
	} deriving Generic

instance Schemable NeoTransactionOutput
instance SchemableField NeoTransactionOutput

instance J.FromJSON NeoTransactionOutput where
	parseJSON = J.withObject "neo transaction output" $ \fields -> NeoTransactionOutput
		<$> (decodeHexBytes =<< fields J..: "Asset")
		<*> (decodeDecNumberStr =<< fields J..: "Value")
		<*> (fields J..: "Address")

instance A.HasAvroSchema NeoTransactionOutput where
	schema = genericAvroSchema
instance A.ToAvro NeoTransactionOutput where
	toAvro = genericToAvro
instance ToPostgresText NeoTransactionOutput

newNeo :: H.Manager -> H.Request -> Neo
newNeo httpManager httpRequest = Neo $ newJsonRpc httpManager httpRequest Nothing

instance BlockChain Neo where
	type Block Neo = NeoBlock
	type Transaction Neo = NeoTransaction

	getCurrentBlockHeight (Neo jsonRpc) = jsonRpcRequest jsonRpc "getblockcount" []

	getBlockByHeight (Neo jsonRpc) blockHeight = jsonRpcRequest jsonRpc "getblock" [J.Number $ fromIntegral blockHeight, J.Number 1]

	blockHeightFieldName _ = "index"
