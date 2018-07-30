{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies #-}

module CoinMetrics.Neo
	( Neo(..)
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
import Data.Proxy
import Data.Scientific
import qualified Data.Text as T
import qualified Data.Vector as V

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Unified
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
		<$> (decode0xHexBytes =<< fields J..: "hash")
		<*> (fields J..: "size")
		<*> (fields J..: "time")
		<*> (fields J..: "index")
		<*> (fields J..: "tx")

instance A.HasAvroSchema NeoBlock where
	schema = genericAvroSchema
instance A.ToAvro NeoBlock where
	toAvro = genericToAvro
instance ToPostgresText NeoBlock

instance IsUnifiedBlock NeoBlock

data NeoTransaction = NeoTransaction
	{ et_txid :: !B.ByteString
	, et_size :: {-# UNPACK #-} !Int64
	, et_type :: !T.Text
	, et_vin :: !(V.Vector NeoTransactionInput)
	, et_vout :: !(V.Vector NeoTransactionOutput)
	, et_sys_fee :: !Scientific
	, et_net_fee :: !Scientific
	} deriving Generic

instance Schemable NeoTransaction
instance SchemableField NeoTransaction

instance J.FromJSON NeoTransaction where
	parseJSON = J.withObject "neo transaction" $ \fields -> NeoTransaction
		<$> (decode0xHexBytes =<< fields J..: "txid")
		<*> (fields J..: "size")
		<*> (fields J..: "type")
		<*> (fields J..: "vin")
		<*> (fields J..: "vout")
		<*> (decodeReadStr =<< fields J..: "sys_fee")
		<*> (decodeReadStr =<< fields J..: "net_fee")

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
		<$> (decode0xHexBytes =<< fields J..: "txid")
		<*> (fields J..: "vout")

instance A.HasAvroSchema NeoTransactionInput where
	schema = genericAvroSchema
instance A.ToAvro NeoTransactionInput where
	toAvro = genericToAvro
instance ToPostgresText NeoTransactionInput

data NeoTransactionOutput = NeoTransactionOutput
	{ nti_asset :: !B.ByteString
	, nti_value :: !Scientific
	, nti_address :: !T.Text
	} deriving Generic

instance Schemable NeoTransactionOutput
instance SchemableField NeoTransactionOutput

instance J.FromJSON NeoTransactionOutput where
	parseJSON = J.withObject "neo transaction output" $ \fields -> NeoTransactionOutput
		<$> (decode0xHexBytes =<< fields J..: "asset")
		<*> (decodeReadStr =<< fields J..: "value")
		<*> (fields J..: "address")

instance A.HasAvroSchema NeoTransactionOutput where
	schema = genericAvroSchema
instance A.ToAvro NeoTransactionOutput where
	toAvro = genericToAvro
instance ToPostgresText NeoTransactionOutput

instance BlockChain Neo where
	type Block Neo = NeoBlock

	getBlockChainInfo _ = BlockChainInfo
		{ bci_init = \BlockChainParams
			{ bcp_httpManager = httpManager
			, bcp_httpRequest = httpRequest
			} -> return $ Neo $ newJsonRpc httpManager httpRequest Nothing
		, bci_defaultApiUrl = "http://127.0.0.1:10332/"
		, bci_defaultBeginBlock = 0
		, bci_defaultEndBlock = -1000 -- very conservative rewrite limit
		, bci_schemas = standardBlockChainSchemas
			(schemaOf (Proxy :: Proxy NeoBlock))
			[ schemaOf (Proxy :: Proxy NeoTransactionInput)
			, schemaOf (Proxy :: Proxy NeoTransactionOutput)
			, schemaOf (Proxy :: Proxy NeoTransaction)
			]
			"CREATE TABLE \"neo\" OF \"NeoBlock\" (PRIMARY KEY (\"index\"));"
		}

	getCurrentBlockHeight (Neo jsonRpc) = (+ (-1)) <$> jsonRpcRequest jsonRpc "getblockcount" ([] :: V.Vector J.Value)

	getBlockByHeight (Neo jsonRpc) blockHeight = jsonRpcRequest jsonRpc "getblock" ([J.Number $ fromIntegral blockHeight, J.Number 1] :: V.Vector J.Value)

	blockHeightFieldName _ = "index"
