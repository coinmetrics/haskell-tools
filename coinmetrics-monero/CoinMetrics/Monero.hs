{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Monero
	( Monero(..)
	, MoneroBlock(..)
	, MoneroTransaction(..)
	, MoneroTransactionInput(..)
	, MoneroTransactionOutput(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Lazy as HML
import Data.Maybe
import GHC.Generics(Generic)
import Data.Int
import Data.Proxy
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Unified
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

newtype Monero = Monero JsonRpc

data MoneroBlock = MoneroBlock
	{ mb_height :: {-# UNPACK #-} !Int64
	, mb_hash :: !B.ByteString
	, mb_major_version :: {-# UNPACK #-} !Int64
	, mb_minor_version :: {-# UNPACK #-} !Int64
	, mb_difficulty :: {-# UNPACK #-} !Int64
	, mb_reward :: {-# UNPACK #-} !Int64
	, mb_timestamp :: {-# UNPACK #-} !Int64
	, mb_nonce :: {-# UNPACK #-} !Int64
	, mb_size :: {-# UNPACK #-} !Int64
	, mb_miner_tx :: !MoneroTransaction
	, mb_transactions :: !(V.Vector MoneroTransaction)
	} deriving Generic

instance Schemable MoneroBlock

instance J.FromJSON MoneroBlock where
	parseJSON = J.withObject "monero block" $ \fields -> MoneroBlock
		<$> (fields J..: "height")
		<*> (decodeHexBytes =<< fields J..: "hash")
		<*> (fields J..: "major_version")
		<*> (fields J..: "minor_version")
		<*> (fields J..: "difficulty")
		<*> (fields J..: "reward")
		<*> (fields J..: "timestamp")
		<*> (fields J..: "nonce")
		<*> (fields J..: "size")
		<*> (fields J..: "miner_tx")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema MoneroBlock where
	schema = genericAvroSchema
instance A.ToAvro MoneroBlock where
	toAvro = genericToAvro
instance ToPostgresText MoneroBlock

instance IsUnifiedBlock MoneroBlock

data MoneroTransaction = MoneroTransaction
	{ mt_hash :: !(Maybe B.ByteString)
	, mt_version :: {-# UNPACK #-} !Int64
	, mt_unlock_time :: {-# UNPACK #-} !Int64
	, mt_vin :: !(V.Vector MoneroTransactionInput)
	, mt_vout :: !(V.Vector MoneroTransactionOutput)
	, mt_extra :: !B.ByteString
	, mt_fee :: !(Maybe Int64)
	} deriving Generic

instance Schemable MoneroTransaction
instance SchemableField MoneroTransaction

instance J.FromJSON MoneroTransaction where
	parseJSON = J.withObject "monero transaction" $ \fields -> MoneroTransaction
		<$> (traverse decodeHexBytes =<< fields J..:? "hash")
		<*> (fields J..: "version")
		<*> (fields J..: "unlock_time")
		<*> (fields J..: "vin")
		<*> (fields J..: "vout")
		<*> (B.pack <$> fields J..: "extra")
		<*> (parseFee fields)
		where
			parseFee fields = do
				maybeRctSignatures <- fields J..:? "rct_signatures"
				case maybeRctSignatures of
					Just rctSignatures -> rctSignatures J..:? "txnFee"
					Nothing -> return Nothing

instance A.HasAvroSchema MoneroTransaction where
	schema = genericAvroSchema
instance A.ToAvro MoneroTransaction where
	toAvro = genericToAvro
instance ToPostgresText MoneroTransaction

data MoneroTransactionInput = MoneroTransactionInput
	{ mti_amount :: !(Maybe Int64)
	, mti_k_image :: !(Maybe B.ByteString)
	, mti_key_offsets :: !(V.Vector Int64)
	, mti_height :: !(Maybe Int64)
	} deriving Generic

instance Schemable MoneroTransactionInput
instance SchemableField MoneroTransactionInput

instance J.FromJSON MoneroTransactionInput where
	parseJSON = J.withObject "monero transaction input" $ \fields -> do
		maybeKey <- fields J..:? "key"
		maybeGen <- fields J..:? "gen"
		MoneroTransactionInput
			<$> (traverse (J..: "amount") maybeKey)
			<*> (traverse (decodeHexBytes <=< (J..: "k_image")) maybeKey)
			<*> (fromMaybe V.empty <$> traverse (J..: "key_offsets") maybeKey)
			<*> (traverse (J..: "height") maybeGen)

instance A.HasAvroSchema MoneroTransactionInput where
	schema = genericAvroSchema
instance A.ToAvro MoneroTransactionInput where
	toAvro = genericToAvro
instance ToPostgresText MoneroTransactionInput

data MoneroTransactionOutput = MoneroTransactionOutput
	{ mto_amount :: !Int64
	, mto_key :: !B.ByteString
	} deriving Generic

instance Schemable MoneroTransactionOutput
instance SchemableField MoneroTransactionOutput

instance J.FromJSON MoneroTransactionOutput where
	parseJSON = J.withObject "monero transaction output" $ \fields -> MoneroTransactionOutput
		<$> (fields J..: "amount")
		<*> (decodeHexBytes =<< (J..: "key") =<< fields J..: "target")

instance A.HasAvroSchema MoneroTransactionOutput where
	schema = genericAvroSchema
instance A.ToAvro MoneroTransactionOutput where
	toAvro = genericToAvro
instance ToPostgresText MoneroTransactionOutput

instance BlockChain Monero where
	type Block Monero = MoneroBlock

	getBlockChainInfo _ = BlockChainInfo
		{ bci_init = \BlockChainParams
			{ bcp_httpManager = httpManager
			, bcp_httpRequest = httpRequest
			} -> return $ Monero $ newJsonRpc httpManager httpRequest Nothing
		, bci_defaultApiUrl = "http://127.0.0.1:18081/json_rpc"
		, bci_defaultBeginBlock = 0
		, bci_defaultEndBlock = -60 -- conservative rewrite limit
		, bci_schemas = standardBlockChainSchemas
			(schemaOf (Proxy :: Proxy MoneroBlock))
			[ schemaOf (Proxy :: Proxy MoneroTransactionInput)
			, schemaOf (Proxy :: Proxy MoneroTransactionOutput)
			, schemaOf (Proxy :: Proxy MoneroTransaction)
			]
			"CREATE TABLE \"monero\" OF \"MoneroBlock\" (PRIMARY KEY (\"height\"));"
		}

	getCurrentBlockHeight (Monero jsonRpc) =
		either fail (return . (+ (-1))) . J.parseEither (J..: "count") =<< jsonRpcRequest jsonRpc "getblockcount" J.Null

	getBlockByHeight (Monero jsonRpc) blockHeight = do
		blockInfo <- jsonRpcRequest jsonRpc "getblock" (J.Object [("height", J.toJSON blockHeight)])
		J.Object blockHeaderFields <- either fail return $ J.parseEither (J..: "block_header") blockInfo
		Just (J.Bool False) <- return $ HML.lookup "orphan_status" blockHeaderFields
		Just blockHash <- return $ HML.lookup "hash" blockHeaderFields
		Just blockDifficulty <- return $ HML.lookup "difficulty" blockHeaderFields
		Just blockReward <- return $ HML.lookup "reward" blockHeaderFields
		Just blockSize <- return $ HML.lookup "block_size" blockHeaderFields
		blockJsonFields <- either fail return $ ((J.eitherDecode' . BL.fromStrict . T.encodeUtf8) =<<) $ J.parseEither (J..: "json") blockInfo
		txHashes <- either fail return $ J.parseEither (J..: "tx_hashes") blockJsonFields
		transactions <- if V.null txHashes
			then return V.empty
			else do
				transactionsJsons <- either fail return . J.parseEither (J..: "txs_as_json") =<< nonJsonRpcRequest jsonRpc "/gettransactions" (J.Object [("txs_hashes", J.Array txHashes), ("decode_as_json", J.Bool True)])
				V.forM (V.zip txHashes transactionsJsons) $ \(txHash, transactionJson) -> do
					transactionFields <- either fail return $ J.eitherDecode' $ BL.fromStrict $ T.encodeUtf8 transactionJson
					return $ J.Object $ HML.insert "hash" txHash transactionFields
		let jsonBlock = J.Object
			$ HML.insert "height" (J.toJSON blockHeight)
			$ HML.insert "hash" blockHash
			$ HML.insert "difficulty" blockDifficulty
			$ HML.insert "reward" blockReward
			$ HML.insert "size" blockSize
			$ HML.insert "transactions" (J.Array transactions)
			blockJsonFields
		case J.fromJSON jsonBlock of
			J.Success block -> return block
			J.Error err -> fail err

	blockHeightFieldName _ = "height"
