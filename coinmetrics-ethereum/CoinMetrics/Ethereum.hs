{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
	( newEthereum
	, EthereumBlock(..)
	, EthereumTransaction(..)
	, EthereumLog(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import qualified Data.HashMap.Lazy as HML
import Data.Maybe
import GHC.Generics(Generic)
import Data.Int
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Ethereum.Util
import CoinMetrics.JsonRpc
import CoinMetrics.Schema
import CoinMetrics.Schema.Avro
import CoinMetrics.Schema.Postgres

newtype Ethereum = Ethereum JsonRpc

data EthereumBlock = EthereumBlock
	{ eb_number :: {-# UNPACK #-} !Int64
	, eb_hash :: !B.ByteString
	, eb_parentHash :: !B.ByteString
	, eb_nonce :: !B.ByteString
	, eb_sha3Uncles :: !B.ByteString
	, eb_logsBloom :: !B.ByteString
	, eb_transactionsRoot :: !B.ByteString
	, eb_stateRoot :: !B.ByteString
	, eb_receiptsRoot :: !B.ByteString
	, eb_miner :: !B.ByteString
	, eb_difficulty :: !Integer
	, eb_totalDifficulty :: !Integer
	, eb_extraData :: !B.ByteString
	, eb_size :: {-# UNPACK #-} !Int64
	, eb_gasLimit :: {-# UNPACK #-} !Int64
	, eb_gasUsed :: {-# UNPACK #-} !Int64
	, eb_timestamp :: {-# UNPACK #-} !Int64
	, eb_transactions :: !(V.Vector (Transaction Ethereum))
	, eb_uncles :: !(V.Vector B.ByteString)
	} deriving Generic

instance Schemable EthereumBlock

instance J.FromJSON EthereumBlock where
	parseJSON = J.withObject "block" $ \fields -> EthereumBlock
		<$> (decodeHexNumber    =<< fields J..: "number")
		<*> (decodeHexBytes     =<< fields J..: "hash")
		<*> (decodeHexBytes     =<< fields J..: "parentHash")
		<*> (decodeHexBytes     =<< fields J..: "nonce")
		<*> (decodeHexBytes     =<< fields J..: "sha3Uncles")
		<*> (decodeHexBytes     =<< fields J..: "logsBloom")
		<*> (decodeHexBytes     =<< fields J..: "transactionsRoot")
		<*> (decodeHexBytes     =<< fields J..: "stateRoot")
		<*> (decodeHexBytes     =<< fields J..: "receiptsRoot")
		<*> (decodeHexBytes     =<< fields J..: "miner")
		<*> (decodeHexNumber    =<< fields J..: "difficulty")
		<*> (decodeHexNumber    =<< fields J..: "totalDifficulty")
		<*> (decodeHexBytes     =<< fields J..: "extraData")
		<*> (decodeHexNumber    =<< fields J..: "size")
		<*> (decodeHexNumber    =<< fields J..: "gasLimit")
		<*> (decodeHexNumber    =<< fields J..: "gasUsed")
		<*> (decodeHexNumber    =<< fields J..: "timestamp")
		<*> (                       fields J..: "transactions")
		<*> (V.mapM decodeHexBytes =<< fields J..: "uncles")

instance A.HasAvroSchema EthereumBlock where
	schema = genericAvroSchema
instance A.ToAvro EthereumBlock where
	toAvro = genericToAvro
instance ToPostgresText EthereumBlock

data EthereumTransaction = EthereumTransaction
	{ et_hash :: !B.ByteString
	, et_nonce :: {-# UNPACK #-} !Int64
	, et_from :: !B.ByteString
	, et_to :: !(Maybe B.ByteString)
	, et_value :: !Integer
	, et_gasPrice :: !Integer
	, et_gas :: {-# UNPACK #-} !Int64
	, et_input :: !B.ByteString
	-- from eth_getTransactionReceipt
	, et_gasUsed :: {-# UNPACK #-} !Int64
	, et_contractAddress :: !(Maybe B.ByteString)
	, et_logs :: !(V.Vector EthereumLog)
	, et_logsBloom :: !B.ByteString
	} deriving Generic

instance Schemable EthereumTransaction
instance SchemableField EthereumTransaction

instance J.FromJSON EthereumTransaction where
	parseJSON = J.withObject "transaction" $ \fields -> EthereumTransaction
		<$> (decodeHexBytes  =<< fields J..: "hash")
		<*> (decodeHexNumber =<< fields J..: "nonce")
		<*> (decodeHexBytes  =<< fields J..: "from")
		<*> (traverse decodeHexBytes =<< fields J..: "to")
		<*> (decodeHexNumber =<< fields J..: "value")
		<*> (decodeHexNumber =<< fields J..: "gasPrice")
		<*> (decodeHexNumber =<< fields J..: "gas")
		<*> (decodeHexBytes  =<< fields J..: "input")
		<*> (decodeHexNumber =<< fields J..: "gasUsed")
		<*> (traverse decodeHexBytes =<< fields J..: "contractAddress")
		<*> (                    fields J..: "logs")
		<*> (decodeHexBytes  =<< fields J..: "logsBloom")

instance A.HasAvroSchema EthereumTransaction where
	schema = genericAvroSchema
instance A.ToAvro EthereumTransaction where
	toAvro = genericToAvro
instance ToPostgresText EthereumTransaction

data EthereumLog = EthereumLog
	{ el_logIndex :: {-# UNPACK #-} !Int64
	, el_address :: !B.ByteString
	, el_data :: !B.ByteString
	, el_topics :: !(V.Vector B.ByteString)
	} deriving Generic

instance Schemable EthereumLog
instance SchemableField EthereumLog

instance J.FromJSON EthereumLog where
	parseJSON = J.withObject "log" $ \fields -> EthereumLog
		<$> (decodeHexNumber =<< fields J..: "logIndex")
		<*> (decodeHexBytes  =<< fields J..: "address")
		<*> (decodeHexBytes  =<< fields J..: "data")
		<*> (V.mapM decodeHexBytes =<< fields J..: "topics")

instance A.HasAvroSchema EthereumLog where
	schema = genericAvroSchema
instance A.ToAvro EthereumLog where
	toAvro = genericToAvro
instance ToPostgresText EthereumLog

newEthereum :: H.Manager -> T.Text -> Int -> Ethereum
newEthereum httpManager host port = Ethereum $ newJsonRpc httpManager host port Nothing

instance BlockChain Ethereum where
	type Block Ethereum = EthereumBlock
	type Transaction Ethereum = EthereumTransaction

	getCurrentBlockHeight (Ethereum jsonRpc) = do
		J.Success height <- J.parse decodeHexNumber <$> jsonRpcRequest jsonRpc "eth_blockNumber" []
		return height

	getBlockByHeight (Ethereum jsonRpc) blockHeight = do
		J.Object blockFields@(HML.lookup "transactions" -> Just (J.Array rawTransactions)) <- jsonRpcRequest jsonRpc "eth_getBlockByNumber" [encodeHexNumber blockHeight, J.Bool True]
		transactions <- V.forM rawTransactions $ \(J.Object fields@(HML.lookup "hash" -> Just transactionHash)) -> do
			J.Object receiptFields <- jsonRpcRequest jsonRpc "eth_getTransactionReceipt" [transactionHash]
			Just gasUsed <- return $ HML.lookup "gasUsed" receiptFields
			Just contractAddress <- return $ HML.lookup "contractAddress" receiptFields
			Just logs <- return $ HML.lookup "logs" receiptFields
			Just logsBloom <- return $ HML.lookup "logsBloom" receiptFields
			return $ J.Object $
				HML.insert "gasUsed" gasUsed $
				HML.insert "contractAddress" contractAddress $
				HML.insert "logs" logs $
				HML.insert "logsBloom" logsBloom $
				fields
		let jsonBlock = J.Object $ HML.insert "transactions" (J.Array transactions) blockFields
		case J.fromJSON jsonBlock of
			J.Success block -> return block
			J.Error err -> fail err
