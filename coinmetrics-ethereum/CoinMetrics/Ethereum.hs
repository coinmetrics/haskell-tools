{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
	( newEthereum
	, EthereumBlock(..)
	, EthereumUncleBlock(..)
	, EthereumTransaction(..)
	, EthereumLog(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import qualified Data.HashMap.Lazy as HML
import GHC.Generics(Generic)
import Data.Int
import Data.Maybe
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

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
	, eb_transactions :: !(V.Vector EthereumTransaction)
	, eb_uncles :: !(V.Vector EthereumUncleBlock)
	} deriving Generic

instance Schemable EthereumBlock

instance J.FromJSON EthereumBlock where
	parseJSON = J.withObject "ethereum block" $ \fields -> EthereumBlock
		<$> (decode0xHexNumber    =<< fields J..: "number")
		<*> (decode0xHexBytes     =<< fields J..: "hash")
		<*> (decode0xHexBytes     =<< fields J..: "parentHash")
		<*> (decode0xHexBytes     =<< fields J..: "nonce")
		<*> (decode0xHexBytes     =<< fields J..: "sha3Uncles")
		<*> (decode0xHexBytes     =<< fields J..: "logsBloom")
		<*> (decode0xHexBytes     =<< fields J..: "transactionsRoot")
		<*> (decode0xHexBytes     =<< fields J..: "stateRoot")
		<*> (decode0xHexBytes     =<< fields J..: "receiptsRoot")
		<*> (decode0xHexBytes     =<< fields J..: "miner")
		<*> (decode0xHexNumber    =<< fields J..: "difficulty")
		<*> (decode0xHexNumber    =<< fields J..: "totalDifficulty")
		<*> (decode0xHexBytes     =<< fields J..: "extraData")
		<*> (decode0xHexNumber    =<< fields J..: "size")
		<*> (decode0xHexNumber    =<< fields J..: "gasLimit")
		<*> (decode0xHexNumber    =<< fields J..: "gasUsed")
		<*> (decode0xHexNumber    =<< fields J..: "timestamp")
		<*> (                       fields J..: "transactions")
		<*> (                       fields J..: "uncles")

instance A.HasAvroSchema EthereumBlock where
	schema = genericAvroSchema
instance A.ToAvro EthereumBlock where
	toAvro = genericToAvro
instance ToPostgresText EthereumBlock

data EthereumUncleBlock = EthereumUncleBlock
	{ eub_number :: {-# UNPACK #-} !Int64
	, eub_hash :: !B.ByteString
	, eub_parentHash :: !B.ByteString
	, eub_nonce :: !B.ByteString
	, eub_sha3Uncles :: !B.ByteString
	, eub_logsBloom :: !B.ByteString
	, eub_transactionsRoot :: !B.ByteString
	, eub_stateRoot :: !B.ByteString
	, eub_receiptsRoot :: !B.ByteString
	, eub_miner :: !B.ByteString
	, eub_difficulty :: !Integer
	, eub_totalDifficulty :: !(Maybe Integer)
	, eub_extraData :: !B.ByteString
	, eub_gasLimit :: {-# UNPACK #-} !Int64
	, eub_gasUsed :: {-# UNPACK #-} !Int64
	, eub_timestamp :: {-# UNPACK #-} !Int64
	} deriving Generic

instance Schemable EthereumUncleBlock
instance SchemableField EthereumUncleBlock

instance J.FromJSON EthereumUncleBlock where
	parseJSON = J.withObject "ethereum uncle block" $ \fields -> EthereumUncleBlock
		<$> (decode0xHexNumber    =<< fields J..: "number")
		<*> (decode0xHexBytes     =<< fields J..: "hash")
		<*> (decode0xHexBytes     =<< fields J..: "parentHash")
		<*> (decode0xHexBytes     =<< fields J..: "nonce")
		<*> (decode0xHexBytes     =<< fields J..: "sha3Uncles")
		<*> (decode0xHexBytes     =<< fields J..: "logsBloom")
		<*> (decode0xHexBytes     =<< fields J..: "transactionsRoot")
		<*> (decode0xHexBytes     =<< fields J..: "stateRoot")
		<*> (decode0xHexBytes     =<< fields J..: "receiptsRoot")
		<*> (decode0xHexBytes     =<< fields J..: "miner")
		<*> (decode0xHexNumber    =<< fields J..: "difficulty")
		<*> (traverse decode0xHexNumber =<< fields J..:? "totalDifficulty")
		<*> (decode0xHexBytes     =<< fields J..: "extraData")
		<*> (decode0xHexNumber    =<< fields J..: "gasLimit")
		<*> (decode0xHexNumber    =<< fields J..: "gasUsed")
		<*> (decode0xHexNumber    =<< fields J..: "timestamp")

instance A.HasAvroSchema EthereumUncleBlock where
	schema = genericAvroSchema
instance A.ToAvro EthereumUncleBlock where
	toAvro = genericToAvro
instance ToPostgresText EthereumUncleBlock

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
	, et_logsBloom :: !(Maybe B.ByteString)
	} deriving Generic

instance Schemable EthereumTransaction
instance SchemableField EthereumTransaction

instance J.FromJSON EthereumTransaction where
	parseJSON = J.withObject "ethereum transaction" $ \fields -> EthereumTransaction
		<$> (decode0xHexBytes  =<< fields J..: "hash")
		<*> (decode0xHexNumber =<< fields J..: "nonce")
		<*> (decode0xHexBytes  =<< fields J..: "from")
		<*> (traverse decode0xHexBytes =<< fields J..: "to")
		<*> (decode0xHexNumber =<< fields J..: "value")
		<*> (decode0xHexNumber =<< fields J..: "gasPrice")
		<*> (decode0xHexNumber =<< fields J..: "gas")
		<*> (decode0xHexBytes  =<< fields J..: "input")
		<*> (decode0xHexNumber =<< fields J..: "gasUsed")
		<*> (traverse decode0xHexBytes =<< fields J..: "contractAddress")
		<*> (                      fields J..: "logs")
		<*> (traverse decode0xHexBytes =<< fields J..:? "logsBloom")

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
	parseJSON = J.withObject "ethereum log" $ \fields -> EthereumLog
		<$> (decode0xHexNumber =<< fields J..: "logIndex")
		<*> (decode0xHexBytes  =<< fields J..: "address")
		<*> (decode0xHexBytes  =<< fields J..: "data")
		<*> (V.mapM decode0xHexBytes =<< fields J..: "topics")

instance A.HasAvroSchema EthereumLog where
	schema = genericAvroSchema
instance A.ToAvro EthereumLog where
	toAvro = genericToAvro
instance ToPostgresText EthereumLog

newEthereum :: H.Manager -> H.Request -> Ethereum
newEthereum httpManager httpRequest = Ethereum $ newJsonRpc httpManager httpRequest Nothing

instance BlockChain Ethereum where
	type Block Ethereum = EthereumBlock

	getCurrentBlockHeight (Ethereum jsonRpc) = do
		J.Success height <- J.parse decode0xHexNumber <$> jsonRpcRequest jsonRpc "eth_blockNumber" ([] :: V.Vector J.Value)
		return height

	getBlockByHeight (Ethereum jsonRpc) blockHeight = do
		blockFields <- jsonRpcRequest jsonRpc "eth_getBlockByNumber" ([encode0xHexNumber blockHeight, J.Bool True] :: V.Vector J.Value)
		J.Success rawTransactions <- return $ J.parse (J..: "transactions") blockFields
		J.Success unclesHashes <- return $ J.parse (mapM decode0xHexBytes <=< (J..: "uncles")) blockFields
		transactions <- V.forM rawTransactions $ \rawTransaction -> do
			J.Success transactionHash <- return $ J.parse (J..: "hash") rawTransaction
			J.Object receiptFields <- jsonRpcRequest jsonRpc "eth_getTransactionReceipt" ([transactionHash] :: V.Vector J.Value)
			Just gasUsed <- return $ HML.lookup "gasUsed" receiptFields
			Just contractAddress <- return $ HML.lookup "contractAddress" receiptFields
			Just logs <- return $ HML.lookup "logs" receiptFields
			let logsBloom = fromMaybe J.Null $ HML.lookup "logsBloom" receiptFields
			return $ J.Object
				$ HML.insert "gasUsed" gasUsed
				$ HML.insert "contractAddress" contractAddress
				$ HML.insert "logs" logs
				$ HML.insert "logsBloom" logsBloom
				rawTransaction
		uncles <- flip V.imapM unclesHashes $ \i _uncleHash -> do
			J.Success uncle <- J.fromJSON <$> jsonRpcRequest jsonRpc "eth_getUncleByBlockNumberAndIndex" ([encode0xHexNumber blockHeight, encode0xHexNumber i] :: V.Vector J.Value)
			return uncle
		let jsonBlock = J.Object
			$ HML.insert "transactions" (J.Array transactions)
			$ HML.insert "uncles" (J.Array uncles)
			blockFields
		case J.fromJSON jsonBlock of
			J.Success block -> return block
			J.Error err -> fail err

	blockHeightFieldName _ = "number"
