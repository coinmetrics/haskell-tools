{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
	( newEthereum
	, EthereumBlock(..)
	, EthereumTransaction(..)
	, EthereumLog(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import qualified Data.HashMap.Lazy as HML
import GHC.Generics(Generic)
import Data.Int
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H
import Numeric

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema
import CoinMetrics.Schema.Avro

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
	, eb_difficulty :: {-# UNPACK #-} !Int64 
	, eb_totalDifficulty :: {-# UNPACK #-} !Int64
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
		<*> (decodeTransactions =<< fields J..: "transactions")
		<*> (decodeUncles       =<< fields J..: "uncles")
		where
			decodeTransactions = J.withArray "transactions" $ V.mapM J.parseJSON
			decodeUncles = J.withArray "uncles" $ V.mapM decodeHexBytes

instance A.HasAvroSchema EthereumBlock where
	schema = genericAvroSchema
instance A.ToAvro EthereumBlock where
	toAvro = genericToAvro

data EthereumTransaction = EthereumTransaction
	{ et_hash :: !B.ByteString
	, et_nonce :: {-# UNPACK #-} !Int64
	, et_blockHash :: !B.ByteString
	, et_blockNumber :: {-# UNPACK #-} !Int64
	, et_transactionIndex :: {-# UNPACK #-} !Int64
	, et_from :: !B.ByteString
	, et_to :: !B.ByteString
	, et_value :: {-# UNPACK #-} !Int64
	, et_gasPrice :: {-# UNPACK #-} !Int64
	, et_gas :: {-# UNPACK #-} !Int64
	, et_input :: !B.ByteString
	-- from eth_getTransactionReceipt
	, et_gasUsed :: {-# UNPACK #-} !Int64
	, et_contractAddress :: !B.ByteString
	, et_logs :: !(V.Vector EthereumLog)
	, et_logsBloom :: !B.ByteString
	} deriving Generic

instance Schemable EthereumTransaction
instance SchemableField EthereumTransaction

instance J.FromJSON EthereumTransaction where
	parseJSON = J.withObject "transaction" $ \fields -> EthereumTransaction
		<$> (decodeHexBytes  =<< fields J..: "hash")
		<*> (decodeHexNumber =<< fields J..: "nonce")
		<*> (decodeHexBytes  =<< fields J..: "blockHash")
		<*> (decodeHexNumber =<< fields J..: "blockNumber")
		<*> (decodeHexNumber =<< fields J..: "transactionIndex")
		<*> (decodeHexBytes  =<< fields J..: "from")
		<*> (fmap (fromMaybe B.empty) . decodeMaybeHexBytes =<< fields J..: "to")
		<*> (decodeHexNumber =<< fields J..: "value")
		<*> (decodeHexNumber =<< fields J..: "gasPrice")
		<*> (decodeHexNumber =<< fields J..: "gas")
		<*> (decodeHexBytes  =<< fields J..: "input")
		<*> (decodeHexNumber =<< fields J..: "gasUsed")
		<*> (fmap (fromMaybe B.empty) . decodeMaybeHexBytes =<< fields J..: "contractAddress")
		<*> (decodeLogs      =<< fields J..: "logs")
		<*> (decodeHexBytes  =<< fields J..: "logsBloom")
		where
			decodeLogs = J.withArray "logs" $ V.mapM J.parseJSON

instance A.HasAvroSchema EthereumTransaction where
	schema = genericAvroSchema
instance A.ToAvro EthereumTransaction where
	toAvro = genericToAvro

data EthereumLog = EthereumLog
	{ el_removed :: !Bool
	, el_logIndex :: {-# UNPACK #-} !Int64
	, el_address :: !B.ByteString
	, el_data :: !B.ByteString
	, el_topics :: !(V.Vector B.ByteString)
	} deriving Generic

instance Schemable EthereumLog
instance SchemableField EthereumLog

instance J.FromJSON EthereumLog where
	parseJSON = J.withObject "log" $ \fields -> EthereumLog
		<$> (J.parseJSON     =<< fields J..: "removed")
		<*> (decodeHexNumber =<< fields J..: "logIndex")
		<*> (decodeHexBytes  =<< fields J..: "address")
		<*> (decodeHexBytes  =<< fields J..: "data")
		<*> (decodeTopics    =<< fields J..: "topics")
		where
			decodeTopics = J.withArray "topics" $ V.mapM decodeHexBytes

instance A.HasAvroSchema EthereumLog where
	schema = genericAvroSchema
instance A.ToAvro EthereumLog where
	toAvro = genericToAvro

newEthereum :: H.Manager -> T.Text -> Int -> Ethereum
newEthereum httpManager host port = Ethereum $ newJsonRpc httpManager host port Nothing

instance BlockChain Ethereum where
	type Block Ethereum = EthereumBlock
	type Transaction Ethereum = EthereumTransaction

	getBlockByHeight (Ethereum jsonRpc) blockHeight = do
		J.Object blockFields@(HML.lookup "transactions" -> Just (J.Array rawTransactions)) <- jsonRpcRequest jsonRpc "eth_getBlockByNumber" [J.String $ T.pack $ "0x" ++ showHex blockHeight "", J.Bool True]
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

decodeHexBytes :: J.Value -> J.Parser B.ByteString
decodeHexBytes = J.withText "hex bytes" $ \str -> do
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) <- return str
	return s

decodeMaybeHexBytes :: J.Value -> J.Parser (Maybe B.ByteString)
decodeMaybeHexBytes value = case value of
	J.String str -> do
		(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) <- return str
		return $ Just s
	J.Null -> return Nothing
	_ -> fail "expected hex bytes or null"

decodeHexNumber :: Integral a => J.Value -> J.Parser a
decodeHexNumber = J.withText "hex number" $ \str -> do
	(T.stripPrefix "0x" -> Just (readHex . T.unpack -> [(n, "")])) <- return str
	return n
