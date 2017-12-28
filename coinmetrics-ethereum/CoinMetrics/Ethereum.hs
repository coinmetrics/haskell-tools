{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
	( newEthereum
	, EthereumBlock(..)
	, EthereumTransaction(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import GHC.Generics(Generic)
import Data.Int
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H
import Numeric

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
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
	, et_to :: !(Maybe B.ByteString)
	, et_value :: {-# UNPACK #-} !Int64
	, et_gasPrice :: {-# UNPACK #-} !Int64
	, et_gas :: {-# UNPACK #-} !Int64
	, et_input :: !B.ByteString
	} deriving Generic

instance J.FromJSON EthereumTransaction where
	parseJSON = J.withObject "transaction" $ \fields -> EthereumTransaction
		<$> (decodeHexBytes  =<< fields J..: "hash")
		<*> (decodeHexNumber =<< fields J..: "nonce")
		<*> (decodeHexBytes  =<< fields J..: "blockHash")
		<*> (decodeHexNumber =<< fields J..: "blockNumber")
		<*> (decodeHexNumber =<< fields J..: "transactionIndex")
		<*> (decodeHexBytes  =<< fields J..: "from")
		<*> (decodeMaybeHexBytes =<< fields J..: "to")
		<*> (decodeHexNumber =<< fields J..: "value")
		<*> (decodeHexNumber =<< fields J..: "gasPrice")
		<*> (decodeHexNumber =<< fields J..: "gas")
		<*> (decodeHexBytes  =<< fields J..: "input")

instance A.HasAvroSchema EthereumTransaction where
	schema = genericAvroSchema
instance A.ToAvro EthereumTransaction where
	toAvro = genericToAvro

newEthereum :: H.Manager -> T.Text -> Int -> Ethereum
newEthereum httpManager host port = Ethereum $ newJsonRpc httpManager host port Nothing

instance BlockChain Ethereum where
	type Block Ethereum = EthereumBlock
	type Transaction Ethereum = EthereumTransaction

	getBlockByHeight (Ethereum jsonRpc) blockHeight = do
		eitherBlock <- J.fromJSON <$> jsonRpcRequest jsonRpc "eth_getBlockByNumber" [J.String $ T.pack $ "0x" ++ showHex blockHeight "", J.Bool True]
		case eitherBlock of
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
