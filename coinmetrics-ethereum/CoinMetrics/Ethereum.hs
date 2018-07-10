{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
	( newEthereum
	, EthereumBlock(..)
	, EthereumUncleBlock(..)
	, EthereumTransaction(..)
	, EthereumLog(..)
	, EthereumAction(..)
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
import qualified Data.Text as T
import qualified Data.Vector as V
import Data.Vector.Instances()
import qualified Data.Vector.Mutable as VM
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

data Ethereum = Ethereum !JsonRpc !Bool

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
	, et_actions :: !(V.Vector EthereumAction)
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
		<*> (fmap (fromMaybe V.empty) $ fields J..:? "actions")

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

data EthereumAction = EthereumAction
	{
	-- | Type of action: "call", "create", "reward" or "suicide"
	  ea_type :: !Int64
	, ea_stack :: !(V.Vector Int64)
	-- | Call is valid (not errored).
	, ea_valid :: !Bool
	-- | Call is succeeded (valid, and operation succeeded).
	, ea_succeeded :: !Bool
	-- | Changes to the state made by this call are not reverted.
	, ea_accounted :: !Bool
	-- | "from" for "call" and "create", "address" for "suicide"
	, ea_from :: !(Maybe B.ByteString)
	-- | "to" for "call", "address" for "create", "author" for "reward", "refund_address" for "suicide"
	, ea_to :: !(Maybe B.ByteString)
	-- | "value" for "call", "create", and "reward" , "balance" for "suicide"
	, ea_value :: !(Maybe Integer)
	-- | "gas" for "call" and "create"
	, ea_gas :: !(Maybe Int64)
	-- | "gas_used" for "call" and "create"
	, ea_gasUsed :: !(Maybe Int64)
	} deriving Generic

instance Schemable EthereumAction
instance SchemableField EthereumAction

instance J.FromJSON EthereumAction where
	parseJSON = J.withObject "ethereum action" $ \fields -> do
		action <- fields J..: "action"
		actionType <- fields J..: "type"
		stack <- fields J..: "traceAddress"
		maybeResult <- fields J..:? "result"
		valid <- fields J..: "valid"
		succeeded <- fields J..: "succeeded"
		accounted <- fields J..: "accounted"
		case actionType :: T.Text of
			"call" -> EthereumAction 0 stack valid succeeded accounted
				<$> (traverse decode0xHexBytes =<< action J..:? "from")
				<*> (traverse decode0xHexBytes =<< action J..:? "to")
				<*> (traverse decode0xHexNumber =<< action J..:? "value")
				<*> (traverse decode0xHexNumber =<< action J..:? "gas")
				<*> (traverse (decode0xHexNumber <=< (J..: "gasUsed")) maybeResult)
			"create" -> EthereumAction 1 stack valid succeeded accounted
				<$> (traverse decode0xHexBytes =<< action J..:? "from")
				<*> (maybe (return Nothing) (traverse decode0xHexBytes <=< (J..:? "address")) maybeResult)
				<*> (traverse decode0xHexNumber =<< action J..:? "value")
				<*> (traverse decode0xHexNumber =<< action J..:? "gas")
				<*> (traverse (decode0xHexNumber <=< (J..: "gasUsed")) maybeResult)
			"reward" -> EthereumAction 2 stack valid succeeded accounted
				<$> (return Nothing)
				<*> (traverse decode0xHexBytes =<< action J..:? "author")
				<*> (traverse decode0xHexNumber =<< action J..:? "value")
				<*> (return Nothing)
				<*> (return Nothing)
			"suicide" -> EthereumAction 3 stack valid succeeded accounted
				<$> (traverse decode0xHexBytes =<< action J..:? "address")
				<*> (traverse decode0xHexBytes =<< action J..:? "refund_address")
				<*> (maybe (return Nothing) (traverse decode0xHexNumber <=< (J..:? "balance")) maybeResult)
				<*> (return Nothing)
				<*> (return Nothing)
			_ -> fail $ "unknown ethereum action: " <> T.unpack actionType

instance A.HasAvroSchema EthereumAction where
	schema = genericAvroSchema
instance A.ToAvro EthereumAction where
	toAvro = genericToAvro
instance ToPostgresText EthereumAction

newEthereum :: H.Manager -> H.Request -> Bool -> Ethereum
newEthereum httpManager httpRequest enableTrace = Ethereum (newJsonRpc httpManager httpRequest Nothing) enableTrace

instance BlockChain Ethereum where
	type Block Ethereum = EthereumBlock

	getCurrentBlockHeight (Ethereum jsonRpc _enableTrace) = do
		J.Success height <- J.parse decode0xHexNumber <$> jsonRpcRequest jsonRpc "eth_blockNumber" ([] :: V.Vector J.Value)
		return height

	getBlockByHeight (Ethereum jsonRpc enableTrace) blockHeight = do
		blockFields <- jsonRpcRequest jsonRpc "eth_getBlockByNumber" ([encode0xHexNumber blockHeight, J.Bool True] :: V.Vector J.Value)
		J.Success rawTransactions <- return $ J.parse (J..: "transactions") blockFields
		J.Success unclesHashes <- return $ J.parse (mapM decode0xHexBytes <=< (J..: "uncles")) blockFields
		blockActions <- if enableTrace
			then do
				jsonActions <- jsonRpcRequest jsonRpc "trace_block" ([encode0xHexNumber blockHeight] :: V.Vector J.Value)
				-- add valid and succeeded fields, and note transaction indices
				indexedActions <- forM jsonActions $ \jsonAction -> either fail return $ flip J.parseEither jsonAction $ J.withObject "ethereum action" $ \actionFields -> do
					let valid = not $ HML.member "error" actionFields
					succeeded <- let
						isObject = \case
							Just (J.Object _) -> True
							_ -> False
						in isObject <$> actionFields J..:? "result"
					txIndex <- fromMaybe (-1) <$> actionFields J..:? "transactionPosition"
					action <- J.parseJSON $ J.Object
						$ HML.insert "valid" (J.Bool valid)
						$ HML.insert "succeeded" (J.Bool succeeded)
						$ HML.insert "accounted" (J.Bool $ valid && succeeded)
						actionFields
					return (txIndex, action)
				-- propagate succeeded field and remember as "accounted"
				return $ V.create $ do
					actions <- V.thaw indexedActions
					let
						indexedActionsMap = HML.fromList $ V.toList $ V.map (\(txIndex, action) -> ((txIndex, ea_stack action), action)) indexedActions
						actionsCount = V.length indexedActions
						step i actionsMap = when (i < actionsCount) $ do
							(txIndex, action@EthereumAction
								{ ea_stack = stack
								, ea_accounted = accounted
								}) <- VM.read actions i
							let
								parentAccounted = if V.null stack
									then True
									else ea_accounted $ actionsMap HML.! (txIndex, V.init stack)
								newAction = action
									{ ea_accounted = accounted && parentAccounted
									}
							VM.write actions i (txIndex, newAction)
							step (i + 1) (HML.insert (txIndex, stack) newAction actionsMap)
						in step 0 indexedActionsMap
					return actions
			else return V.empty
		transactions <- flip V.imapM rawTransactions $ \i rawTransaction -> do
			transactionHash <- either fail return $ J.parseEither (J..: "hash") rawTransaction
			receiptFields <- jsonRpcRequest jsonRpc "eth_getTransactionReceipt" ([transactionHash] :: V.Vector J.Value)
			either fail return $ flip J.parseEither receiptFields $ \receiptFields' -> do
				gasUsed <- receiptFields' J..: "gasUsed"
				contractAddress <- receiptFields' J..: "contractAddress"
				logs <- receiptFields' J..: "logs"
				logsBloom <- fromMaybe J.Null <$> receiptFields' J..:? "logsBloom"
				transaction <- J.parseJSON $ J.Object
					$ HML.insert "gasUsed" gasUsed
					$ HML.insert "contractAddress" contractAddress
					$ HML.insert "logs" logs
					$ HML.insert "logsBloom" logsBloom
					$ HML.insert "actions" (J.Array [])
					rawTransaction
				return transaction
					{ et_actions = V.map snd $ V.filter ((== i) . fst) blockActions
					}
		uncles <- flip V.imapM unclesHashes $ \i _uncleHash ->
			jsonRpcRequest jsonRpc "eth_getUncleByBlockNumberAndIndex" ([encode0xHexNumber blockHeight, encode0xHexNumber i] :: V.Vector J.Value)
		block <- either fail return $ J.parseEither J.parseJSON $ J.Object
			$ HML.insert "transactions" (J.Array [])
			$ HML.insert "uncles" (J.Array [])
			blockFields
		return block
			{ eb_transactions = transactions
			, eb_uncles = uncles
			}

	blockHeightFieldName _ = "number"
