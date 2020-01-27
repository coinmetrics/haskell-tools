{-# LANGUAGE DeriveGeneric, LambdaCase, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ethereum
  ( Ethereum(..)
  , EthereumBlock(..)
  , EthereumUncleBlock(..)
  , EthereumTransaction(..)
  , EthereumLog(..)
  , EthereumAction(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.HashMap.Lazy as HML
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import Data.Vector.Instances()
import qualified Data.Vector.Mutable as VM

import CoinMetrics.BlockChain
import CoinMetrics.JsonRpc
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Ethereum = Ethereum
  { ethereum_jsonRpc :: !JsonRpc
  , ethereum_enableTrace :: !Bool
  , ethereum_excludeUnaccountedActions :: !Bool
  }

data EthereumBlock = EthereumBlock
  { eb_number :: {-# UNPACK #-} !Int64
  , eb_hash :: {-# UNPACK #-} !HexString
  , eb_parentHash :: {-# UNPACK #-} !HexString
  , eb_nonce :: {-# UNPACK #-} !HexString
  , eb_sha3Uncles :: {-# UNPACK #-} !HexString
  , eb_logsBloom :: {-# UNPACK #-} !HexString
  , eb_transactionsRoot :: {-# UNPACK #-} !HexString
  , eb_stateRoot :: {-# UNPACK #-} !HexString
  , eb_receiptsRoot :: {-# UNPACK #-} !HexString
  , eb_miner :: {-# UNPACK #-} !HexString
  , eb_difficulty :: !Integer
  , eb_totalDifficulty :: !Integer
  , eb_extraData :: {-# UNPACK #-} !HexString
  , eb_size :: {-# UNPACK #-} !Int64
  , eb_gasLimit :: {-# UNPACK #-} !Int64
  , eb_gasUsed :: {-# UNPACK #-} !Int64
  , eb_timestamp :: {-# UNPACK #-} !Int64
  , eb_transactions :: !(V.Vector EthereumTransaction)
  , eb_uncles :: !(V.Vector EthereumUncleBlock)
  }

instance HasBlockHeader EthereumBlock where
  getBlockHeader EthereumBlock
    { eb_number = number
    , eb_hash = hash
    , eb_timestamp = timestamp
    } = BlockHeader
    { bh_height = number
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp
    }

newtype EthereumBlockWrapper = EthereumBlockWrapper
  { unwrapEthereumBlock :: EthereumBlock
  }

instance J.FromJSON EthereumBlockWrapper where
  parseJSON = J.withObject "ethereum block" $ \fields -> fmap EthereumBlockWrapper $ EthereumBlock
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
    <*> (V.map unwrapEthereumTransaction <$> fields J..: "transactions")
    <*> (V.map unwrapEthereumUncleBlock <$> fields J..: "uncles")

data EthereumUncleBlock = EthereumUncleBlock
  { eub_number :: {-# UNPACK #-} !Int64
  , eub_hash :: {-# UNPACK #-} !HexString
  , eub_parentHash :: {-# UNPACK #-} !HexString
  , eub_nonce :: {-# UNPACK #-} !HexString
  , eub_sha3Uncles :: {-# UNPACK #-} !HexString
  , eub_logsBloom :: {-# UNPACK #-} !HexString
  , eub_transactionsRoot :: {-# UNPACK #-} !HexString
  , eub_stateRoot :: {-# UNPACK #-} !HexString
  , eub_receiptsRoot :: {-# UNPACK #-} !HexString
  , eub_miner :: {-# UNPACK #-} !HexString
  , eub_difficulty :: !Integer
  , eub_totalDifficulty :: !(Maybe Integer)
  , eub_extraData :: {-# UNPACK #-} !HexString
  , eub_gasLimit :: {-# UNPACK #-} !Int64
  , eub_gasUsed :: {-# UNPACK #-} !Int64
  , eub_timestamp :: {-# UNPACK #-} !Int64
  }

newtype EthereumUncleBlockWrapper = EthereumUncleBlockWrapper
  { unwrapEthereumUncleBlock :: EthereumUncleBlock
  }

instance J.FromJSON EthereumUncleBlockWrapper where
  parseJSON = J.withObject "ethereum uncle block" $ \fields -> fmap EthereumUncleBlockWrapper $ EthereumUncleBlock
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

data EthereumTransaction = EthereumTransaction
  { et_hash :: {-# UNPACK #-} !HexString
  , et_nonce :: {-# UNPACK #-} !Int64
  , et_from :: {-# UNPACK #-} !HexString
  , et_to :: !(Maybe HexString)
  , et_value :: !Integer
  , et_gasPrice :: !Integer
  , et_gas :: {-# UNPACK #-} !Int64
  , et_input :: {-# UNPACK #-} !HexString
  -- from eth_getTransactionReceipt
  , et_gasUsed :: {-# UNPACK #-} !Int64
  , et_contractAddress :: !(Maybe HexString)
  , et_logs :: !(V.Vector EthereumLog)
  , et_logsBloom :: !(Maybe HexString)
  , et_actions :: !(V.Vector EthereumAction)
  }

newtype EthereumTransactionWrapper = EthereumTransactionWrapper
  { unwrapEthereumTransaction :: EthereumTransaction
  }

instance J.FromJSON EthereumTransactionWrapper where
  parseJSON = J.withObject "ethereum transaction" $ \fields -> fmap EthereumTransactionWrapper $ EthereumTransaction
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
    <*> (V.map unwrapEthereumLog <$> fields J..: "logs")
    <*> (traverse decode0xHexBytes =<< fields J..:? "logsBloom")
    <*> (V.map unwrapEthereumAction . fromMaybe V.empty <$> fields J..:? "actions")

data EthereumLog = EthereumLog
  { el_logIndex :: {-# UNPACK #-} !Int64
  , el_address :: {-# UNPACK #-} !HexString
  , el_data :: {-# UNPACK #-} !HexString
  , el_topics :: !(V.Vector HexString)
  }

newtype EthereumLogWrapper = EthereumLogWrapper
  { unwrapEthereumLog :: EthereumLog
  }

instance J.FromJSON EthereumLogWrapper where
  parseJSON = J.withObject "ethereum log" $ \fields -> fmap EthereumLogWrapper $ EthereumLog
    <$> (decode0xHexNumber =<< fields J..: "logIndex")
    <*> (decode0xHexBytes  =<< fields J..: "address")
    <*> (decode0xHexBytes  =<< fields J..: "data")
    <*> (V.mapM decode0xHexBytes =<< fields J..: "topics")

data EthereumAction = EthereumAction
  {
  -- | Type of action: "call", "create", "reward" or "suicide"
    ea_type :: {-# UNPACK #-} !Int64
  , ea_stack :: !(V.Vector Int64)
  -- | Call is valid (not errored).
  , ea_valid :: !Bool
  -- | Call is succeeded (valid, and operation succeeded).
  , ea_succeeded :: !Bool
  -- | Changes to the state made by this call are not reverted.
  , ea_accounted :: !Bool
  -- | "from" for "call" and "create", "address" for "suicide"
  , ea_from :: !(Maybe HexString)
  -- | "to" for "call", "address" for "create", "author" for "reward", "refund_address" for "suicide"
  , ea_to :: !(Maybe HexString)
  -- | "value" for "call", "create", and "reward" , "balance" for "suicide"
  , ea_value :: !(Maybe Integer)
  -- | "gas" for "call" and "create"
  , ea_gas :: !(Maybe Int64)
  -- | "gas_used" for "call" and "create"
  , ea_gasUsed :: !(Maybe Int64)
  -- | Raw action JSON.
  , ea_raw :: !J.Value
  }

newtype EthereumActionWrapper = EthereumActionWrapper
  { unwrapEthereumAction :: EthereumAction
  }

instance J.FromJSON EthereumActionWrapper where
  parseJSON = J.withObject "ethereum action" $ \fields -> do
    action <- fields J..: "action"
    actionType <- fields J..: "type"
    stack <- fields J..: "traceAddress"
    maybeResult <- fields J..:? "result"
    valid <- fields J..: "valid"
    succeeded <- fields J..: "succeeded"
    accounted <- fields J..: "accounted"
    fmap EthereumActionWrapper $ case actionType :: T.Text of
      "call" -> EthereumAction 0 stack valid succeeded accounted
        <$> (traverse decode0xHexBytes =<< action J..:? "from")
        <*> (traverse decode0xHexBytes =<< action J..:? "to")
        <*> (traverse decode0xHexNumber =<< action J..:? "value")
        <*> (traverse decode0xHexNumber =<< action J..:? "gas")
        <*> traverse (decode0xHexNumber <=< (J..: "gasUsed")) maybeResult
        <*> (return $ J.Object fields)
      "create" -> EthereumAction 1 stack valid succeeded accounted
        <$> (traverse decode0xHexBytes =<< action J..:? "from")
        <*> maybe (return Nothing) (traverse decode0xHexBytes <=< (J..:? "address")) maybeResult
        <*> (traverse decode0xHexNumber =<< action J..:? "value")
        <*> (traverse decode0xHexNumber =<< action J..:? "gas")
        <*> traverse (decode0xHexNumber <=< (J..: "gasUsed")) maybeResult
        <*> (return $ J.Object fields)
      "reward" -> EthereumAction 2 stack valid succeeded accounted
        <$> return Nothing
        <*> (traverse decode0xHexBytes =<< action J..:? "author")
        <*> (traverse decode0xHexNumber =<< action J..:? "value")
        <*> return Nothing
        <*> return Nothing
        <*> (return $ J.Object fields)
      "suicide" -> EthereumAction 3 stack valid succeeded accounted
        <$> (traverse decode0xHexBytes =<< action J..:? "address")
        <*> (traverse decode0xHexBytes =<< action J..:? "refundAddress")
        <*> (traverse decode0xHexNumber =<< action J..:? "balance")
        <*> return Nothing
        <*> return Nothing
        <*> (return $ J.Object fields)
      _ -> fail $ "unknown ethereum action: " <> T.unpack actionType

genSchemaInstances [''EthereumBlock, ''EthereumTransaction, ''EthereumLog, ''EthereumAction, ''EthereumUncleBlock]
genFlattenedTypes "number" [| eb_number |] [("block", ''EthereumBlock), ("transaction", ''EthereumTransaction), ("log", ''EthereumLog), ("action", ''EthereumAction), ("uncle", ''EthereumUncleBlock)]

instance BlockChain Ethereum where
  type Block Ethereum = EthereumBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      , bcp_trace = trace
      , bcp_excludeUnaccountedActions = excludeUnaccountedActions
      } -> return Ethereum
      { ethereum_jsonRpc = newJsonRpc httpManager httpRequest Nothing
      , ethereum_enableTrace = trace
      , ethereum_excludeUnaccountedActions = excludeUnaccountedActions
      }
    , bci_defaultApiUrls = ["http://127.0.0.1:8545/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -100 -- conservative rewrite limit
    , bci_heightFieldName = "number"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy EthereumBlock))
      [ schemaOf (Proxy :: Proxy EthereumAction)
      , schemaOf (Proxy :: Proxy EthereumLog)
      , schemaOf (Proxy :: Proxy EthereumTransaction)
      , schemaOf (Proxy :: Proxy EthereumUncleBlock)
      ]
      "CREATE TABLE \"ethereum\" OF \"EthereumBlock\" (PRIMARY KEY (\"number\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "logs", "actions", "uncles"]
    , bci_flattenPack = let
      f (blocks, (transactions, logs, actions), uncles) =
        [ SomeBlocks (blocks :: [EthereumBlock_flattened])
        , SomeBlocks (transactions :: [EthereumTransaction_flattened])
        , SomeBlocks (logs :: [EthereumLog_flattened])
        , SomeBlocks (actions :: [EthereumAction_flattened])
        , SomeBlocks (uncles :: [EthereumUncleBlock_flattened])
        ]
      in f . mconcat . map flatten
    }

  getBlockChainNodeInfo Ethereum
    { ethereum_jsonRpc = jsonRpc
    } = do
    let
      parseVersionString = J.parseEither $ \obj -> do
        version <- obj J..: "version"
        major <- version J..: "major"
        minor <- version J..: "minor"
        patch <- version J..: "patch"
        return $ T.pack $ showsPrec 0 (major :: Int) $ '.' : (showsPrec 0 (minor :: Int) $ '.' : show (patch :: Int))
    networkInfoJson <- jsonRpcRequest jsonRpc "parity_versionInfo" ([] :: V.Vector J.Value)
    version <- either fail return $ parseVersionString networkInfoJson
    return BlockChainNodeInfo
      { bcni_version = version
      }

  getCurrentBlockHeight Ethereum
    { ethereum_jsonRpc = jsonRpc
    } = do
    J.Success height <- J.parse decode0xHexNumber <$> jsonRpcRequest jsonRpc "eth_blockNumber" ([] :: V.Vector J.Value)
    return height

  getBlockByHeight Ethereum
    { ethereum_jsonRpc = jsonRpc
    , ethereum_enableTrace = enableTrace
    , ethereum_excludeUnaccountedActions = excludeUnaccountedActions
    } blockHeight = do
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
            in do
              hasResult <- isObject <$> actionFields J..:? "result"
              isSuicide <- (== J.String "suicide") <$> actionFields J..: "type"
              return $ hasResult || isSuicide -- suicide actions always succeed and don't have result
          txIndex <- fromMaybe (-1) <$> actionFields J..:? "transactionPosition"
          action <- fmap unwrapEthereumAction $ J.parseJSON $ J.Object
            $ HML.insert "valid" (J.Bool valid)
            $ HML.insert "succeeded" (J.Bool succeeded)
            $ HML.insert "accounted" (J.Bool $ valid && succeeded)
            actionFields
          return (txIndex, action)
        -- propagate succeeded field and remember as "accounted"
        return $ (if excludeUnaccountedActions then V.filter (ea_accounted . snd) else id) $ V.create $ do
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
                parentAccounted = V.null stack || ea_accounted (actionsMap HML.! (txIndex, V.init stack))
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
        transaction <- fmap unwrapEthereumTransaction $ J.parseJSON $ J.Object
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
      unwrapEthereumUncleBlock <$> jsonRpcRequest jsonRpc "eth_getUncleByBlockNumberAndIndex" ([encode0xHexNumber blockHeight, encode0xHexNumber i] :: V.Vector J.Value)
    block <- either fail (return . unwrapEthereumBlock) $ J.parseEither J.parseJSON $ J.Object
      $ HML.insert "transactions" (J.Array [])
      $ HML.insert "uncles" (J.Array [])
      blockFields
    return block
      { eb_transactions = transactions
      , eb_uncles = uncles
      }
