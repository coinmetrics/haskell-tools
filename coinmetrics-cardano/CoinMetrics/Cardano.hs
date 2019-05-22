{-# LANGUAGE DeriveGeneric, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Cardano
  ( Cardano(..)
  , CardanoBlock(..)
  , CardanoTransaction(..)
  , CardanoInput(..)
  , CardanoOutput(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Proxy
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

-- | Cardano connector.
data Cardano = Cardano
  { cardano_httpManager :: !H.Manager
  , cardano_httpRequest :: !H.Request
  }

cardanoRequest :: J.FromJSON r => Cardano -> T.Text -> [(B.ByteString, Maybe B.ByteString)] -> IO r
cardanoRequest Cardano
  { cardano_httpManager = httpManager
  , cardano_httpRequest = httpRequest
  } path params = do
  body <- H.responseBody <$> tryWithRepeat (H.httpLbs (H.setQueryString params httpRequest
    { H.path = T.encodeUtf8 path
    }) httpManager)
  either fail return $ J.eitherDecode body

-- API: https://cardanodocs.com/technical/explorer/api

data CardanoBlock = CardanoBlock
  { cb_height :: {-# UNPACK #-} !Int64
  , cb_epoch :: {-# UNPACK #-} !Int64
  , cb_slot :: {-# UNPACK #-} !Int64
  , cb_hash :: {-# UNPACK #-} !HexString
  , cb_timeIssued :: {-# UNPACK #-} !Int64
  , cb_totalSent :: !Integer
  , cb_size :: {-# UNPACK #-} !Int64
  , cb_blockLead :: {-# UNPACK #-} !HexString
  , cb_fees :: !Integer
  , cb_transactions :: !(V.Vector CardanoTransaction)
  }

instance HasBlockHeader CardanoBlock where
  getBlockHeader CardanoBlock
    { cb_height = height
    , cb_hash = hash
    , cb_timeIssued = timeIssued
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timeIssued
    }

newtype CardanoBlockWrapper = CardanoBlockWrapper
  { unwrapCardanoBlock :: CardanoBlock
  }

instance J.FromJSON CardanoBlockWrapper where
  parseJSON = J.withObject "cardano block" $ \fields -> fmap CardanoBlockWrapper $ CardanoBlock
    <$> (fields J..: "height")
    <*> (fields J..: "cbeEpoch")
    <*> (fields J..: "cbeSlot")
    <*> (fields J..: "cbeBlkHash")
    <*> (fields J..: "cbeTimeIssued")
    <*> (decodeValue =<< fields J..: "cbeTotalSent")
    <*> (fields J..: "cbeSize")
    <*> (fields J..: "cbeBlockLead")
    <*> (decodeValue =<< fields J..: "cbeFees")
    <*> (V.map unwrapCardanoTransaction <$> fields J..: "transactions")

data CardanoTransaction = CardanoTransaction
  { ct_id :: {-# UNPACK #-} !HexString
  , ct_timeIssued :: {-# UNPACK #-} !Int64
  , ct_fees :: !Integer
  , ct_inputs :: !(V.Vector CardanoInput)
  , ct_outputs :: !(V.Vector CardanoOutput)
  }

newtype CardanoTransactionWrapper = CardanoTransactionWrapper
  { unwrapCardanoTransaction :: CardanoTransaction
  }

instance J.FromJSON CardanoTransactionWrapper where
  parseJSON = J.withObject "cardano transaction" $ \fields -> fmap CardanoTransactionWrapper $ CardanoTransaction
    <$> (fields J..: "ctsId")
    <*> (fields J..: "ctsTxTimeIssued")
    <*> (decodeValue =<< fields J..: "ctsFees")
    <*> (V.map unwrapCardanoInput <$> fields J..: "ctsInputs")
    <*> (V.map unwrapCardanoOutput <$> fields J..: "ctsOutputs")

data CardanoInput = CardanoInput
  { ci_address :: !T.Text
  , ci_value :: !Integer
  , ci_txid :: !(Maybe HexString)
  , ci_output :: !(Maybe Int64)
  }

newtype CardanoInputWrapper = CardanoInputWrapper
  { unwrapCardanoInput :: CardanoInput
  }

instance J.FromJSON CardanoInputWrapper where
  parseJSON = J.withArray "cardano input" $ \fields -> fmap CardanoInputWrapper $ case V.length fields of
    -- official node
    2 -> CardanoInput
      <$> J.parseJSON (fields V.! 0)
      <*> decodeValue (fields V.! 1)
      <*> return Nothing
      <*> return Nothing
    -- forked node
    4 -> CardanoInput
      <$> J.parseJSON (fields V.! 2)
      <*> decodeValue (fields V.! 3)
      <*> (Just <$> J.parseJSON (fields V.! 0))
      <*> (Just <$> J.parseJSON (fields V.! 1))
    _ -> fail "wrong cardano input array"

data CardanoOutput = CardanoOutput
  { co_address :: !T.Text
  , co_value :: !Integer
  }

newtype CardanoOutputWrapper = CardanoOutputWrapper
  { unwrapCardanoOutput :: CardanoOutput
  }

instance J.FromJSON CardanoOutputWrapper where
  parseJSON = J.withArray "cardano output" $ \fields -> do
    unless (V.length fields == 2) $ fail "wrong cardano output array"
    fmap CardanoOutputWrapper $ CardanoOutput
      <$> J.parseJSON (fields V.! 0)
      <*> decodeValue (fields V.! 1)

decodeValue :: J.Value -> J.Parser Integer
decodeValue = J.withObject "cardano value" $ \fields -> read . T.unpack <$> fields J..: "getCoin"

genSchemaInstances [''CardanoBlock, ''CardanoTransaction, ''CardanoInput, ''CardanoOutput]
genFlattenedTypes "height" [| cb_height |] [("block", ''CardanoBlock), ("transaction", ''CardanoTransaction), ("input", ''CardanoInput), ("output", ''CardanoOutput)]

instance BlockChain Cardano where
  type Block Cardano = CardanoBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Cardano
        { cardano_httpManager = httpManager
        , cardano_httpRequest = httpRequest
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:8100/"]
    , bci_defaultBeginBlock = 2
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy CardanoBlock))
      [ schemaOf (Proxy :: Proxy CardanoInput)
      , schemaOf (Proxy :: Proxy CardanoOutput)
      , schemaOf (Proxy :: Proxy CardanoTransaction)
      ]
      "CREATE TABLE \"cardano\" OF \"CardanoBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks", "transactions", "logs", "actions", "uncles"]
    , bci_flattenPack = let
      f (blocks, (transactions, inputs, outputs)) =
        [ SomeBlocks (blocks :: [CardanoBlock_flattened])
        , SomeBlocks (transactions :: [CardanoTransaction_flattened])
        , SomeBlocks (inputs :: [CardanoInput_flattened])
        , SomeBlocks (outputs :: [CardanoOutput_flattened])
        ]
      in f . mconcat . map flatten
    }

  -- pageSize param doesn't work anymore
  -- getCurrentBlockHeight cardano = either fail return =<< cardanoRequest cardano "/api/blocks/pages/total" [("pageSize", Just "1")]
  getCurrentBlockHeight cardano = either fail (return . (+ (-8)) . (* 10)) =<< cardanoRequest cardano "/api/blocks/pages/total" []

  getBlockByHeight cardano blockHeight = do
    -- calculate page's index and block's index on page
    let
      pageIndex = (blockHeight + 8) `quot` 10
      blockIndexOnPage = blockHeight - (pageIndex * 10 - 8)
      reverseGet i v = v V.! (V.length v - 1 - i)
    -- get page with blocks
    pageObject <- either fail return =<< cardanoRequest cardano "/api/blocks/pages"
      [ ("page", Just $ fromString $ show pageIndex)
      , ("pageSize", Just "10")
      ]
    blockObject <- either fail return $ flip J.parseEither pageObject $ J.withArray "page" $
      \((V.! 1) -> blocksObjectsObject) ->
      J.withArray "blocks" (J.parseJSON . reverseGet (fromIntegral blockIndexOnPage)) blocksObjectsObject
    blockHashText <- either fail return $ J.parseEither (J..: "cbeBlkHash") blockObject
    blockTxsBriefObjects <- either fail return =<< cardanoRequest cardano ("/api/blocks/txs/" <> blockHashText) [("limit", Just "1000000000000000000")]
    blockTxs <- forM blockTxsBriefObjects $ \txBriefObject -> do
      txIdText <- either fail return $ J.parseEither (J..: "ctbId") txBriefObject
      txInputs <- either fail return $ J.parseEither (J..: "ctbInputs") txBriefObject
      -- forked node has more information about inputs, so replace them
      either fail return . J.parseEither J.parseJSON . J.Object
        =<< either fail (return . HM.insert "ctsInputs" txInputs)
        =<< cardanoRequest cardano ("/api/txs/summary/" <> txIdText) []
    either fail (return . unwrapCardanoBlock) $ J.parseEither (J.parseJSON . J.Object)
      $ HM.insert "height" (J.Number $ fromIntegral blockHeight)
      $ HM.insert "transactions" (J.Array blockTxs)
      blockObject
