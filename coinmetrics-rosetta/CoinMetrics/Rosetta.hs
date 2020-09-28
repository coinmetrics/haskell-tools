{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies #-}

module CoinMetrics.Rosetta
  ( Rosetta(..)
  , RosettaBlock(..)
  , RosettaTransaction(..)
  ) where

import Control.Concurrent.STM
import qualified Data.Aeson as J
import qualified Data.ByteString as B
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Rosetta = Rosetta
  { rosetta_httpManager :: !H.Manager
  , rosetta_httpRequest :: !H.Request
  , rosetta_networkIdentifierVar :: !(TVar (Maybe RosettaNetworkIdentifier))
  }

rosettaRequest :: (J.ToJSON req, J.FromJSON res) => Rosetta -> B.ByteString -> req -> IO res
rosettaRequest Rosetta
  { rosetta_httpManager = httpManager
  , rosetta_httpRequest = httpRequest
  } path request = do
  body <- H.responseBody <$> tryWithRepeat (H.httpLbs (httpRequest
    { H.path = path
    , H.requestBody = H.RequestBodyLBS $ J.encode request
    }) httpManager)
  either fail return $ J.eitherDecode body

data RosettaBlock = RosettaBlock
  { rb_block_identifier :: {-# UNPACK #-} !RosettaBlockIdentifier
  , rb_parent_block_identifier :: {-# UNPACK #-} !RosettaBlockIdentifier
  , rb_timestamp :: {-# UNPACK #-} !Int64
  , rb_transactions :: !(V.Vector RosettaTransaction)
  }

data RosettaBlockIdentifier = RosettaBlockIdentifier
  { rbi_index :: !(Maybe Int64)
  , rbi_hash :: !(Maybe HexString)
  }

instance HasBlockHeader RosettaBlock where
  getBlockHeader RosettaBlock
    { rb_block_identifier = RosettaBlockIdentifier
      { rbi_index = maybeHeight
      , rbi_hash = maybeHash
      }
    , rb_parent_block_identifier = RosettaBlockIdentifier
      { rbi_hash = maybePrevHash
      }
    , rb_timestamp = time
    } = BlockHeader
    { bh_height = fromJust maybeHeight
    , bh_hash = fromJust maybeHash
    , bh_prevHash = maybePrevHash
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral time * 0.001
    }

data RosettaTransaction = RosettaTransaction
  { rt_transaction_identifier :: {-# UNPACK #-} !RosettaTransactionIdentifier
  }

data RosettaTransactionIdentifier = RosettaTransactionIdentifier
  { rti_hash :: {-# UNPACK #-} !HexString
  }

data RosettaNetworkIdentifier = RosettaNetworkIdentifier
  { rni_blockchain :: !T.Text
  , rni_network :: !T.Text
  }

data RosettaNetworkListResponse = RosettaNetworkListResponse
  { rnlr_network_identifiers :: !(V.Vector RosettaNetworkIdentifier)
  }

data RosettaNetworkStatusRequest = RosettaNetworkStatusRequest
  { rnsr_network_identifier :: !RosettaNetworkIdentifier
  }

data RosettaNetworkStatusResponse = RosettaNetworkStatusResponse
  { rnsr_current_block_identifier :: !RosettaBlockIdentifier
  }

data RosettaBlockRequest = RosettaBlockRequest
  { rbr_network_identifier :: !RosettaNetworkIdentifier
  , rbr_block_identifier :: !RosettaBlockIdentifier
  }

data RosettaBlockResponse = RosettaBlockResponse
  { rbr_block :: !RosettaBlock
  }

genSchemaInstances
  [ ''RosettaBlock, ''RosettaTransaction
  , ''RosettaBlockIdentifier, ''RosettaTransactionIdentifier, ''RosettaNetworkIdentifier
  , ''RosettaNetworkListResponse
  , ''RosettaNetworkStatusRequest, ''RosettaNetworkStatusResponse
  , ''RosettaBlockRequest, ''RosettaBlockResponse
  ]

instance BlockChain Rosetta where
  type Block Rosetta = RosettaBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> do
      networkIdentifierVar <- newTVarIO Nothing
      return Rosetta
        { rosetta_httpManager = httpManager
        , rosetta_httpRequest = httpRequest
          { H.method = "POST"
          , H.requestHeaders = ("Content-Type", "application/json") : H.requestHeaders httpRequest
          }
        , rosetta_networkIdentifierVar = networkIdentifierVar
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:8080/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -100 -- conservative rewrite limit
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy RosettaBlock))
      [ schemaOf (Proxy :: Proxy RosettaBlockIdentifier)
      , schemaOf (Proxy :: Proxy RosettaTransactionIdentifier)
      , schemaOf (Proxy :: Proxy RosettaTransaction)
      ]
      "CREATE TABLE \"rosetta\" OF \"RosettaBlock\" (PRIMARY KEY (\"height\"));"
    }

  getCurrentBlockHeight rosetta = do
    networkIdentifier <- getNetworkIdentifier rosetta
    RosettaNetworkStatusResponse
      { rnsr_current_block_identifier = RosettaBlockIdentifier
        { rbi_index = Just blockHeight
        }
      } <- rosettaRequest rosetta "/network/status" RosettaNetworkStatusRequest
      { rnsr_network_identifier = networkIdentifier
      }
    return blockHeight

  getBlockByHeight rosetta blockHeight = do
    networkIdentifier <- getNetworkIdentifier rosetta
    RosettaBlockResponse
      { rbr_block = block
      } <- rosettaRequest rosetta "/block" RosettaBlockRequest
      { rbr_network_identifier = networkIdentifier
      , rbr_block_identifier = RosettaBlockIdentifier
        { rbi_index = Just blockHeight
        , rbi_hash = Nothing
        }
      }
    return block

getNetworkIdentifier :: Rosetta -> IO RosettaNetworkIdentifier
getNetworkIdentifier rosetta@Rosetta
  { rosetta_networkIdentifierVar = networkIdentifierVar
  } = do
  maybeNetworkIdentifier <- readTVarIO networkIdentifierVar
  case maybeNetworkIdentifier of
    Just networkIdentifier -> return networkIdentifier
    Nothing -> do
      RosettaNetworkListResponse
        { rnlr_network_identifiers = [networkIdentifier]
        } <- rosettaRequest rosetta "/network/list" (J.Object [])
      atomically $ writeTVar networkIdentifierVar $ Just networkIdentifier
      return networkIdentifier
