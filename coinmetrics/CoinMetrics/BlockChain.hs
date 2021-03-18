{-# LANGUAGE GADTs, FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
  ( BlockChain(..)
  , HasBlockHeader(..)
  , BlockChainParams(..)
  , BlockChainInfo(..)
  , BlockChainNodeInfo(..)
  , BlockHeader(..)
  , BlockHash()
  , BlockHeight()
  , BlockTimestamp()
  , SomeBlockChain(..)
  , SomeBlockChainInfo(..)
  , SomeBlocks(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import Data.Int
import qualified Data.HashMap.Strict as HM
import Data.Proxy
import qualified Data.Text as T
import Data.Time.Clock
import qualified Network.HTTP.Client as H

import Hanalytics.Schema
import Hanalytics.Schema.Postgres

import CoinMetrics.Util

class (HasBlockHeader (Block a), Schemable (Block a), A.ToAvro (Block a), ToPostgresText (Block a), J.ToJSON (Block a)) => BlockChain a where
  type Block a :: *

  getBlockChainInfo :: Proxy a -> BlockChainInfo a

  getBlockChainNodeInfo :: a -> IO BlockChainNodeInfo
  getBlockChainNodeInfo _ = return BlockChainNodeInfo
    { bcni_version = T.empty
    }

  getCurrentBlockHeight :: a -> IO BlockHeight

  getBlockHeaderByHeight :: a -> BlockHeight -> IO BlockHeader
  getBlockHeaderByHeight blockChain blockHeight = getBlockHeader <$> getBlockByHeight blockChain blockHeight

  getBlockByHeight :: a -> BlockHeight -> IO (Block a)


class HasBlockHeader a where
  getBlockHeader :: a -> BlockHeader

-- | Params for initializing blockchain.
data BlockChainParams = BlockChainParams
  { bcp_httpManager :: !H.Manager
  , bcp_httpRequest :: !H.Request
  -- | Include transaction trace information.
  , bcp_trace :: !Bool
  -- | Exclude unaccounted actions from trace information.
  , bcp_excludeUnaccountedActions :: !Bool
  -- | Specify API flavor. Used to when two blockchains only have minor differences in their API.
  , bcp_apiFlavor :: !T.Text
  -- | Number of threads working with blockchain.
  , bcp_threadsCount :: {-# UNPACK #-} !Int
  }

-- | Information about blockchain.
data BlockChainInfo a = BlockChainInfo
  { bci_init :: !(BlockChainParams -> IO a)
  , bci_defaultApiUrls :: ![String]
  , bci_defaultBeginBlock :: {-# UNPACK #-} !BlockHeight
  , bci_defaultEndBlock :: {-# UNPACK #-} !BlockHeight
  , bci_heightFieldName :: !T.Text
  -- | Schemas referenced by storage type.
  , bci_schemas :: !(HM.HashMap T.Text T.Text)
  -- | Table suffixes for flattened blocks.
  , bci_flattenSuffixes :: [T.Text]
  -- | Flatten block function.
  , bci_flattenPack :: [Block a] -> [SomeBlocks]
  }

-- | Information about blockchain node.
data BlockChainNodeInfo = BlockChainNodeInfo
  { bcni_version :: !T.Text
  }

-- | Information about block.
data BlockHeader = BlockHeader
  { bh_height :: {-# UNPACK #-} !BlockHeight
  , bh_hash :: {-# UNPACK #-} !BlockHash
  , bh_prevHash :: !(Maybe BlockHash)
  , bh_timestamp :: !UTCTime
  }

instance HasBlockHeader BlockHeader where
  getBlockHeader = id

type BlockHash = HexString
type BlockHeight = Int64
type BlockTimestamp = UTCTime

data SomeBlockChain where
  SomeBlockChain :: BlockChain a => a -> SomeBlockChain

data SomeBlockChainInfo where
  SomeBlockChainInfo :: BlockChain a => BlockChainInfo a -> SomeBlockChainInfo

-- | Helper data type to contain stripe of arbitrary blocks.
data SomeBlocks where
  SomeBlocks :: (Schemable b, A.ToAvro b, ToPostgresText b, J.ToJSON b) => [b] -> SomeBlocks
