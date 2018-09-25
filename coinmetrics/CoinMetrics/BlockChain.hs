{-# LANGUAGE GADTs, FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
	( BlockChain(..)
	, IsBlock(..)
	, BlockChainParams(..)
	, BlockChainInfo(..)
	, BlockHash()
	, BlockHeight()
	, BlockTimestamp()
	, SomeBlockChain(..)
	, SomeBlockChainInfo(..)
	, SomeBlocks(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import qualified Data.HashMap.Strict as HM
import Data.Proxy
import qualified Data.Text as T
import Data.Time.Clock
import qualified Network.HTTP.Client as H

import Hanalytics.Schema
import Hanalytics.Schema.Postgres

class (IsBlock (Block a), Schemable (Block a), A.ToAvro (Block a), ToPostgresText (Block a), J.ToJSON (Block a)) => BlockChain a where
	type Block a :: *
	getBlockChainInfo :: Proxy a -> BlockChainInfo a
	getCurrentBlockHeight :: a -> IO Int64
	getBlockByHeight :: a -> BlockHeight -> IO (Block a)
	blockHeightFieldName :: a -> T.Text

class IsBlock a where
	getBlockHeight :: a -> BlockHeight
	getBlockTimestamp :: a -> BlockTimestamp

-- | Params for initializing blockchain.
data BlockChainParams = BlockChainParams
	{ bcp_httpManager :: !H.Manager
	, bcp_httpRequest :: !H.Request
	-- | Include transaction trace information.
	, bcp_trace :: !Bool
	-- | Exclude unaccounted actions from trace information.
	, bcp_excludeUnaccountedActions :: !Bool
	-- | Number of threads working with blockchain.
	, bcp_threadsCount :: !Int
	}

-- | Information about blockchain.
data BlockChainInfo a = BlockChainInfo
	{ bci_init :: !(BlockChainParams -> IO a)
	, bci_defaultApiUrl :: !String
	, bci_defaultBeginBlock :: {-# UNPACK #-} !BlockHeight
	, bci_defaultEndBlock :: {-# UNPACK #-} !BlockHeight
	-- | Schemas referenced by storage type.
	, bci_schemas :: !(HM.HashMap T.Text T.Text)
	-- | Table suffixes for flattened blocks.
	, bci_flattenSuffixes :: [T.Text]
	-- | Flatten block function.
	, bci_flattenPack :: ([Block a] -> [SomeBlocks])
	}

type BlockHash = B.ByteString
type BlockHeight = Int64
type BlockTimestamp = UTCTime

data SomeBlockChain where
	SomeBlockChain :: BlockChain a => a -> SomeBlockChain

data SomeBlockChainInfo where
	SomeBlockChainInfo :: BlockChain a => BlockChainInfo a -> SomeBlockChainInfo

-- | Helper data type to contain stripe of arbitrary blocks.
data SomeBlocks where
	SomeBlocks :: (Schemable b, A.ToAvro b, ToPostgresText b, J.ToJSON b) => [b] -> SomeBlocks
