{-# LANGUAGE GADTs, FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
	( BlockChain(..)
	, BlockChainParams(..)
	, BlockChainInfo(..)
	, BlockHash()
	, BlockHeight()
	, SomeBlockChain(..)
	, SomeBlockChainInfo(..)
	) where

import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import qualified Data.HashMap.Strict as HM
import Data.Proxy
import qualified Data.Text as T
import qualified Network.HTTP.Client as H

import CoinMetrics.Unified
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

class (Schemable (Block a), A.ToAvro (Block a), ToPostgresText (Block a), IsUnifiedBlock (Block a)) => BlockChain a where
	type Block a :: *
	getBlockChainInfo :: Proxy a -> BlockChainInfo a
	getCurrentBlockHeight :: a -> IO Int64
	getBlockByHeight :: a -> BlockHeight -> IO (Block a)
	blockHeightFieldName :: a -> T.Text

-- | Params for initializing blockchain.
data BlockChainParams = BlockChainParams
	{ bcp_httpManager :: !H.Manager
	, bcp_httpRequest :: !H.Request
	-- | Include transaction trace information.
	, bcp_trace :: !Bool
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
	}

type BlockHash = B.ByteString
type BlockHeight = Int64

data SomeBlockChain where
	SomeBlockChain :: BlockChain a => a -> SomeBlockChain

data SomeBlockChainInfo where
	SomeBlockChainInfo :: BlockChain a => BlockChainInfo a -> SomeBlockChainInfo
