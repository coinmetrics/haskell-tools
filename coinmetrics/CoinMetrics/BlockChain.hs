{-# LANGUAGE GADTs, FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
	( BlockChain(..)
	, BlockHash()
	, BlockHeight()
	, SomeBlockChain(..)
	) where

import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import qualified Data.Text as T

import Hanalytics.Schema.Postgres

class (A.ToAvro (Block a), ToPostgresText (Block a)) => BlockChain a where
	type Block a :: *
	type Transaction a :: *
	getCurrentBlockHeight :: a -> IO Int64
	getBlockByHeight :: a -> BlockHeight -> IO (Block a)
	blockHeightFieldName :: a -> T.Text

type BlockHash = B.ByteString
type BlockHeight = Int64

data SomeBlockChain where
	SomeBlockChain :: BlockChain a => a -> SomeBlockChain
