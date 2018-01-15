{-# LANGUAGE FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
	( BlockChain(..)
	, BlockHash()
	, BlockHeight()
	) where

import qualified Data.ByteString as B
import Data.Int

class BlockChain a where
	type Block a :: *
	type Transaction a :: *
	getCurrentBlockHeight :: a -> IO Int64
	getBlockByHeight :: a -> BlockHeight -> IO (Block a)

type BlockHash = B.ByteString
type BlockHeight = Int64
