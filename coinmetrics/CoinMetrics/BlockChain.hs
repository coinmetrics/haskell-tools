{-# LANGUAGE FlexibleContexts, TypeFamilies #-}

module CoinMetrics.BlockChain
	( BlockChain(..)
	, BlockHash()
	, BlockHeight()
	) where

import qualified Data.ByteString as B
import Data.Word

class BlockChain a where
	type Block a :: *
	type Transaction a :: *
	getBlockByHeight :: a -> BlockHeight -> IO (Block a)

type BlockHash = B.ByteString
type BlockHeight = Word64
