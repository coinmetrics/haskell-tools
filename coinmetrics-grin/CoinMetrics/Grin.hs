{-# LANGUAGE DeriveGeneric, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Grin
  ( Grin(..)
  , GrinBlock(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString as B
import Data.Int
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock.POSIX
import Data.Time.ISO8601
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Flatten
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

data Grin = Grin
  { grin_httpManager :: !H.Manager
  , grin_httpRequest :: !H.Request
  }

grinRequest :: J.FromJSON r => Grin -> T.Text -> [(B.ByteString, Maybe B.ByteString)] -> IO r
grinRequest Grin
  { grin_httpManager = httpManager
  , grin_httpRequest = httpRequest
  } path params = do
  body <- H.responseBody <$> tryWithRepeat (H.httpLbs (H.setQueryString params httpRequest
    { H.path = T.encodeUtf8 path
    }) httpManager)
  either fail return $ J.eitherDecode body

-- API: https://github.com/mimblewimble/grin/blob/master/doc/api/node_api.md

data GrinBlock = GrinBlock
  { gb_hash :: {-# UNPACK #-} !HexString
  , gb_version :: {-# UNPACK #-} !Int64
  , gb_height :: {-# UNPACK #-} !Int64
  , gb_timestamp :: {-# UNPACK #-} !Int64
  , gb_nonce :: !Integer
  , gb_total_difficulty :: !Integer
  , gb_secondary_scaling :: {-# UNPACK #-} !Int64
  , gb_total_kernel_offset :: {-# UNPACK #-} !HexString
  , gb_inputs :: !(V.Vector HexString)
  , gb_outputs :: !(V.Vector GrinOutput)
  , gb_kernels :: !(V.Vector GrinKernel)
  }

instance HasBlockHeader GrinBlock where
  getBlockHeader GrinBlock
    { gb_hash = hash
    , gb_height = height
    , gb_timestamp = timestamp
    } = BlockHeader
    { bh_height = height
    , bh_hash = hash
    , bh_prevHash = Nothing
    , bh_timestamp = posixSecondsToUTCTime $ fromIntegral timestamp
    }

newtype GrinBlockWrapper = GrinBlockWrapper
  { unwrapGrinBlock :: GrinBlock
  }

instance J.FromJSON GrinBlockWrapper where
  parseJSON = J.withObject "grin block" $ \fields -> do
    header <- fields J..: "header"
    fmap GrinBlockWrapper $ GrinBlock
      <$> (header J..: "hash")
      <*> (header J..: "version")
      <*> (header J..: "height")
      <*> (decodeDate =<< header J..: "timestamp")
      <*> (header J..: "nonce")
      <*> (header J..: "total_difficulty")
      <*> (header J..: "secondary_scaling")
      <*> (header J..: "total_kernel_offset")
      <*> (fields J..: "inputs")
      <*> (V.map unwrapGrinOutput <$> fields J..: "outputs")
      <*> (V.map unwrapGrinKernel <$> fields J..: "kernels")

data GrinOutput = GrinOutput
  { go_output_type :: !T.Text
  , go_commit :: {-# UNPACK #-} !HexString
  }

newtype GrinOutputWrapper = GrinOutputWrapper
  { unwrapGrinOutput :: GrinOutput
  }

instance J.FromJSON GrinOutputWrapper where
  parseJSON = J.withObject "grin output" $ \fields -> fmap GrinOutputWrapper $ GrinOutput
    <$> (fields J..: "output_type")
    <*> (fields J..: "commit")

data GrinKernel = GrinKernel
  { gk_features :: !T.Text
  , gk_fee :: !Integer
  , gk_lock_height :: {-# UNPACK #-} !Int64
  , gk_excess :: {-# UNPACK #-} !HexString
  }

newtype GrinKernelWrapper = GrinKernelWrapper
  { unwrapGrinKernel :: GrinKernel
  }

instance J.FromJSON GrinKernelWrapper where
  parseJSON = J.withObject "grin kernel" $ \fields -> fmap GrinKernelWrapper $ GrinKernel
    <$> (fields J..: "features")
    <*> (fields J..: "fee")
    <*> (fields J..: "lock_height")
    <*> (fields J..: "excess")

decodeDate :: T.Text -> J.Parser Int64
decodeDate (T.unpack -> t) = case parseISO8601 t of
  Just date -> return $ floor $ utcTimeToPOSIXSeconds date
  Nothing -> fail $ "wrong date: " <> t

genSchemaInstances [''GrinBlock, ''GrinOutput, ''GrinKernel]
genFlattenedTypes "height" [| gb_height |] [("block", ''GrinBlock), ("output", ''GrinOutput), ("kernel", ''GrinKernel)]

instance BlockChain Grin where
  type Block Grin = GrinBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Grin
        { grin_httpManager = httpManager
        , grin_httpRequest = httpRequest
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:3413/"]
    , bci_defaultBeginBlock = 0
    , bci_defaultEndBlock = -60
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy GrinBlock))
      [ schemaOf (Proxy :: Proxy GrinOutput)
      , schemaOf (Proxy :: Proxy GrinKernel)
      ]
      "CREATE TABLE \"grin\" OF \"GrinBlock\" (PRIMARY KEY (\"height\"));"
    , bci_flattenSuffixes = ["blocks", "outputs", "kernels"]
    , bci_flattenPack = let
      f (blocks, outputs, kernels) =
        [ SomeBlocks (blocks :: [GrinBlock_flattened])
        , SomeBlocks (outputs :: [GrinOutput_flattened])
        , SomeBlocks (kernels :: [GrinKernel_flattened])
        ]
      in f . mconcat . map flatten
    }

  getCurrentBlockHeight grin = either fail return . J.parseEither ((J..: "height") <=< (J..: "tip")) =<< grinRequest grin "/v1/status" []

  getBlockByHeight grin blockHeight = unwrapGrinBlock <$> grinRequest grin ("/v1/blocks/" <> T.pack (show blockHeight)) []
