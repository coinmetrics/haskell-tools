{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, StandaloneDeriving, TemplateHaskell, TypeFamilies #-}

module CoinMetrics.Nem
  ( Nem(..)
  , NemBlock(..)
  , NemTransaction(..)
  , NemNestedTransaction(..)
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Data.Time.Clock
import Data.Time.Format
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Schema.Util
import CoinMetrics.Util
import Hanalytics.Schema

-- | Nem connector.
data Nem = Nem
  { nem_httpManager :: !H.Manager
  , nem_httpRequest :: !H.Request
  }

nemRequest :: J.FromJSON r => Nem -> T.Text -> Maybe J.Value -> [(B.ByteString, Maybe B.ByteString)] -> IO r
nemRequest Nem
  { nem_httpManager = httpManager
  , nem_httpRequest = httpRequest
  } path maybeBody params = either fail return . J.eitherDecode' . H.responseBody =<< H.httpLbs (H.setQueryString params httpRequest
  { H.path = H.path httpRequest <> T.encodeUtf8 path
  , H.method = if isJust maybeBody then "POST" else "GET"
  , H.requestBody = maybe (H.requestBody httpRequest) (H.RequestBodyLBS . J.encode) maybeBody
  }) httpManager

data NemBlock = NemBlock
  { nb_timeStamp :: {-# UNPACK #-} !Int64
  , nb_height :: {-# UNPACK #-} !Int64
  , nb_signer :: {-# UNPACK #-} !HexString
  , nb_transactions :: !(V.Vector NemTransaction)
  }

instance HasBlockHeader NemBlock where
  getBlockHeader NemBlock
    { nb_timeStamp = timeStamp
    , nb_height = height
    } = BlockHeader
    { bh_height = height
    , bh_hash = mempty
    , bh_prevHash = Nothing
    , bh_timestamp = fromIntegral timeStamp `addUTCTime` nemBlockGenesisTimestamp
    }

nemBlockGenesisTimestamp :: UTCTime
nemBlockGenesisTimestamp = parseTimeOrError False defaultTimeLocale "%Y-%m-%dT%H:%M:%SZ" "2015-03-29T00:06:25Z"

newtype NemBlockWrapper = NemBlockWrapper
  { unwrapNemBlock :: NemBlock
  }

instance J.FromJSON NemBlockWrapper where
  parseJSON = J.withObject "nem block" $ \fields -> fmap NemBlockWrapper $ NemBlock
    <$> (fields J..: "timeStamp")
    <*> (fields J..: "height")
    <*> (fields J..: "signer")
    <*> (V.map unwrapNemTransaction <$> fields J..: "transactions")

data NemTransaction = NemTransaction
  { nt_timeStamp :: {-# UNPACK #-} !Int64
  , nt_fee :: {-# UNPACK #-} !Int64
  , nt_type :: {-# UNPACK #-} !Int64
  , nt_deadline :: {-# UNPACK #-} !Int64
  , nt_signer :: {-# UNPACK #-} !HexString
  , nt_signerAddress :: !T.Text
  , nt_recipient :: !(Maybe T.Text)
  , nt_mode :: !(Maybe Int64)
  , nt_remoteAccount :: !(Maybe HexString)
  , nt_creationFee :: !(Maybe Int64)
  , nt_creationFeeSink :: !(Maybe T.Text)
  , nt_delta :: !(Maybe Int64)
  , nt_otherAccount :: !(Maybe T.Text)
  , nt_rentalFee :: !(Maybe Int64)
  , nt_rentalFeeSink :: !(Maybe T.Text)
  , nt_newPart :: !(Maybe T.Text)
  , nt_parent :: !(Maybe T.Text)
  , nt_amount :: !(Maybe Int64)
  , nt_otherTrans :: !(Maybe NemNestedTransaction)
  , nt_signatures :: !(V.Vector NemNestedTransaction)
  }

newtype NemTransactionWrapper = NemTransactionWrapper
  { unwrapNemTransaction :: NemTransaction
  }

instance J.FromJSON NemTransactionWrapper where
  parseJSON = J.withObject "nem transaction" $ \fields -> fmap NemTransactionWrapper $ NemTransaction
    <$> (fields J..: "timeStamp")
    <*> (fields J..: "fee")
    <*> (fields J..: "type")
    <*> (fields J..: "deadline")
    <*> (fields J..: "signer")
    <*> return ""
    <*> (fields J..:? "recipient")
    <*> (fields J..:? "mode")
    <*> (fields J..:? "remoteAccount")
    <*> (fields J..:? "creationFee")
    <*> (fields J..:? "creationFeeSink")
    <*> (fields J..:? "delta")
    <*> (fields J..:? "otherAccount")
    <*> (fields J..:? "rentalFee")
    <*> (fields J..:? "rentalFeeSink")
    <*> (fields J..:? "newPart")
    <*> (fields J..:? "parent")
    <*> (fields J..:? "amount")
    <*> (fields J..:? "otherTrans")
    <*> (fromMaybe V.empty <$> fields J..:? "signatures")

data NemNestedTransaction = NemNestedTransaction
  { nnt_timeStamp :: {-# UNPACK #-} !Int64
  , nnt_fee :: {-# UNPACK #-} !Int64
  , nnt_type :: {-# UNPACK #-} !Int64
  , nnt_deadline :: {-# UNPACK #-} !Int64
  , nnt_signer :: {-# UNPACK #-} !HexString
  , nnt_recipient :: !(Maybe T.Text)
  , nnt_mode :: !(Maybe Int64)
  , nnt_remoteAccount :: !(Maybe HexString)
  , nnt_creationFee :: !(Maybe Int64)
  , nnt_creationFeeSink :: !(Maybe T.Text)
  , nnt_delta :: !(Maybe Int64)
  , nnt_otherAccount :: !(Maybe T.Text)
  , nnt_rentalFee :: !(Maybe Int64)
  , nnt_rentalFeeSink :: !(Maybe T.Text)
  , nnt_newPart :: !(Maybe T.Text)
  , nnt_parent :: !(Maybe T.Text)
  , nnt_amount :: !(Maybe Int64)
  }

genSchemaInstances [''NemBlock, ''NemTransaction, ''NemNestedTransaction]

instance BlockChain Nem where
  type Block Nem = NemBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = \BlockChainParams
      { bcp_httpManager = httpManager
      , bcp_httpRequest = httpRequest
      } -> return Nem
        { nem_httpManager = httpManager
        , nem_httpRequest = httpRequest
          { H.requestHeaders = [("Content-Type", "application/json")]
          }
        }
    , bci_defaultApiUrls = ["http://127.0.0.1:7890/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = -360 -- actual rewrite limit
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy NemBlock))
      [ schemaOf (Proxy :: Proxy NemNestedTransaction)
      , schemaOf (Proxy :: Proxy NemTransaction)
      ]
      "CREATE TABLE \"nem\" OF \"NemBlock\" (PRIMARY KEY (\"height\"));"
    }

  getCurrentBlockHeight nem = tryWithRepeat $ either fail return
    . J.parseEither (J..: "height")
    =<< nemRequest nem "/chain/height" Nothing []

  getBlockByHeight nem blockHeight = do
    block@NemBlock
      { nb_transactions = transactions
      } <- tryWithRepeat $ either fail (return . unwrapNemBlock) . J.parseEither J.parseJSON =<< nemRequest nem "/block/at/public" (Just $ J.Object [("height", J.Number $ fromIntegral blockHeight)]) []
    signersAddresses <- V.forM transactions $ \NemTransaction
      { nt_signer = signer
      } -> tryWithRepeat $ either fail return
      . J.parseEither ((J..: "address") <=< (J..: "account"))
      =<< nemRequest nem "/account/get/from-public-key" Nothing [("publicKey", Just $ BA.convertToBase BA.Base16 $ BS.fromShort $ unHexString signer)]
    return block
      { nb_transactions = V.zipWith (\transaction signerAddress -> transaction
        { nt_signerAddress = signerAddress
        }) transactions signersAddresses
      }
