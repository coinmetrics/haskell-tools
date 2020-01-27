{-# LANGUAGE GeneralizedNewtypeDeriving, OverloadedStrings, TypeFamilies #-}

module CoinMetrics.Cosmos
  ( Cosmos(..)
  , CosmosBlock
  , CosmosTransaction(..)
  ) where

import Control.Monad
import qualified Crypto.Hash as C
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Proxy
import Data.String
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Tendermint
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

newtype Cosmos = Cosmos (Tendermint CosmosTransaction)

type CosmosBlock = TendermintBlock CosmosTransaction

newtype CosmosTransaction = CosmosTransaction J.Value deriving (SchemableField, J.FromJSON, J.ToJSON, A.ToAvro, A.HasAvroSchema, ToPostgresText)

instance TendermintTx CosmosTransaction where
  -- only calculate hex-encoded hash here; transaction is retrieved in JSON format later
  decodeTendermintTx t = do
    bytes <- either fail return $ BA.convertFromBase BA.Base64 $ T.encodeUtf8 t
    let
      hash = T.decodeUtf8 $ BA.convertToBase BA.Base16 (C.hash (bytes :: B.ByteString) :: C.Digest C.SHA256)
    return $ CosmosTransaction $ J.toJSON hash


instance BlockChain Cosmos where
  type Block Cosmos = CosmosBlock

  getBlockChainInfo _ = BlockChainInfo
    { bci_init = fmap Cosmos . bci_init (getBlockChainInfo undefined)
    , bci_defaultApiUrls = ["http://127.0.0.1:1317/"]
    , bci_defaultBeginBlock = 1
    , bci_defaultEndBlock = 0
    , bci_heightFieldName = "height"
    , bci_schemas = standardBlockChainSchemas
      (schemaOf (Proxy :: Proxy CosmosBlock))
      [
      ]
      "CREATE TABLE \"cosmos\" OF \"CosmosBlock\" (PRIMARY KEY (\"height\"));"
    }

  getBlockChainNodeInfo (Cosmos Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    }) = do
    response <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/node_info"
      } httpManager
    result <- either fail return $ J.eitherDecode' $ H.responseBody response
    version <- either fail return $ J.parseEither ((J..: "application_version") >=> (J..: "version")) result
    return BlockChainNodeInfo
      { bcni_version = version
      }

  getCurrentBlockHeight (Cosmos Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    }) = do
    blockResponse <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/blocks/latest"
      } httpManager
    either fail (return . (tb_height :: TendermintBlock CosmosTransaction -> BlockHeight) . unwrapTendermintBlock) $ J.eitherDecode' $ H.responseBody blockResponse

  getBlockByHeight (Cosmos Tendermint
    { tendermint_httpManager = httpManager
    , tendermint_httpRequest = httpRequest
    }) height = do
    blockResponse <- tryWithRepeat $ H.httpLbs httpRequest
      { H.path = "/blocks/" <> fromString (show height)
      } httpManager
    block <- either fail (return . unwrapTendermintBlock) $ J.eitherDecode' $ H.responseBody blockResponse
    -- retrieve transactions
    decodedTransactions <- V.forM (tb_transactions block) $ \(CosmosTransaction (J.String hash)) -> do
      txResponse <- tryWithRepeat $ H.httpLbs httpRequest
        { H.path = "/txs/" <> T.encodeUtf8 hash
        } httpManager
      either fail return $ J.eitherDecode' $ H.responseBody txResponse
    return block
      { tb_transactions = decodedTransactions
      }
