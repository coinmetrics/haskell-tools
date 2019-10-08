{-# LANGUAGE OverloadedLists, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Export.Storage.Elastic
  ( ElasticExportStorage()
  , ElasticFileExportStorage()
  ) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Encoding as T
import qualified Network.HTTP.Client as H

import CoinMetrics.Export.Storage

newtype ElasticExportStorage = ElasticExportStorage ExportStorageOptions

instance ExportStorage ElasticExportStorage where
  initExportStorage = return . ElasticExportStorage

  getExportStorageMaxBlock (ElasticExportStorage ExportStorageOptions
    { eso_httpManager = httpManager
    , eso_tables = (head -> table)
    , eso_primaryField = primaryField
    }) ExportStorageParams
    { esp_destination = destination
    } = Just $ do
    httpRequest <- H.parseRequest destination
    response <- H.responseBody <$> H.httpLbs httpRequest
      { H.method = "GET"
      , H.requestHeaders = [("Content-Type", "application/json")]
      , H.path = "/" <> T.encodeUtf8 table <> "/_search"
      , H.requestBody = H.RequestBodyLBS $ J.encode $ J.Object
        [ ("size", J.Number 0)
        , ("track_total_hits", J.Bool False)
        , ("aggs", J.Object
          [ ("max_key", J.Object
            [ ("max", J.Object
              [ ("field", J.String primaryField)
              ])
            ])
          ])
        ]
      } httpManager
    either fail return . J.parseEither ((J..:? "value") <=< (J..: "max_key") <=< (J..: "aggregations")) =<< either fail return (J.eitherDecode response)

  writeExportStorage (ElasticExportStorage options@ExportStorageOptions
    { eso_httpManager = httpManager
    }) ExportStorageParams
    { esp_destination = destination
    } packs = do
    httpRequest <- H.parseRequest destination
    forM_ packs $ \pack -> do
      response <- H.responseBody <$> H.httpLbs httpRequest
        { H.method = "POST"
        , H.requestHeaders = [("Content-Type", "application/json")]
        , H.path = "/_bulk"
        , H.requestBody = H.RequestBodyLBS $ elasticExportStoragePack options pack
        } httpManager
      errors <- either fail return . J.parseEither (J..: "errors") =<< either fail return (J.eitherDecode response)
      when errors $ fail "errors while uploading to ElasticSearch"

newtype ElasticFileExportStorage = ElasticFileExportStorage ExportStorageOptions

instance ExportStorage ElasticFileExportStorage where
  initExportStorage = return . ElasticFileExportStorage

  writeExportStorage (ElasticFileExportStorage options) ExportStorageParams
    { esp_destination = destination
    } = BL.writeFile destination . mconcat . map (elasticExportStoragePack options)

elasticExportStoragePack :: J.ToJSON a => ExportStorageOptions -> [a] -> BL.ByteString
elasticExportStoragePack ExportStorageOptions
  { eso_tables = (head -> table)
  , eso_primaryField = primaryField
  , eso_upsert = upsert
  } = mconcat . map elasticLine
  where
    elasticLine block = let
      J.Object (J.parseEither (J..: primaryField) -> Right key) = J.toJSON block
      in J.encode (J.Object
        [ (if upsert then "index" else "create", J.Object
          [ ("_index", J.toJSON table)
          , ("_id", key)
          ])
        ]) <> "\n" <> J.encode block <> "\n"
