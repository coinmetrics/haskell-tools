{-# LANGUAGE OverloadedStrings #-}

module CoinMetrics.Export.Storage.Elastic
	( ElasticExportStorage()
	, ElasticFileExportStorage()
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteString.Lazy as BL
import qualified Network.HTTP.Client as H

import CoinMetrics.Export.Storage
import CoinMetrics.Unified

newtype ElasticExportStorage = ElasticExportStorage ExportStorageOptions

instance ExportStorage ElasticExportStorage where
	initExportStorage = return . ElasticExportStorage

	writeExportStorage (ElasticExportStorage options@ExportStorageOptions
		{ eso_httpManager = httpManager
		, eso_destination = destination
		}) packs = do
		httpRequest <- H.parseRequest destination
		forM_ packs $ \pack -> do
			response <- H.responseBody <$> H.httpLbs httpRequest
				{ H.method = "POST"
				, H.requestHeaders = [("Content-Type", "application/json")]
				, H.path = "/_bulk"
				, H.requestBody = H.RequestBodyLBS $ elasticExportStoragePack options pack
				} httpManager
			errors <- either fail return . J.parseEither (J..: "errors") =<< either fail return (J.eitherDecode response)
			when errors $ fail "errors while inserting to ElasticSearch"

newtype ElasticFileExportStorage = ElasticFileExportStorage ExportStorageOptions

instance ExportStorage ElasticFileExportStorage where
	initExportStorage = return . ElasticFileExportStorage

	writeExportStorage (ElasticFileExportStorage options@ExportStorageOptions
		{ eso_destination = destination
		}) = BL.writeFile destination . mconcat . map (elasticExportStoragePack options)

elasticExportStoragePack :: IsUnifiedBlock a => ExportStorageOptions -> [a] -> BL.ByteString
elasticExportStoragePack ExportStorageOptions
	{ eso_table = table
	} = mconcat . map elasticLine
	where
		elasticLine doc = "{\"index\":{\"_index\":" <> J.encode table <> ",\"_type\":\"block\"}}\n" <> J.encode (unifyBlock doc) <> "\n"
