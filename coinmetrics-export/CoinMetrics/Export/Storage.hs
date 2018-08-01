{-# LANGUAGE GADTs #-}

module CoinMetrics.Export.Storage
	( ExportStorage(..)
	, SomeExportStorage(..)
	, ExportStorageOptions(..)
	) where

import qualified Data.Avro as A
import qualified Data.Text as T
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Unified
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

class ExportStorage s where
	initExportStorage :: ExportStorageOptions -> IO s
	getExportStorageMaxBlock :: s -> Maybe (IO (Maybe BlockHeight))
	getExportStorageMaxBlock _ = Nothing
	writeExportStorage :: (Schemable a, A.ToAvro a, ToPostgresText a, IsUnifiedBlock a) => s -> [[a]] -> IO ()

data SomeExportStorage where
	SomeExportStorage :: ExportStorage a => a -> SomeExportStorage

data ExportStorageOptions = ExportStorageOptions
	{ eso_httpManager :: !H.Manager
	, eso_destination :: !String
	, eso_table :: !T.Text
	, eso_primaryField :: !T.Text
	, eso_upsert :: !Bool
	}
