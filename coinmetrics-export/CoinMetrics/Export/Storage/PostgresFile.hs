module CoinMetrics.Export.Storage.PostgresFile
	( PostgresFileExportStorage()
	, postgresExportStorageSql
	) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Text.Lazy.Encoding as TL

import CoinMetrics.Export.Storage
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

newtype PostgresFileExportStorage = PostgresFileExportStorage ExportStorageOptions

instance ExportStorage PostgresFileExportStorage where
	initExportStorage = return . PostgresFileExportStorage

	writeExportStorage (PostgresFileExportStorage options@ExportStorageOptions
		{ eso_destination = destination
		}) = BL.writeFile destination . TL.encodeUtf8 . TL.toLazyText . mconcat . map (postgresExportStorageSql options)

postgresExportStorageSql :: (Schemable a, ToPostgresText a) => ExportStorageOptions -> [a] -> TL.Builder
postgresExportStorageSql ExportStorageOptions
	{ eso_table = table
	, eso_primaryField = primaryField
	, eso_upsert = upsert
	} = (if upsert then postgresSqlUpsertGroup primaryField else postgresSqlInsertGroup) table
