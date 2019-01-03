{-# LANGUAGE OverloadedStrings #-}

module CoinMetrics.Export.Storage.PostgresFile
  ( PostgresFileExportStorage()
  , postgresExportStorageSql
  ) where

import qualified Data.ByteString.Lazy as BL
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Text.Lazy.Encoding as TL

import CoinMetrics.BlockChain
import CoinMetrics.Export.Storage
import Hanalytics.Schema.Postgres

newtype PostgresFileExportStorage = PostgresFileExportStorage ExportStorageOptions

instance ExportStorage PostgresFileExportStorage where
  initExportStorage = return . PostgresFileExportStorage

  writeExportStorageSomeBlocks (PostgresFileExportStorage options) ExportStorageParams
    { esp_destination = destination
    } = BL.writeFile destination . TL.encodeUtf8 . TL.toLazyText . mconcat . map (postgresExportStorageSql options)

postgresExportStorageSql :: ExportStorageOptions -> [SomeBlocks] -> TL.Builder
postgresExportStorageSql ExportStorageOptions
  { eso_tables = tables
  , eso_primaryField = primaryField
  , eso_upsert = upsert
  } = (if length tables > 1 then wrapWithTx else id) . mconcat . zipWith statementGroup tables
  where
    statementGroup table (SomeBlocks blocks) = if null blocks
      then mempty
      else (if upsert then postgresSqlUpsertGroup primaryField else postgresSqlInsertGroup) table blocks
    wrapWithTx s = "BEGIN;\n" <> s <> "COMMIT;\n"
