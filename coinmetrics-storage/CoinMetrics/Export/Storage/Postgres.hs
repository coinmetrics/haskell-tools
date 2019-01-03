{-# LANGUAGE OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Export.Storage.Postgres
  ( PostgresExportStorage()
  ) where

import Control.Monad
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TL

import CoinMetrics.Export.Storage
import CoinMetrics.Export.Storage.PostgresFile

newtype PostgresExportStorage = PostgresExportStorage ExportStorageOptions

instance ExportStorage PostgresExportStorage where
  initExportStorage = return . PostgresExportStorage

  getExportStorageMaxBlock (PostgresExportStorage ExportStorageOptions
    { eso_tables = (head -> table)
    , eso_primaryField = primaryField
    }) ExportStorageParams
    { esp_destination = destination
    } = Just $ do
    connection <- connectDestination destination
    let query = "SELECT MAX(\"" <> primaryField <> "\") FROM \"" <> table <> "\""
    result <- maybe (fail "cannot get latest block from postgres") return =<< PQ.execParams connection (T.encodeUtf8 query) [] PQ.Text
    resultStatus <- PQ.resultStatus result
    unless (resultStatus == PQ.TuplesOk) $ fail $ "cannot get latest block from postgres: " <> show resultStatus
    tuplesCount <- PQ.ntuples result
    unless (tuplesCount == 1) $ fail "cannot decode tuples from postgres"
    maybeValue <- PQ.getvalue result 0 0
    PQ.finish connection
    return $ read . T.unpack . T.decodeUtf8 <$> maybeValue

  writeExportStorageSomeBlocks (PostgresExportStorage options) ExportStorageParams
    { esp_destination = destination
    } = mapM_ $ \pack -> do
    connection <- connectDestination destination
    let query = TL.toStrict $ TL.toLazyText $ postgresExportStorageSql options pack
    resultStatus <- maybe (return PQ.FatalError) PQ.resultStatus =<< PQ.exec connection (T.encodeUtf8 query)
    unless (resultStatus == PQ.CommandOk) $ fail $ "command failed: " <> show resultStatus
    PQ.finish connection

connectDestination :: String -> IO PQ.Connection
connectDestination destination = do
  connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack destination
  connectionStatus <- PQ.status connection
  unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
  return connection
