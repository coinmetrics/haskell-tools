{-# LANGUAGE OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Export.Storage.Postgres
  ( PostgresExportStorage()
  ) where

import Control.Exception
import Control.Monad
import Data.Maybe
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Short as DBS
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Database.PostgreSQL.Simple as PQS
import qualified Database.PostgreSQL.Simple.Types as PQST
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TL
import           Data.String

import CoinMetrics.Export.Storage
import CoinMetrics.Export.Storage.PostgresFile
import CoinMetrics.BlockChain
import CoinMetrics.Util

withDB :: String -> (PQS.Connection -> IO a) -> IO a
withDB connectionString = bracket (PQS.connectPostgreSQL $ BC8.pack connectionString) PQS.close

newtype PostgresExportStorage = PostgresExportStorage ExportStorageOptions

instance ExportStorage PostgresExportStorage where
  initExportStorage = return . PostgresExportStorage

  getExportStorageMaxBlock (PostgresExportStorage ExportStorageOptions
    { eso_tables = (head -> table)
    , eso_primaryField = primaryField
    }) ExportStorageParams
    { esp_destination = destination
    } = Just $ withConnection destination $ \connection -> do
    let query = "SELECT MAX(\"" <> primaryField <> "\") FROM \"" <> table <> "\""
    result <- maybe (fail "cannot get latest block from postgres") return =<< PQ.execParams connection (T.encodeUtf8 query) [] PQ.Text
    resultStatus <- PQ.resultStatus result
    unless (resultStatus == PQ.TuplesOk) $ fail $ "cannot get latest block from postgres: " <> show resultStatus
    tuplesCount <- PQ.ntuples result
    unless (tuplesCount == 1) $ fail "cannot decode tuples from postgres"
    maybeValue <- PQ.getvalue result 0 0
    return $ read . T.unpack . T.decodeUtf8 <$> maybeValue

  isBlockStored (PostgresExportStorage ExportStorageOptions
    { eso_tables = (head -> table)
    , eso_primaryField = primaryField
    }) ExportStorageParams
    { esp_destination = destination
    } = Just isStored
    where 
      toBool :: [PQS.Only Integer] -> Bool
      toBool [] = False
      toBool xs = ( > 0) $ PQS.fromOnly $ head xs
      isStored :: BlockHash -> IO Bool
      isStored blockHash =
        withDB destination $ \connection -> do
          let query = "SELECT COUNT(*) FROM \""
                    <> TL.fromText table
                    <> "\" WHERE \""
                    <> TL.fromText primaryField
                    <> "\"=?"
          let hash = PQST.Binary $ DBS.fromShort $ unHexString blockHash
          result <- PQS.query connection
            (fromString $ TL.unpack $ TL.toLazyText query) (PQS.Only hash)
          return $ toBool result

  writeExportStorageSomeBlocks (PostgresExportStorage options) ExportStorageParams
    { esp_destination = destination
    } = mapM_ $ \pack -> withConnection destination $ \connection -> do
    let query = TL.toStrict $ TL.toLazyText $ postgresExportStorageSql options pack
    resultStatus <- maybe (return PQ.FatalError) PQ.resultStatus =<< PQ.exec connection (T.encodeUtf8 query)
    unless (resultStatus == PQ.CommandOk) $ fail $ "command failed: " <> show resultStatus

withConnection :: String -> (PQ.Connection -> IO a) -> IO a
withConnection destination = bracket (connectDestination destination) PQ.finish

connectDestination :: String -> IO PQ.Connection
connectDestination destination = do
  connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack destination
  connectionStatus <- PQ.status connection
  unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
  return connection
