{-# LANGUAGE GADTs #-}

module CoinMetrics.Export.Storage
  ( ExportStorage(..)
  , SomeExportStorage(..)
  , ExportStorageOptions(..)
  , ExportStorageParams(..)
  ) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.Text as T
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import Hanalytics.Schema
import Hanalytics.Schema.Postgres

class ExportStorage s where

  initExportStorage :: ExportStorageOptions a -> IO (s a)

  getExportStorageMaxBlock :: s a -> ExportStorageParams -> Maybe (IO (Maybe BlockHeight))
  getExportStorageMaxBlock _ _ = Nothing

  writeExportStorage :: (Schemable a, A.ToAvro a, ToPostgresText a, J.ToJSON a) => s a -> ExportStorageParams -> [[a]] -> IO ()
  writeExportStorage s params = writeExportStorageSomeBlocks s params . map (\pack -> [SomeBlocks pack])

  writeExportStorageSomeBlocks :: s a -> ExportStorageParams -> [[SomeBlocks]] -> IO ()
  writeExportStorageSomeBlocks _ _ _ = fail "this storage does not support flattened output"

data SomeExportStorage a where
  SomeExportStorage :: ExportStorage s => s a -> SomeExportStorage a

data ExportStorageOptions a = ExportStorageOptions
  { eso_httpManager :: !H.Manager
  , eso_tables :: [T.Text]
  , eso_primaryField :: !T.Text
  , eso_getPrimaryField :: !(a -> T.Text)
  , eso_upsert :: !Bool
  }

newtype ExportStorageParams = ExportStorageParams
  { esp_destination :: String
  }
