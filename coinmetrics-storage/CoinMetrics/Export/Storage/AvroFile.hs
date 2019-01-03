module CoinMetrics.Export.Storage.AvroFile
  ( AvroFileExportStorage()
  ) where

import Control.Monad
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL

import CoinMetrics.Export.Storage

newtype AvroFileExportStorage = AvroFileExportStorage ExportStorageOptions

instance ExportStorage AvroFileExportStorage where
  initExportStorage = return . AvroFileExportStorage

  writeExportStorage AvroFileExportStorage {} ExportStorageParams
    { esp_destination = destination
    } = BL.writeFile destination <=< A.encodeContainer
