{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns, TypeApplications #-}

module Options
  ( Options(..)
  , OptionCommand(..)
  , Api(..)
  , Output(..)
  , OutputStorage(..)
  , OutputStorages(..)
  , initOutputStorages
  , initContinuingOutputStorages
  , initRealtimeOutputStorages
  , writeToOutputStorages
  , logStrLn
  , logPrint
  , logStrMVar
  , prepare
  , parser
  ) where

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Monad
import qualified Data.Aeson                              as J
import qualified Data.Avro                               as A
import           Data.Either
import           Data.Maybe
import           Data.Proxy
import qualified Data.Text                               as T
import qualified Network.HTTP.Client                     as H
import qualified Options.Applicative                     as O
import           System.IO
import           System.IO.Unsafe
import qualified System.Process                          as P

import           CoinMetrics.BlockChain
import           CoinMetrics.BlockChain.All
import           CoinMetrics.Export.Storage
import           CoinMetrics.Export.Storage.AvroFile
import           CoinMetrics.Export.Storage.Elastic
import           CoinMetrics.Export.Storage.Postgres
import           CoinMetrics.Export.Storage.PostgresFile
import           CoinMetrics.Export.Storage.RabbitMQ
import           Hanalytics.Schema
import           Hanalytics.Schema.Postgres

newtype Options = Options
  { options_command :: OptionCommand
  }

data OptionCommand
  = OptionExportCommand { options_apis                      :: [Api]
                        , options_blockchain                :: !T.Text
                        , options_beginBlock                :: !BlockHeight
                        , options_endBlock                  :: !BlockHeight
                        , options_blocksFile                :: !(Maybe String)
                        , options_continue                  :: !Bool
                        , options_trace                     :: !Bool
                        , options_excludeUnaccountedActions :: !Bool
                        , options_output                    :: !Output
                        , options_threadsCount              :: !Int
                        , options_ignoreMissingBlocks       :: !Bool }
  | OptionExportIotaCommand { options_apiUrl       :: !String
                            , options_syncDbFile   :: !String
                            , options_readDump     :: !Bool
                            , options_output       :: !Output
                            , options_threadsCount :: !Int }
  | OptionPrintSchemaCommand { options_schema  :: !T.Text
                             , options_storage :: !T.Text }

data Api = Api
  { api_apiUrl         :: !String
  , api_apiUrlUserName :: !String
  , api_apiUrlPassword :: !String
  , api_apiUrlInsecure :: !Bool
  }

data Output = Output
  { output_avroFile         :: !(Maybe String)
  , output_postgresFile     :: !(Maybe String)
  , output_elasticFile      :: !(Maybe String)
  , output_postgres         :: !(Maybe String)
  , output_rabbitmq         :: !(Maybe String)
  , output_rabbitmqQueue    :: !(Maybe String)
  , output_rabbitmqExchange :: !String
  , output_elastic          :: !(Maybe String)
  , output_postgresTable    :: !(Maybe String)
  , output_elasticIndex     :: !(Maybe String)
  , output_packSize         :: !Int
  , output_fileSize         :: !Int
  , output_fileExec         :: !(Maybe String)
  , output_upsert           :: !Bool
  , output_flat             :: !Bool
  }

-- | Helper struct for output storage.
data OutputStorage = OutputStorage
  { os_storage       :: !SomeExportStorage
  , os_skipBlocks    :: {-# UNPACK #-}!Int
  , os_destFunc      :: !(BlockHeight -> String)
  , os_fileExec      :: !(String -> IO ())
  , os_isBlockStored :: !(BlockHash -> IO Bool)
  }

-- | Helper struct for output storages.
data OutputStorages = OutputStorages
  { oss_storages :: ![OutputStorage]
  , oss_packSize :: {-# UNPACK #-}!Int
  , oss_fileSize :: {-# UNPACK #-}!Int
  , oss_flat     :: !Bool
  }

initOutputStorages :: H.Manager -> Output -> T.Text ->  T.Text -> [T.Text] -> IO OutputStorages
initOutputStorages httpManager Output
  { output_avroFile = maybeOutputAvroFile
  , output_postgresFile = maybeOutputPostgresFile
  , output_elasticFile = maybeOutputElasticFile
  , output_postgres = maybeOutputPostgres
  , output_rabbitmq = maybeOutputRabbitmq
  , output_rabbitmqQueue = maybeOutputRabbitmqQueueName
  , output_rabbitmqExchange = outputRabbitmqExchangeName
  , output_elastic = maybeOutputElastic
  , output_postgresTable = maybePostgresTable
  , output_elasticIndex = maybeElasticIndex
  , output_packSize = packSize
  , output_fileSize = fileSize
  , output_fileExec = maybeFileExec
  , output_upsert = upsert
  , output_flat = flat
  } defaultTableName primaryField flattenSuffixes = mkOutputStorages . map mkOutputStorage . concat <$> sequence
  [ case maybeOutputAvroFile of
    Just outputAvroFile -> initStorage (Proxy @AvroFileExportStorage) mempty (mkFileDestFunc outputAvroFile)
    Nothing -> return []
  , case maybeOutputPostgresFile of
    Just outputPostgresFile -> initStorage (Proxy @PostgresFileExportStorage) postgresTableName (mkFileDestFunc outputPostgresFile)
    Nothing -> return []
  , case maybeOutputElasticFile of
    Just outputElasticFile -> initStorage (Proxy @ElasticFileExportStorage) elasticIndexName (mkFileDestFunc outputElasticFile)
    Nothing -> return []
  , case maybeOutputPostgres of
    Just outputPostgres -> initStorage (Proxy @PostgresExportStorage) postgresTableName (const outputPostgres)
    Nothing -> return []
  , case maybeOutputRabbitmq of
    Just outputRabbitmq -> initStorage (Proxy @RabbitMQExportStorage) amqpQueueExchangeName (const outputRabbitmq)
    Nothing -> return []
  , case maybeOutputElastic of
    Just outputElastic -> initStorage (Proxy @ElasticExportStorage) elasticIndexName (const outputElastic)
    Nothing -> return []
  ]
  where
    initStorage :: ExportStorage s => Proxy s -> T.Text -> (BlockHeight -> String) -> IO [(SomeExportStorage, BlockHeight -> String)]
    initStorage p table destFunc = let
      f :: ExportStorage s => Proxy s -> ExportStorageOptions -> IO s
      f Proxy = initExportStorage
      in do
        storage <- f p ExportStorageOptions
          { eso_httpManager = httpManager
          , eso_tables = if flat
            then map ((table <> "_") <>) flattenSuffixes
            else [table]
          , eso_primaryField = primaryField
          , eso_upsert = upsert
          }
        return [(SomeExportStorage storage, destFunc)]

    mkOutputStorage (storage, destFunc) = OutputStorage
      { os_storage = storage
      , os_skipBlocks = 0
      , os_destFunc = destFunc
      , os_fileExec = case maybeFileExec of
        Just fileExec -> \destination -> P.callCommand $ T.unpack $ T.replace "%F" (T.pack destination) (T.pack fileExec)
        Nothing -> const $ return ()
      , os_isBlockStored = \_ -> return False
      }

    mkOutputStorages storages = OutputStorages
      { oss_storages = storages
      , oss_packSize = packSize
      , oss_fileSize = fileSize
      , oss_flat = flat
      }

    mkFileDestFunc template beginBlock = T.unpack $ T.replace "%N" (T.pack $ show beginBlock) (T.pack template)

    postgresTableName = maybe defaultTableName T.pack maybePostgresTable
    amqpQueueExchangeName = maybe
      (mkAmqpNames (T.unpack defaultTableName) outputRabbitmqExchangeName)
      (flip mkAmqpNames outputRabbitmqExchangeName)
      maybeOutputRabbitmqQueueName
    mkAmqpNames queue exchange = T.pack (queue <> ":" <> exchange)
    elasticIndexName = maybe defaultTableName T.pack maybeElasticIndex

-- | Get minimum begin block among storages, and initialize them with skip blocks values.
-- All storages must support getting max block.
initContinuingOutputStorages :: OutputStorages -> BlockHeight -> IO (OutputStorages, BlockHeight)
initContinuingOutputStorages outputStorages@OutputStorages
  { oss_storages = storages
  } defaultBeginBlock = do
  getBeginBlockFuncs <- forM storages $ \OutputStorage
    { os_storage = SomeExportStorage storage
    , os_destFunc = destFunc
    } -> do
    getBeginBlockFunc <- maybe (fail "output storage does not support getting max block") return $ getExportStorageMaxBlock storage ExportStorageParams
      { esp_destination = destFunc defaultBeginBlock
      }
    return $ maybe defaultBeginBlock (max defaultBeginBlock . (+ 1)) <$> getBeginBlockFunc
  beginBlocks <- sequence getBeginBlockFuncs
  logStrLn $ "continuing from blocks: " <> show beginBlocks
  let beginBlock = if null beginBlocks then defaultBeginBlock else minimum beginBlocks
  isBlockStoredFuncs <- forM storages $ \OutputStorage
    { os_storage = SomeExportStorage storage
    , os_destFunc = destFunc
    } -> maybe (fail "output storage does not support isBlockStored") return $ isBlockStored storage ExportStorageParams
           { esp_destination = destFunc defaultBeginBlock }
  let triple s b f = (s,b,f)
  let triplets = zipWith3 triple storages beginBlocks isBlockStoredFuncs
  return
    ( outputStorages
      { oss_storages = map (\(storage, storageBeginBlock, iBS) -> storage
        { os_skipBlocks = fromIntegral $ storageBeginBlock - beginBlock
        , os_isBlockStored = iBS
        }) triplets
      }
    , beginBlock
    )

initRealtimeOutputStorages :: OutputStorages -> BlockHeight -> IO OutputStorages
initRealtimeOutputStorages outputStorages@OutputStorages
  { oss_storages = storages
  } defaultBeginBlock = do
  isBlockStoredFuncs <- forM storages $ \OutputStorage
    { os_storage = SomeExportStorage storage
    , os_destFunc = destFunc
    } -> maybe (fail "output storage does not support isBlockStored") return $ isBlockStored storage ExportStorageParams
           { esp_destination = destFunc defaultBeginBlock }
  return outputStorages
    { oss_storages = zipWith (\storage iBS -> storage
      { os_isBlockStored = iBS
      }) storages isBlockStoredFuncs
    }

writeToOutputStorages :: (Schemable a, A.ToAvro a, ToPostgresText a, J.ToJSON a) => OutputStorages -> ([a] -> [SomeBlocks]) -> BlockHeight -> [a] -> IO ()
writeToOutputStorages OutputStorages
  { oss_storages = storages
  , oss_packSize = packSize
  , oss_fileSize = fileSize
  , oss_flat = flat
  } flattenPack beginBlock blocks = do
  vars <- forM storages $ \outputStorage@OutputStorage
    { os_skipBlocks = skipBlocks
    } -> do
    var <- newTVarIO Nothing
    void $ forkFinally (writeToStorage outputStorage $ splitBlocks skipBlocks blocks) $ atomically . writeTVar var . Just
    return var
  results <- atomically $ do
    results <- mapM readTVar vars
    unless (all isJust results || any (maybe False isLeft) results) retry
    return results
  let erroredResults = concatMap (maybe [] (either pure (const []))) results
  unless (null erroredResults) $ do
    logPrint erroredResults
    fail "output failed"

  where
    writeToStorage OutputStorage
      { os_storage = SomeExportStorage storage
      , os_destFunc = destFunc
      , os_fileExec = fileExec
      } = mapM_ $ \(fileBeginBlock, packs) -> do
      let
        destination = destFunc fileBeginBlock
        storageParams = ExportStorageParams
          { esp_destination = destination
          }
      if flat
        then writeExportStorageSomeBlocks storage storageParams $ map flattenPack packs
        else writeExportStorage storage storageParams packs
      fileExec destination

    splitWithSize n = \case
      [] -> []
      (splitAt n -> (a, b)) -> a : splitWithSize n b

    splitBlocks skipBlocks
      = zipWith (\fileIndex file ->
        ( fromIntegral $ fromIntegral beginBlock + skipBlocks + fileIndex * fileSize
        , splitWithSize packSize file
        )) [0..]
      -- next line when fileSize > 0 causes space leak :( fixed by -O0 in .cabal
      -- TODO: figure out why
      . (if fileSize > 0 then splitWithSize fileSize else pure)
      . drop skipBlocks

logStrLn :: String -> IO ()
logStrLn str = withMVar logStrMVar $ \_ -> hPutStrLn stderr str

logPrint :: Show a => a -> IO ()
logPrint = logStrLn . show

{-# NOINLINE logStrMVar #-}
logStrMVar :: MVar ()
logStrMVar = unsafePerformIO $ newMVar ()

prepare :: IO ()
prepare = do
  hSetBuffering stdout LineBuffering
  hSetBuffering stderr LineBuffering

parser :: O.ParserInfo Options
parser =
  O.info
    (O.helper <*> opts)
    (O.fullDesc <> O.progDesc "Exports blocks from blockchains into files" <>
      O.header "coinmetrics-export")

opts :: O.Parser Options
opts =
  Options <$>
  O.subparser
    (O.command
        "export"
        (O.info
          (O.helper <*>
            (OptionExportCommand <$> O.many optionApi <*>
            O.strOption
              (O.long "blockchain" <> O.metavar "BLOCKCHAIN" <>
                O.help ("Type of blockchain: " <> blockchainTypesStr)) <*>
            O.option
              O.auto
              (O.long "begin-block" <> O.metavar "BEGIN_BLOCK" <> O.value (-1) <>
                O.help "Begin block number (inclusive)") <*>
            O.option
              O.auto
              (O.long "end-block" <> O.value 0 <> O.metavar "END_BLOCK" <>
                O.help
                  "End block number if positive (exclusive), offset to top block if negative, default offset to top block if zero") <*>
            O.option
              (O.maybeReader (Just . Just))
              (O.long "blocks-file" <> O.metavar "BLOCKS_FILE" <>
                O.value Nothing <>
                O.help "File name with block numbers to export") <*>
            O.switch
              (O.long "continue" <>
                O.help
                  "Get BEGIN_BLOCK from output, to continue after latest written block. Works with --output-postgres only") <*>
            O.switch
              (O.long "trace" <>
                O.help "Perform tracing of actions performed by transactions") <*>
            O.switch
              (O.long "exclude-unaccounted-actions" <>
                O.help "When --trace is on, exclude unaccounted actions") <*>
            optionOutput <*>
            O.option
              O.auto
              (O.long "threads" <> O.value 1 <> O.showDefault <>
                O.metavar "THREADS" <>
                O.help "Threads count") <*>
            O.switch
              (O.long "ignore-missing-blocks" <>
                O.help "Ignore errors when getting blocks from daemon")))
          (O.fullDesc <> O.progDesc "Export blockchain")) <>
      O.command
        "export-iota"
        (O.info
          (O.helper <*>
            (OptionExportIotaCommand <$>
            O.strOption
              (O.long "api-url" <> O.metavar "API_URL" <>
                O.value "http://127.0.0.1:14265/" <>
                O.showDefault <>
                O.help "IOTA API url, like \"http://<host>:<port>/\"") <*>
            O.strOption
              (O.long "sync-db" <> O.metavar "SYNC_DB" <> O.help "Sync DB file") <*>
            O.switch
              (O.long "read-dump" <> O.help "Read transaction dump from stdin") <*>
            optionOutput <*>
            O.option
              O.auto
              (O.long "threads" <> O.value 1 <> O.showDefault <>
                O.metavar "THREADS" <>
                O.help "Threads count")))
          (O.fullDesc <> O.progDesc "Export IOTA data")) <>
      O.command
        "print-schema"
        (O.info
          (O.helper <*>
            (OptionPrintSchemaCommand <$>
            O.strOption
              (O.long "schema" <> O.metavar "SCHEMA" <>
                O.help ("Type of schema: " <> blockchainTypesStr)) <*>
            O.strOption
              (O.long "storage" <> O.metavar "STORAGE" <>
                O.help "Storage type: postgres | bigquery")))
          (O.fullDesc <> O.progDesc "Prints schema")))

optionApi :: O.Parser Api
optionApi =
  Api <$>
  O.strOption
    (O.long "api-url" <> O.metavar "API_URL" <>
      O.help "Blockchain API url, like \"http://<host>:<port>/\"") <*>
  O.strOption
    (O.long "api-url-username" <> O.metavar "API_URL_USERNAME" <> O.value "" <>
      O.help "Blockchain API url username for authentication") <*>
  O.strOption
    (O.long "api-url-password" <> O.metavar "API_URL_PASSWORD" <> O.value "" <>
      O.help "Blockchain API url password for authentication") <*>
  O.switch
    (O.long "api-url-insecure" <> O.help "Do not validate HTTPS certificate")

optionOutput :: O.Parser Output
optionOutput =
  Output <$>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-avro-file" <> O.value Nothing <>
      O.metavar "OUTPUT_AVRO_FILE" <>
      O.help "Output Avro file") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-postgres-file" <> O.value Nothing <>
      O.metavar "OUTPUT_POSTGRES_FILE" <>
      O.help "Output PostgreSQL file") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-elastic-file" <> O.value Nothing <>
      O.metavar "OUTPUT_ELASTIC_FILE" <>
      O.help "Output ElasticSearch JSON file") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-postgres" <> O.value Nothing <> O.metavar "OUTPUT_POSTGRES" <>
      O.help "Output directly to PostgreSQL DB") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-rabbitmq" <> O.value Nothing <> O.metavar "OUTPUT_RABBITMQ" <>
      O.help "Connection string like amqp://user:pass@host:10000/vhost") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-rabbitmq-queue-name" <> O.value Nothing <>
      O.metavar "OUTPUT_RABBITMQ_QUEUE" <>
      O.help "Name of RabbitMQ Queue") <*>
  O.strOption
    (O.long "output-rabbitmq-exchange-name" <> O.value "export" <>
      O.metavar "OUTPUT_RABBITMQ_EXCHANGE" <>
      O.help "Name of RabbitMQ exchange (queue required)") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-elastic" <> O.value Nothing <> O.metavar "OUTPUT_ELASTIC" <>
      O.help "Output directly to ElasticSearch") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-postgres-table" <> O.value Nothing <>
      O.metavar "OUTPUT_POSTGRES_TABLE" <>
      O.help "Table name for PostgreSQL output") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "output-elastic-index" <> O.value Nothing <>
      O.metavar "OUTPUT_ELASTIC_INDEX" <>
      O.help "Index name for ElasticSearch output") <*>
  O.option
    O.auto
    (O.long "pack-size" <> O.value 100 <> O.showDefault <> O.metavar "PACK_SIZE" <>
      O.help "Number of records in pack (SQL INSERT command, or Avro block)") <*>
  O.option
    O.auto
    (O.long "file-size" <> O.value (-1) <> O.showDefault <>
      O.metavar "FILE_SIZE" <>
      O.help "Number of records per file (-1 means infinite)") <*>
  O.option
    (O.maybeReader (Just . Just))
    (O.long "file-exec" <> O.value Nothing <> O.metavar "FILE_EXEC" <>
      O.help
        "Command to execute per output file. Variable substitutions: %F - file name") <*>
  O.switch (O.long "upsert" <> O.help "Perform UPSERT instead of INSERT") <*>
  O.switch (O.long "flat" <> O.help "Use flat schema")
