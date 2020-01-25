{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns, TypeApplications #-}

module Main(main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL
import Data.Default
import qualified Data.DiskHash as DH
import Data.Either
import qualified Data.HashMap.Strict as HM
import Data.Maybe
import Data.Proxy
import qualified Data.Serialize as S
import Data.String
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.IO as TL
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Vector as V
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Network.Connection as NC
import qualified Network.HTTP.Client as H
import qualified Network.HTTP.Client.TLS as H
import qualified Options.Applicative as O
import System.Directory
import System.IO
import System.IO.Unsafe
import qualified System.Process as P

import CoinMetrics.BlockChain
import CoinMetrics.BlockChain.All
import CoinMetrics.Export.Storage
import CoinMetrics.Export.Storage.AvroFile
import CoinMetrics.Export.Storage.Elastic
import CoinMetrics.Export.Storage.Postgres
import CoinMetrics.Export.Storage.PostgresFile
import CoinMetrics.Export.Storage.RabbitMQ
import CoinMetrics.Iota
import Hanalytics.Schema
import Hanalytics.Schema.BigQuery
import Hanalytics.Schema.Postgres

main :: IO ()
main = do
  prepare
  run =<< O.execParser parser
  where
  parser = O.info (O.helper <*> opts)
    (  O.fullDesc
    <> O.progDesc "Exports blocks from blockchains into files"
    <> O.header "coinmetrics-export"
    )
  opts = Options
    <$> O.subparser
      (  O.command "export"
        (  O.info
          (O.helper <*> (OptionExportCommand
            <$> O.many optionApi
            <*> O.strOption
              (  O.long "blockchain"
              <> O.metavar "BLOCKCHAIN"
              <> O.help ("Type of blockchain: " <> blockchainTypesStr)
              )
            <*> O.option O.auto
              (  O.long "begin-block"
              <> O.metavar "BEGIN_BLOCK"
              <> O.value (-1)
              <> O.help "Begin block number (inclusive)"
              )
            <*> O.option O.auto
              (  O.long "end-block"
              <> O.value 0
              <> O.metavar "END_BLOCK"
              <> O.help "End block number if positive (exclusive), offset to top block if negative, default offset to top block if zero"
              )
            <*> O.option (O.maybeReader (Just . Just))
              (  O.long "blocks-file"
              <> O.metavar "BLOCKS_FILE"
              <> O.value Nothing
              <> O.help "File name with block numbers to export"
              )
            <*> O.switch
              (  O.long "continue"
              <> O.help "Get BEGIN_BLOCK from output, to continue after latest written block. Works with --output-postgres only"
              )
            <*> O.switch
              (  O.long "trace"
              <> O.help "Perform tracing of actions performed by transactions"
              )
            <*> O.switch
              (  O.long "exclude-unaccounted-actions"
              <> O.help "When --trace is on, exclude unaccounted actions"
              )
            <*> optionOutput
            <*> O.option O.auto
              (  O.long "threads"
              <> O.value 1 <> O.showDefault
              <> O.metavar "THREADS"
              <> O.help "Threads count"
              )
            <*> O.switch
              (  O.long "ignore-missing-blocks"
              <> O.help "Ignore errors when getting blocks"
              )
            <*> O.option O.auto
              (  O.long "request-timeout"
              <> O.value 60 <> O.showDefault
              <> O.metavar "REQUEST_TIMEOUT"
              <> O.help "Timeout for requests to full nodes"
              )
          )) (O.fullDesc <> O.progDesc "Export blockchain")
        )
      <> O.command "export-iota"
        (  O.info
          (O.helper <*> (OptionExportIotaCommand
            <$> O.strOption
              (  O.long "api-url"
              <> O.metavar "API_URL"
              <> O.value "http://127.0.0.1:14265/" <> O.showDefault
              <> O.help "IOTA API url, like \"http://<host>:<port>/\""
              )
            <*> O.strOption
              (  O.long "sync-db"
              <> O.metavar "SYNC_DB"
              <> O.help "Sync DB file"
              )
            <*> O.switch
              (  O.long "read-dump"
              <> O.help "Read transaction dump from stdin"
              )
            <*> optionOutput
            <*> O.option O.auto
              (  O.long "threads"
              <> O.value 1 <> O.showDefault
              <> O.metavar "THREADS"
              <> O.help "Threads count"
              )
            <*> O.option O.auto
              (  O.long "request-timeout"
              <> O.value 60 <> O.showDefault
              <> O.metavar "REQUEST_TIMEOUT"
              <> O.help "Timeout for requests to full nodes"
              )
          )) (O.fullDesc <> O.progDesc "Export IOTA data")
        )
      <> O.command "print-schema"
        (  O.info
          (O.helper <*> (OptionPrintSchemaCommand
            <$> O.strOption
              (  O.long "schema"
              <> O.metavar "SCHEMA"
              <> O.help ("Type of schema: " <> blockchainTypesStr)
              )
            <*> O.strOption
              (  O.long "storage"
              <> O.metavar "STORAGE"
              <> O.help "Storage type: postgres | bigquery"
              )
          )) (O.fullDesc <> O.progDesc "Prints schema")
        )
      )
  optionApi = Api
    <$> O.strOption
      (  O.long "api-url"
      <> O.metavar "API_URL"
      <> O.help "Blockchain API url, like \"http://<host>:<port>/\""
      )
    <*> O.strOption
      (  O.long "api-url-username"
      <> O.metavar "API_URL_USERNAME"
      <> O.value ""
      <> O.help "Blockchain API url username for authentication"
      )
    <*> O.strOption
      (  O.long "api-url-password"
      <> O.metavar "API_URL_PASSWORD"
      <> O.value ""
      <> O.help "Blockchain API url password for authentication"
      )
    <*> O.switch
      (  O.long "api-url-insecure"
      <> O.help "Do not validate HTTPS certificate"
      )
  optionOutput = Output
    <$> O.option (O.maybeReader (Just . Just))
      (  O.long "output-avro-file"
      <> O.value Nothing
      <> O.metavar "OUTPUT_AVRO_FILE"
      <> O.help "Output Avro file"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-postgres-file"
      <> O.value Nothing
      <> O.metavar "OUTPUT_POSTGRES_FILE"
      <> O.help "Output PostgreSQL file"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-elastic-file"
      <> O.value Nothing
      <> O.metavar "OUTPUT_ELASTIC_FILE"
      <> O.help "Output ElasticSearch JSON file"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-postgres"
      <> O.value Nothing
      <> O.metavar "OUTPUT_POSTGRES"
      <> O.help "Output directly to PostgreSQL DB"
      )
      <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-rabbitmq"
      <> O.value Nothing
      <> O.metavar "OUTPUT_RABBITMQ"
      <> O.help "Connection string like amqp://user:pass@host:10000/vhost"
      )
      <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-rabbitmq-queue-name"
      <> O.value Nothing
      <> O.metavar "OUTPUT_RABBITMQ_QUEUE"
      <> O.help "Name of RabbitMQ Queue"
      )
      <*> O.strOption
      (  O.long "output-rabbitmq-exchange-name"
      <> O.value "export"
      <> O.metavar "OUTPUT_RABBITMQ_EXCHANGE"
      <> O.help "Name of RabbitMQ exchange (queue required)"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-elastic"
      <> O.value Nothing
      <> O.metavar "OUTPUT_ELASTIC"
      <> O.help "Output directly to ElasticSearch"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-postgres-table"
      <> O.value Nothing
      <> O.metavar "OUTPUT_POSTGRES_TABLE"
      <> O.help "Table name for PostgreSQL output"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "output-elastic-index"
      <> O.value Nothing
      <> O.metavar "OUTPUT_ELASTIC_INDEX"
      <> O.help "Index name for ElasticSearch output"
      )
    <*> O.option O.auto
      (  O.long "pack-size"
      <> O.value 100 <> O.showDefault
      <> O.metavar "PACK_SIZE"
      <> O.help "Number of records in pack (SQL INSERT command, or Avro block)"
      )
    <*> O.option O.auto
      (  O.long "file-size"
      <> O.value (-1) <> O.showDefault
      <> O.metavar "FILE_SIZE"
      <> O.help "Number of records per file (-1 means infinite)"
      )
    <*> O.option (O.maybeReader (Just . Just))
      (  O.long "file-exec"
      <> O.value Nothing
      <> O.metavar "FILE_EXEC"
      <> O.help "Command to execute per output file. Variable substitutions: %F - file name"
      )
    <*> O.switch
      (  O.long "upsert"
      <> O.help "Perform UPSERT instead of INSERT"
      )
    <*> O.switch
      (  O.long "flat"
      <> O.help "Use flat schema"
      )

newtype Options = Options
  { options_command :: OptionCommand
  }

data OptionCommand
  = OptionExportCommand
    { options_apis :: [Api]
    , options_blockchain :: !T.Text
    , options_beginBlock :: !BlockHeight
    , options_endBlock :: !BlockHeight
    , options_blocksFile :: !(Maybe String)
    , options_continue :: !Bool
    , options_trace :: !Bool
    , options_excludeUnaccountedActions :: !Bool
    , options_output :: !Output
    , options_threadsCount :: !Int
    , options_ignoreMissingBlocks :: !Bool
    , options_requestTimeout ::  !Int
    }
  | OptionExportIotaCommand
    { options_apiUrl :: !String
    , options_syncDbFile :: !String
    , options_readDump :: !Bool
    , options_output :: !Output
    , options_threadsCount :: !Int
    , options_requestTimeout ::  !Int
    }
  | OptionPrintSchemaCommand
    { options_schema :: !T.Text
    , options_storage :: !T.Text
    }

data Api = Api
  { api_apiUrl :: !String
  , api_apiUrlUserName :: !String
  , api_apiUrlPassword :: !String
  , api_apiUrlInsecure :: !Bool
  }

data Output = Output
  { output_avroFile :: !(Maybe String)
  , output_postgresFile :: !(Maybe String)
  , output_elasticFile :: !(Maybe String)
  , output_postgres :: !(Maybe String)
  , output_rabbitmq :: !(Maybe String)
  , output_rabbitmqQueue :: !(Maybe String)
  , output_rabbitmqExchange :: !String
  , output_elastic :: !(Maybe String)
  , output_postgresTable :: !(Maybe String)
  , output_elasticIndex :: !(Maybe String)
  , output_packSize :: !Int
  , output_fileSize :: !Int
  , output_fileExec :: !(Maybe String)
  , output_upsert :: !Bool
  , output_flat :: !Bool
  }

run :: Options -> IO ()
run Options
  { options_command = command
  } = case command of

  OptionExportCommand
    { options_apis = apisByUser
    , options_blockchain = blockchainType
    , options_beginBlock = maybeBeginBlock
    , options_endBlock = maybeEndBlock
    , options_blocksFile = maybeBlocksFile
    , options_continue = continue
    , options_trace = trace
    , options_excludeUnaccountedActions = excludeUnaccountedActions
    , options_output = output
    , options_threadsCount = threadsCount
    , options_ignoreMissingBlocks = ignoreMissingBlocks
    , options_requestTimeout = requestTimeout
    } -> do
    -- get blockchain info by name
    SomeBlockChainInfo BlockChainInfo
      { bci_init = initBlockChain
      , bci_defaultApiUrls = defaultApiUrls
      , bci_defaultBeginBlock = defaultBeginBlock
      , bci_defaultEndBlock = defaultEndBlock
      , bci_heightFieldName = heightFieldName
      , bci_flattenSuffixes = flattenSuffixes
      , bci_flattenPack = flattenPack
      } <- maybe (fail "wrong blockchain type") return $ getSomeBlockChainInfo blockchainType

    let
      apis = if null apisByUser
        then map (\defaultApiUrl -> Api
          { api_apiUrl = defaultApiUrl
          , api_apiUrlUserName = ""
          , api_apiUrlPassword = ""
          , api_apiUrlInsecure = False
          }) defaultApiUrls
        else apisByUser
      apisCount = length apis
      httpManagerConnCount = (apisCount * threadsCount + 1) * 2

    -- init http managers
    httpSecureManager <- H.newTlsManagerWith H.tlsManagerSettings
      { H.managerConnCount = httpManagerConnCount
      , H.managerResponseTimeout = H.responseTimeoutMicro $ requestTimeout * 1000000
      }
    httpInsecureManager <- H.newTlsManagerWith (H.mkManagerSettings def
      { NC.settingDisableCertificateValidation = True
      } Nothing)
      { H.managerConnCount = httpManagerConnCount
      , H.managerResponseTimeout = H.responseTimeoutMicro $ requestTimeout * 1000000
      }

    -- init blockchains
    blockchains <- forM apis $ \Api
      { api_apiUrl = apiUrl
      , api_apiUrlUserName = apiUrlUserName
      , api_apiUrlPassword = apiUrlPassword
      , api_apiUrlInsecure = apiUrlInsecure
      } -> do
      httpRequest <- do
        httpRequest <- H.parseRequest apiUrl
        return $ if not (null apiUrlUserName) || not (null apiUrlPassword)
          then H.applyBasicAuth (fromString apiUrlUserName) (fromString apiUrlPassword) httpRequest
          else httpRequest
      initBlockChain BlockChainParams
        { bcp_httpManager = if apiUrlInsecure then httpInsecureManager else httpSecureManager
        , bcp_httpRequest = httpRequest
        , bcp_trace = trace
        , bcp_excludeUnaccountedActions = excludeUnaccountedActions
        , bcp_threadsCount = threadsCount
        }

    -- init output storages and begin block
    (outputStorages, beginBlock) <- do
      outputStorages <- initOutputStorages httpSecureManager output blockchainType heightFieldName flattenSuffixes
      let specifiedBeginBlock = if maybeBeginBlock >= 0 then maybeBeginBlock else defaultBeginBlock
      if continue
        then initContinuingOutputStorages outputStorages specifiedBeginBlock
        else return (outputStorages, specifiedBeginBlock)

    let endBlock = if maybeEndBlock == 0 then defaultEndBlock else maybeEndBlock

    -- simple multithreaded pipeline
    let queueSize = apisCount * threadsCount * 2
    blockIndexQueue <- newTBQueueIO $ fromIntegral queueSize -- queue of (index, nextIndex) items
    blockIndexQueueEndedVar <- newTVarIO False

    -- upper limits of blockchains
    blockchainsLimitVars <- forM blockchains $ \_ -> newTVarIO 0

    -- if end block is fixed, or blocks file is specified, just add indices without checking for blockchains limits
    if endBlock > 0
      then void $ forkIO $ do
        blocksIndices <- case maybeBlocksFile of
          Just blocksFile -> map (read . TL.unpack) . TL.lines <$> TL.readFile blocksFile
          Nothing -> return [beginBlock..(endBlock - 1)]
        mapM_ (atomically . writeTBQueue blockIndexQueue) $ zip blocksIndices (tail blocksIndices ++ [-1])
        atomically $ writeTVar blockIndexQueueEndedVar True
    -- else blockchains add indices concurrently (but every index still can be added only once)
    else do
      nextIndexToAddVar <- newTVarIO beginBlock
      forM_ (zip blockchains blockchainsLimitVars) $ \(blockchain, blockchainLimitVar) -> forkIO $ let
        step = do
          -- determine current (known) block index
          eitherCurrentBlockIndex <- try $ getCurrentBlockHeight blockchain
          case eitherCurrentBlockIndex of
            Right currentBlockIndex -> do
              -- insert indices up to this index minus offset
              let endIndex = currentBlockIndex + endBlock + 1
              atomically $ writeTVar blockchainLimitVar endIndex
              logStrLn $ "continuously syncing blocks... up to " <> show (endIndex - 1)
              let
                f = join $ atomically $ do
                  nextIndexToAdd <- readTVar nextIndexToAddVar
                  if nextIndexToAdd < endIndex
                    then do
                      writeTBQueue blockIndexQueue (nextIndexToAdd, nextIndexToAdd + 1)
                      writeTVar nextIndexToAddVar (nextIndexToAdd + 1)
                      return f
                    else return $ return ()
                in f
            Left (SomeException err) -> logPrint err
          -- pause
          threadDelay 10000000
          -- repeat
          step
        in step

    blockQueue <- newTBQueueIO $ fromIntegral queueSize
    blockQueueNextBlockIndexVar <- newTVarIO beginBlock

    -- work threads getting blocks from blockchain
    forM_ (zip blockchains blockchainsLimitVars) $ \(blockchain, blockchainLimitVar) ->
      forM_ [1..threadsCount] $ \_ -> let
        step = do
          maybeBlockIndex <- atomically $ do
            maybeBlockIndex <- tryReadTBQueue blockIndexQueue
            case maybeBlockIndex of
              Just (blockIndex, nextBlockIndex) -> do
                blockchainLimit <- readTVar blockchainLimitVar
                if blockIndex < blockchainLimit || endBlock > 0
                  then return $ Just (blockIndex, nextBlockIndex)
                  else retry
              Nothing -> do
                blockIndexQueueEnded <- readTVar blockIndexQueueEndedVar
                if blockIndexQueueEnded
                  then return Nothing
                  else retry
          case maybeBlockIndex of
            Just (blockIndex, nextBlockIndex) -> do
              -- get block from blockchain
              eitherBlock <- try $ getBlockByHeight blockchain blockIndex
              case eitherBlock of
                Right block ->
                  -- insert block into block queue ensuring order
                  atomically $ do
                    blockQueueNextBlockIndex <- readTVar blockQueueNextBlockIndexVar
                    if blockIndex == blockQueueNextBlockIndex then do
                      writeTBQueue blockQueue (blockIndex, block)
                      writeTVar blockQueueNextBlockIndexVar nextBlockIndex
                    else retry
                Left (SomeException err) -> do
                  logPrint err
                  -- if it's allowed to ignore errors, do that
                  if ignoreMissingBlocks
                    then atomically $ do
                      blockQueueNextBlockIndex <- readTVar blockQueueNextBlockIndexVar
                      if blockIndex == blockQueueNextBlockIndex
                        then writeTVar blockQueueNextBlockIndexVar nextBlockIndex
                        else retry
                    -- otherwise rethrow error
                    else throwIO err
              -- repeat
              step
            Nothing -> return ()
        in forkIO step

    -- write blocks into outputs, using lazy IO
    let
      step i = if endBlock <= 0 || i < endBlock
        then unsafeInterleaveIO $ do
          (blockIndex, block) <- atomically $ readTBQueue blockQueue
          when (blockIndex `rem` 100 == 0) $ logStrLn $ "synced up to " <> show blockIndex
          (block :) <$> step (blockIndex + 1)
        else return []
    writeToOutputStorages outputStorages flattenPack beginBlock =<< step beginBlock
    logStrLn $ "sync from " <> show beginBlock <> " to " <> show (endBlock - 1) <> " complete"

  OptionExportIotaCommand
    { options_apiUrl = apiUrl
    , options_syncDbFile = syncDbFile
    , options_readDump = readDump
    , options_output = output@Output
      { output_postgres = maybeOutputPostgres
      , output_postgresTable = maybePostgresTable
      }
    , options_threadsCount = threadsCount
    , options_requestTimeout = requestTimeout
    } -> do
    httpManager <- H.newTlsManagerWith H.tlsManagerSettings
      { H.managerConnCount = threadsCount * 2
      , H.managerResponseTimeout = H.responseTimeoutMicro $ requestTimeout * 1000000
      }
    httpRequest <- H.parseRequest apiUrl
    let iota = newIota httpManager httpRequest

    outputStorages <- initOutputStorages httpManager output "iota" "hash" []

    -- simple multithreaded pipeline
    hashQueue <- newTQueueIO
    transactionQueue <- newTBQueueIO $ fromIntegral $ threadsCount * 2

    queueSizeVar <- newTVarIO 0 :: IO (TVar Int)

    -- thread working with sync db
    syncDbActionsQueue <- newTBQueueIO $ fromIntegral $ threadsCount * 2
    removeFile syncDbFile `catch` (\SomeException {} -> return ())
    void $ forkIO $ DH.withDiskHashRW syncDbFile 82 $ \syncDb -> forever $ do
      action <- atomically $ readTBQueue syncDbActionsQueue
      action syncDb

    outputPostgres <- maybe (fail "postgres output is mandatory") return maybeOutputPostgres

    let
      checkHash hash@(T.encodeUtf8 -> hashBytes) f = atomically $ writeTBQueue syncDbActionsQueue $ \syncDb ->
        do
          hashProcessed <- isJust <$> DH.htLookupRW hashBytes syncDb
          unless hashProcessed $ do
            ok <- DH.htInsert hashBytes () syncDb
            unless ok $ fail "can't write into sync db"

            -- check that hash is not in the database
            connection <- connectDb
            let query = "SELECT COUNT(*) FROM " <> maybe "iota" T.pack maybePostgresTable <> " WHERE \"hash\" = '" <> hash <> "'"
            result <- maybe (fail "cannot get hash status from postgres") return =<< PQ.execParams connection (T.encodeUtf8 query) [] PQ.Text
            resultStatus <- PQ.resultStatus result
            unless (resultStatus == PQ.TuplesOk) $ fail $ "cannot get hash status from postgres: " <> show resultStatus
            tuplesCount <- PQ.ntuples result
            unless (tuplesCount == 1) $ fail "cannot decode tuples from postgres"
            inDatabase <- maybe False (/= "0") <$> PQ.getvalue result 0 0
            PQ.finish connection

            unless inDatabase f

      connectDb = do
        connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack outputPostgres
        connectionStatus <- PQ.status connection
        unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
        return connection

      addHash hash = checkHash hash $ atomically $ do
        writeTQueue hashQueue hash
        modifyTVar' queueSizeVar (+ 1)

      addTransaction transaction@IotaTransaction
        { it_hash = hash
        } = checkHash hash $ atomically $ writeTBQueue transactionQueue transaction

      takeHashes limit = let
        step n hashes = if n <= (0 :: Int) then return hashes else do
          maybeHash <- tryReadTQueue hashQueue
          case maybeHash of
            Just hash -> do
              modifyTVar' queueSizeVar (subtract 1)
              step (n - 1) (hash : hashes)
            Nothing -> return hashes
        in step limit []

    -- read dumps from stdin
    when readDump $ void $ forkIO $ let
      step i = do
        maybeLine <- either (\SomeException {} -> return Nothing) (return . Just) =<< try T.getLine
        case maybeLine of
          Just (T.splitOn "," -> [transactionHash@(T.length -> 81), transactionData@(T.length -> 2673)]) -> do
            transaction <- either (fail "failed to parse transaction from dump") return $ S.runGet (deserIotaTransaction transactionHash) $ T.encodeUtf8 transactionData
            addTransaction transaction
            step $ i + 1
          Nothing -> logStrLn $ "read " <> show i <> " transactions from dump"
          _ -> fail "wrong line in transaction dump"
      in step (0 :: Int)

    -- initial hashes (ones pointed by other hashes, but not in the database)
    unless readDump $ do
      connection <- connectDb
      let tableName = maybe "iota" T.pack maybePostgresTable
      let query = "SELECT q.\"hash\" FROM ( \
        \ (SELECT t.\"trunkTransaction\" \"hash\" FROM " <> tableName <> " t LEFT JOIN " <> tableName <> " a ON t.\"trunkTransaction\" = a.\"hash\" WHERE t.\"trunkTransaction\" IS NOT NULL AND a IS NULL) \
        \ UNION \
        \ (SELECT t.\"branchTransaction\" \"hash\" FROM " <> tableName <> " t LEFT JOIN " <> tableName <> " b ON t.\"branchTransaction\" = b.\"hash\" WHERE t.\"branchTransaction\" IS NOT NULL AND b IS NULL) \
        \ ) q"
      result <- maybe (fail "cannot get initial hashes from postgres") return =<< PQ.execParams connection (T.encodeUtf8 query) [] PQ.Text
      resultStatus <- PQ.resultStatus result
      unless (resultStatus == PQ.TuplesOk) $ fail $ "cannot get initial hashes from postgres: " <> show resultStatus
      tuplesCount <- PQ.ntuples result
      let
        step i = when (i < tuplesCount) $ do
          hash <- maybe (fail "failed to get initial hash from postgres") (return . T.decodeUtf8) =<< PQ.getvalue result i 0
          addHash hash
          step $ i + 1
        in step 0
      PQ.finish connection

    -- thread adding milestones to hash queue
    unless readDump $ void $ forkIO $ forever $ do
      -- get milestone
      milestones <- iotaGetMilestones iota
      logStrLn $ "milestones: " <> show milestones
      -- put milestones into queue
      mapM_ addHash milestones
      -- output queue size
      queueSize <- readTVarIO queueSizeVar
      logStrLn $ "queue size: " <> show queueSize
      -- pause
      threadDelay 10000000

    -- work threads getting transactions from blockchain
    unless readDump $ forM_ [1..threadsCount] $ const $ forkIO $ forever $ do
      hashes <- atomically $ do
        hashes <- V.fromList <$> takeHashes 10
        when (V.null hashes) retry
        return hashes
      transactions <- iotaGetTransactions iota hashes
      forM_ transactions $ \transaction@IotaTransaction
        { it_trunkTransaction = trunkTransaction
        , it_branchTransaction = branchTransaction
        } -> do
        atomically $ writeTBQueue transactionQueue transaction
        addHash trunkTransaction
        addHash branchTransaction

    -- write blocks into outputs, using lazy IO
    let
      step i = unsafeInterleaveIO $ do
        transaction <- atomically $ readTBQueue transactionQueue
        when (i `rem` 100 == 0) $ logStrLn $ "synced transactions: " <> show i
        (transaction :) <$> step (i + 1)
      in writeToOutputStorages outputStorages (pure . SomeBlocks) 0 =<< step (0 :: Int)

  OptionPrintSchemaCommand
    { options_schema = schemaTypeStr
    , options_storage = storageTypeStr
    } -> case getSomeBlockChainInfo schemaTypeStr of
    Just (SomeBlockChainInfo BlockChainInfo
      { bci_schemas = (HM.lookup storageTypeStr -> Just schema)
      }) -> T.putStrLn schema
    _ -> case (schemaTypeStr, storageTypeStr) of
      ("iota", "postgres") -> do
        putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
          [ schemaOf (Proxy @IotaTransaction)
          ]
        putStrLn $ T.unpack "CREATE TABLE \"iota\" OF \"IotaTransaction\" (PRIMARY KEY (\"hash\"));"
      ("iota", "bigquery") ->
        putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy IotaTransaction)
      _ -> fail "wrong pair schema+storage"

-- | Helper struct for output storage.
data OutputStorage = OutputStorage
  { os_storage :: !SomeExportStorage
  , os_skipBlocks :: {-# UNPACK #-} !Int
  , os_destFunc :: !(BlockHeight -> String)
  , os_fileExec :: !(String -> IO ())
  }

-- | Helper struct for output storages.
data OutputStorages = OutputStorages
  { oss_storages :: ![OutputStorage]
  , oss_packSize :: {-# UNPACK #-} !Int
  , oss_fileSize :: {-# UNPACK #-} !Int
  , oss_flat :: !Bool
  }

initOutputStorages :: H.Manager -> Output -> T.Text -> T.Text -> [T.Text] -> IO OutputStorages
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
  return
    ( outputStorages
      { oss_storages = zipWith (\storage storageBeginBlock -> storage
        { os_skipBlocks = fromIntegral $ storageBeginBlock - beginBlock
        }) storages beginBlocks
      }
    , beginBlock
    )

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
