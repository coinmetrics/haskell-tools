{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns, TypeApplications #-}

module Main(main) where

import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Exception
import           Control.Monad
import qualified Data.Aeson                              as J
import qualified Data.ByteString.Lazy                    as BL
import           Data.Default
import qualified Data.DiskHash                           as DH
import qualified Data.HashMap.Strict                     as HM
import           Data.Maybe
import           Data.Proxy
import qualified Data.Serialize                          as S
import           Data.String
import qualified Data.Text                               as T
import qualified Data.Text.Encoding                      as T
import qualified Data.Text.IO                            as T
import qualified Data.Text.Lazy                          as TL
import qualified Data.Text.Lazy.IO                       as TL
import qualified Data.Text.Lazy.Builder                  as TL
import qualified Data.Vector                             as V
import qualified Database.PostgreSQL.LibPQ               as PQ
import qualified Network.Connection                      as NC
import qualified Network.HTTP.Client                     as H
import qualified Network.HTTP.Client.TLS                 as H
import           NQE
import qualified Options.Applicative                     as O
import           System.Directory
import           System.IO.Unsafe

import           Actors 
import           CoinMetrics.BlockChain
import           CoinMetrics.BlockChain.All
import           CoinMetrics.Iota
import           Hanalytics.Schema
import           Hanalytics.Schema.BigQuery
import           Hanalytics.Schema.Postgres
import           Options

main :: IO ()
main = do
  prepare
  run =<< O.execParser parser

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
    } -> do
    -- get blockchain info by name
    SomeBlockChainInfo BlockChainInfo
      { bci_init = initBlockChain
      , bci_defaultApiUrls = defaultApiUrls
      , bci_defaultBeginBlock = defaultBeginBlock
      , bci_defaultEndBlock = defaultEndBlock
      , bci_heightFieldName = heightFieldName
      , bci_hashFieldName = hashFieldName
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
      }
    httpInsecureManager <- H.newTlsManagerWith (H.mkManagerSettings def
      { NC.settingDisableCertificateValidation = True
      } Nothing)
      { H.managerConnCount = httpManagerConnCount
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
      outputStorages <- initOutputStorages httpSecureManager output blockchainType heightFieldName hashFieldName flattenSuffixes
      let specifiedBeginBlock = if maybeBeginBlock >= 0 then maybeBeginBlock else defaultBeginBlock
      if continue
        then initContinuingOutputStorages outputStorages specifiedBeginBlock
        else return (outputStorages, specifiedBeginBlock)

    -- -------------------------------------------------------------------------
    -- TODO :: code to start the refactor of heigh exporter
    let endBlock = if maybeEndBlock == 0 then defaultEndBlock else maybeEndBlock
    -- let subChainWriter = writeToOutputStorages outputStorages flattenPack
    -- let writer = subChainWriter beginBlock
    -- withSupervisor KillAll
    --   $ blockHeighExporter blockchains beginBlock endBlock maybeBlocksFile threadsCount writer

    -- -------------------------------------------------------------------------
    -- TODO :: Old heigh exporter
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

  -- ---------------------------------------------------------------------------
  OptionExportIotaCommand
    { options_apiUrl = apiUrl
    , options_syncDbFile = syncDbFile
    , options_readDump = readDump
    , options_output = output@Output
      { output_postgres = maybeOutputPostgres
      , output_postgresTable = maybePostgresTable
      }
    , options_threadsCount = threadsCount
    } -> do
    httpManager <- H.newTlsManagerWith H.tlsManagerSettings
      { H.managerConnCount = threadsCount * 2
      }
    httpRequest <- H.parseRequest apiUrl
    let iota = newIota httpManager httpRequest

    outputStorages <- initOutputStorages httpManager output "iota" "hash" "hash" []

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
