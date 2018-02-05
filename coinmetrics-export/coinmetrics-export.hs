{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns #-}

module Main(main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL
import qualified Data.DiskHash as DH
import Data.Either
import Data.Maybe
import Data.Monoid
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Data.Vector as V
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Network.HTTP.Client as H
import qualified Network.HTTP.Client.TLS as H
import qualified Options.Applicative as O
import System.IO.Unsafe

import CoinMetrics.BlockChain
import CoinMetrics.Cardano
import CoinMetrics.Ethereum
import CoinMetrics.Ethereum.ERC20
import CoinMetrics.Iota
import CoinMetrics.Ripple
import Hanalytics.Schema
import Hanalytics.Schema.BigQuery
import Hanalytics.Schema.Postgres

main :: IO ()
main = run =<< O.execParser parser where
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
						<$> O.strOption
							(  O.long "api-host"
							<> O.metavar "API_HOST"
							<> O.value "127.0.0.1" <> O.showDefault
							<> O.help "Ethereum API host"
							)
						<*> O.option O.auto
							(  O.long "api-port"
							<> O.metavar "API_PORT"
							<> O.value (-1)
							<> O.help "Ethereum API port"
							)
						<*> O.strOption
							(  O.long "blockchain"
							<> O.metavar "BLOCKCHAIN"
							<> O.help "Type of blockchain: ethereum | cardano | ripple"
							)
						<*> O.option O.auto
							(  O.long "begin-block"
							<> O.metavar "BEGIN_BLOCK"
							<> O.value (-1)
							<> O.help "Begin block number (inclusive)"
							)
						<*> O.option O.auto
							(  O.long "end-block"
							<> O.value (-1000) <> O.showDefault
							<> O.metavar "END_BLOCK"
							<> O.help "End block number if positive (exclusive), offset to top block if negative"
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
							<> O.help "Ignore errors when getting blocks from daemon"
							)
					)) (O.fullDesc <> O.progDesc "Export blockchain")
				)
			<> O.command "export-iota"
				(  O.info
					(O.helper <*> (OptionExportIotaCommand
						<$> O.strOption
							(  O.long "api-host"
							<> O.metavar "API_HOST"
							<> O.value "127.0.0.1" <> O.showDefault
							<> O.help "IOTA API host"
							)
						<*> O.option O.auto
							(  O.long "api-port"
							<> O.metavar "API_PORT"
							<> O.value (-1)
							<> O.help "IOTA API port"
							)
						<*> O.strOption
							(  O.long "sync-db"
							<> O.metavar "SYNC_DB"
							<> O.help "Sync DB file"
							)
						<*> optionOutput
						<*> O.option O.auto
							(  O.long "threads"
							<> O.value 1 <> O.showDefault
							<> O.metavar "THREADS"
							<> O.help "Threads count"
							)
					)) (O.fullDesc <> O.progDesc "Export IOTA data")
				)
			<> O.command "print-schema"
				(  O.info
					(O.helper <*> (OptionPrintSchemaCommand
						<$> O.strOption
							(  O.long "schema"
							<> O.metavar "SCHEMA"
							<> O.help "Type of schema: ethereum | erc20tokens | iota | cardano | ripple"
							)
						<*> O.strOption
							(  O.long "storage"
							<> O.metavar "STORAGE"
							<> O.help "Storage type: postgres | bigquery"
							)
					)) (O.fullDesc <> O.progDesc "Prints schema")
				)
			<> O.command "export-erc20-info"
				(  O.info
					(O.helper <*> (OptionExportERC20InfoCommand
						<$> O.strOption
							(  O.long "input-json-file"
							<> O.metavar "INPUT_JSON_FILE"
							<> O.help "Input JSON file"
							)
						<*> optionOutput
					)) (O.fullDesc <> O.progDesc "Exports ERC20 info")
				)
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
			(  O.long "output-postgres"
			<> O.value Nothing
			<> O.metavar "OUTPUT_POSTGRES"
			<> O.help "Output directly to PostgreSQL DB"
			)
		<*> O.option (O.maybeReader (Just . Just))
			(  O.long "output-postgres-table"
			<> O.value Nothing
			<> O.metavar "OUTPUT_POSTGRES_TABLE"
			<> O.help "Table name for PostgreSQL output"
			)
		<*> O.option O.auto
			(  O.long "pack-size"
			<> O.value 100 <> O.showDefault
			<> O.metavar "PACK_SIZE"
			<> O.help "Number of records in pack (SQL INSERT command, or Avro block)"
			)

data Options = Options
	{ options_command :: !OptionCommand
	}

data OptionCommand
	= OptionExportCommand
		{ options_apiHost :: !T.Text
		, options_apiPort :: !Int
		, options_blockchain :: !T.Text
		, options_beginBlock :: !BlockHeight
		, options_endBlock :: !BlockHeight
		, options_outputFile :: !Output
		, options_threadsCount :: !Int
		, options_ignoreMissingBlocks :: !Bool
		}
	| OptionExportIotaCommand
		{ options_apiHost :: !T.Text
		, options_apiPort :: !Int
		, options_syncDbFile :: !String
		, options_outputFile :: !Output
		, options_threadsCount :: !Int
		}
	| OptionPrintSchemaCommand
		{ options_schema :: !T.Text
		, options_storage :: !T.Text
		}
	| OptionExportERC20InfoCommand
		{ options_inputJsonFile :: !String
		, options_outputFile :: !Output
		}

data Output = Output
	{ output_avroFile :: !(Maybe String)
	, output_postgresFile :: !(Maybe String)
	, output_postgres :: !(Maybe String)
	, output_postgresTable :: !(Maybe String)
	, output_packSize :: !Int
	}

run :: Options -> IO ()
run Options
	{ options_command = command
	} = case command of

	OptionExportCommand
		{ options_apiHost = apiHost
		, options_apiPort = maybeApiPort
		, options_blockchain = blockchainType
		, options_beginBlock = maybeBeginBlock
		, options_endBlock = endBlock
		, options_outputFile = outputFile
		, options_threadsCount = threadsCount
		, options_ignoreMissingBlocks = ignoreMissingBlocks
		} -> do
		httpManager <- H.newTlsManagerWith H.tlsManagerSettings
			{ H.managerConnCount = threadsCount * 2
			}
		(SomeBlockChain blockChain, beginBlock) <- case blockchainType of
			"ethereum" -> return (SomeBlockChain $ newEthereum httpManager apiHost (if maybeApiPort >= 0 then maybeApiPort else 8545), if maybeBeginBlock >= 0 then maybeBeginBlock else 0)
			"cardano" -> return (SomeBlockChain $ newCardano httpManager apiHost (if maybeApiPort >= 0 then maybeApiPort else 8100), if maybeBeginBlock >= 0 then maybeBeginBlock else 2)
			"ripple" -> return (SomeBlockChain $ newRipple httpManager apiHost (if maybeApiPort >= 0 then maybeApiPort else 443), if maybeBeginBlock >= 0 then maybeBeginBlock else 32570)
			_ -> fail "wrong blockchain specified"

		-- simple multithreaded pipeline
		blockIndexQueue <- newTBQueueIO (threadsCount * 2)
		blockIndexQueueEndedVar <- newTVarIO False
		nextBlockIndexVar <- newTVarIO beginBlock
		blockQueue <- newTBQueueIO (threadsCount * 2)

		-- thread adding indices to index queue
		void $ forkIO $
			if endBlock >= 0 then do
				mapM_ (atomically . writeTBQueue blockIndexQueue) [beginBlock..(endBlock - 1)]
				atomically $ writeTVar blockIndexQueueEndedVar True
			-- else do infinite stream of indices
			else let
				step i = do
					-- determine current (known) block index
					currentBlockIndex <- getCurrentBlockHeight blockChain
					-- insert indices up to this index minus offset
					let endIndex = currentBlockIndex + endBlock
					putStrLn $ "continuously syncing blocks... currently from " <> show i <> " to " <> show (endIndex - 1)
					mapM_ (atomically . writeTBQueue blockIndexQueue) [i..(endIndex - 1)]
					-- pause
					threadDelay 10000000
					-- repeat
					step endIndex
				in step beginBlock

		-- work threads getting blocks from blockchain
		forM_ [1..threadsCount] $ \_ -> let
			step = do
				maybeBlockIndex <- atomically $ do
					maybeBlockIndex <- tryReadTBQueue blockIndexQueue
					case maybeBlockIndex of
						Just _ -> return maybeBlockIndex
						Nothing -> do
							blockIndexQueueEnded <- readTVar blockIndexQueueEndedVar
							if blockIndexQueueEnded
								then return Nothing
								else retry
				case maybeBlockIndex of
					Just blockIndex -> do
						-- get block from blockchain
						eitherBlock <- try $ getBlockByHeight blockChain blockIndex
						case eitherBlock of
							Right block ->
								-- insert block into block queue ensuring order
								atomically $ do
									nextBlockIndex <- readTVar nextBlockIndexVar
									if blockIndex == nextBlockIndex then do
										writeTBQueue blockQueue (blockIndex, block)
										writeTVar nextBlockIndexVar (nextBlockIndex + 1)
									else retry
							Left (SomeException err) -> do
								print err
								-- if it's allowed to ignore errors, do that
								if ignoreMissingBlocks
									then atomically $ do
										nextBlockIndex <- readTVar nextBlockIndexVar
										if blockIndex == nextBlockIndex
											then writeTVar nextBlockIndexVar (nextBlockIndex + 1)
											else retry
									-- otherwise rethrow error
									else throwIO err
						-- repeat
						step
					Nothing -> return ()
			in forkIO step

		-- write blocks into outputs, using lazy IO
		let step i = if endBlock < 0 || i < endBlock
			then unsafeInterleaveIO $ do
				(blockIndex, block) <- atomically $ readTBQueue blockQueue
				when (blockIndex `rem` 100 == 0) $ putStrLn $ "synced up to " ++ show blockIndex
				(block :) <$> step (blockIndex + 1)
			else return []
		writeOutput outputFile blockchainType =<< step beginBlock
		putStrLn $ "sync from " ++ show beginBlock ++ " to " ++ show (endBlock - 1) ++ " complete"

	OptionExportIotaCommand
		{ options_apiHost = apiHost
		, options_apiPort = maybeApiPort
		, options_syncDbFile = syncDbFile
		, options_outputFile = outputFile
		, options_threadsCount = threadsCount
		} -> do
		httpManager <- H.newTlsManagerWith H.tlsManagerSettings
			{ H.managerConnCount = threadsCount * 2
			}
		let iota = newIota httpManager apiHost (if maybeApiPort >= 0 then maybeApiPort else 14265)

		-- simple multithreaded pipeline
		hashQueue <- newTQueueIO
		transactionQueue <- newTBQueueIO (threadsCount * 2)

		queueSizeVar <- newTVarIO 0 :: IO (TVar Int)

		-- thread working with sync db
		syncDbActionsQueue <- newTQueueIO
		void $ forkIO $ DH.withDiskHashRW syncDbFile 82 $ \syncDb -> forever $ do
			action <- atomically $ readTQueue syncDbActionsQueue
			action syncDb

		let addHash hash@(T.encodeUtf8 -> hashBytes) = atomically $ writeTQueue syncDbActionsQueue $ \syncDb -> do
			hashProcessed <- isJust <$> DH.htLookupRW hashBytes syncDb
			unless (hashProcessed) $ do
				ok <- DH.htInsert hashBytes () syncDb
				unless ok $ fail "can't write into sync db"
				atomically $ do
					writeTQueue hashQueue hash
					modifyTVar' queueSizeVar (+ 1)

		let takeHashes limit = let
			step n hashes = if n <= (0 :: Int) then return hashes else do
				maybeHash <- tryReadTQueue hashQueue
				case maybeHash of
					Just hash -> do
						modifyTVar' queueSizeVar (subtract 1)
						step (n - 1) (hash : hashes)
					Nothing -> return hashes
			in step limit []

		-- thread adding milestones to hash queue
		void $ forkIO $ forever $ do
			-- get milestone
			latestMilestoneHash <- iotaGetLatestMilestone iota
			putStrLn $ "latest milestone: " <> T.unpack latestMilestoneHash
			-- put milestone into queue
			addHash latestMilestoneHash
			-- output queue size
			queueSize <- readTVarIO queueSizeVar
			putStrLn $ "queue size: " <> show queueSize
			-- pause
			threadDelay 10000000

		-- work threads getting transactions from blockchain
		forM_ [1..threadsCount] $ const $ forkIO $ forever $ do
			hashes <- V.fromList <$> atomically (takeHashes 1000)
			transactions <- iotaGetTransactions iota hashes
			forM_ transactions $ \transaction@IotaTransaction
				{ it_trunkTransaction = trunkTransaction
				, it_branchTransaction = branchTransaction
				} -> do
				atomically $ writeTBQueue transactionQueue transaction
				addHash trunkTransaction
				addHash branchTransaction

		-- write blocks into outputs, using lazy IO
		let step i = unsafeInterleaveIO $ do
			transaction <- atomically $ readTBQueue transactionQueue
			when (i `rem` 100 == 0) $ putStrLn $ "synced transactions: " ++ show i
			(transaction :) <$> step (i + 1)
		writeOutput outputFile "iota" =<< step (0 :: Int)

	OptionPrintSchemaCommand
		{ options_schema = schemaTypeStr
		, options_storage = storageTypeStr
		} -> case (schemaTypeStr, storageTypeStr) of
		("ethereum", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy EthereumLog)
				, schemaOf (Proxy :: Proxy EthereumTransaction)
				, schemaOf (Proxy :: Proxy EthereumUncleBlock)
				, schemaOf (Proxy :: Proxy EthereumBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"ethereum\" OF \"EthereumBlock\" (PRIMARY KEY (\"number\"));"
		("ethereum", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy EthereumBlock)
		("erc20tokens", "postgres") ->
			putStrLn $ T.unpack $ TL.toStrict $ TL.toLazyText $ "CREATE TABLE erc20tokens (" <> concatFields (postgresSchemaFields True $ schemaOf (Proxy :: Proxy ERC20Info)) <> ");"
		("erc20tokens", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy ERC20Info)
		("iota", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy IotaTransaction)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"iota\" OF \"IotaTransaction\" (PRIMARY KEY (\"hash\"));"
		("iota", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy IotaTransaction)
		("cardano", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy CardanoInput)
				, schemaOf (Proxy :: Proxy CardanoOutput)
				, schemaOf (Proxy :: Proxy CardanoTransaction)
				, schemaOf (Proxy :: Proxy CardanoBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"cardano\" OF \"CardanoBlock\" (PRIMARY KEY (\"height\"));"
		("cardano", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy CardanoBlock)
		("ripple", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy RippleTransaction)
				, schemaOf (Proxy :: Proxy RippleLedger)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"ripple\" OF \"RippleLedger\" (PRIMARY KEY (\"index\"));"
		("ripple", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy RippleLedger)
		_ -> fail "wrong pair schema+storage"

	OptionExportERC20InfoCommand
		{ options_inputJsonFile = inputJsonFile
		, options_outputFile = outputFile
		} -> do
		tokensInfos <- either fail return . J.eitherDecode' =<< BL.readFile inputJsonFile
		writeOutput outputFile "erc20tokens" (tokensInfos :: [ERC20Info])

	where
		blockSplit :: Int -> [a] -> [[a]]
		blockSplit packSize = \case
			[] -> []
			xs -> let (a, b) = splitAt packSize xs in a : blockSplit packSize b

		writeOutput :: (A.ToAvro a, ToPostgresText a) => Output -> T.Text -> [a] -> IO ()
		writeOutput Output
			{ output_avroFile = maybeOutputAvroFile
			, output_postgresFile = maybeOutputPostgresFile
			, output_postgres = maybeOutputPostgres
			, output_postgresTable = maybePostgresTable
			, output_packSize = packSize
			} defaultTableName (blockSplit packSize -> blocks) = do
			vars <- forM outputs $ \output -> do
				var <- newTVarIO Nothing
				void $ forkFinally output $ atomically . writeTVar var . Just
				return var
			results <- atomically $ do
				results <- mapM readTVar vars
				unless (all isJust results || any (maybe False isLeft) results) retry
				return results
			let erroredResults = concat $ map (maybe [] (either pure (const []))) results
			unless (null erroredResults) $ do
				print erroredResults
				fail "output failed"

			where outputs = let tableName = fromMaybe defaultTableName $ T.pack <$> maybePostgresTable in concat
				[ case maybeOutputAvroFile of
					Just outputAvroFile -> [BL.writeFile outputAvroFile =<< A.encodeContainer blocks]
					Nothing -> []
				, case maybeOutputPostgresFile of
					Just outputPostgresFile -> [BL.writeFile outputPostgresFile $ TL.encodeUtf8 $ TL.toLazyText $ mconcat $ map (postgresSqlInsertGroup tableName) blocks]
					Nothing -> []
				, case maybeOutputPostgres of
					Just outputPostgres ->
						[ do
							connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack outputPostgres
							connectionStatus <- PQ.status connection
							unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
							forM_ blocks $ \block -> do
								resultStatus <- maybe (return PQ.FatalError) PQ.resultStatus <=< PQ.exec connection $ T.encodeUtf8 $ TL.toStrict $ TL.toLazyText $ postgresSqlInsertGroup tableName block
								unless (resultStatus == PQ.CommandOk) $ fail $ "command failed: " <> show resultStatus
							PQ.finish connection
							]
					Nothing -> []
				]

		concatFields = foldr1 $ \a b -> a <> ", " <> b
