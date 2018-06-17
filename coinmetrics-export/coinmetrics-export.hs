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
import Data.String
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
import System.IO
import System.IO.Unsafe

import CoinMetrics.Bitcoin
import CoinMetrics.BlockChain
import CoinMetrics.Cardano
import CoinMetrics.Ethereum
import CoinMetrics.Ethereum.ERC20
import CoinMetrics.Iota
import CoinMetrics.Monero
import CoinMetrics.Nem
import CoinMetrics.Neo
import CoinMetrics.Ripple
import CoinMetrics.Stellar
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
							(  O.long "api-url"
							<> O.metavar "API_URL"
							<> O.value ""
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
						<*> O.strOption
							(  O.long "blockchain"
							<> O.metavar "BLOCKCHAIN"
							<> O.help "Type of blockchain: bitcoin | ethereum | cardano | monero | nem | neo | ripple | stellar"
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
						<*> O.switch
							(  O.long "continue"
							<> O.help "Get BEGIN_BLOCK from output, to continue after latest written block. Works with --output-postgres only"
							)
						<*> O.switch
							(  O.long "trace"
							<> O.help "Perform tracing of actions performed by transactions"
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
							<> O.help "Type of schema: bitcoin | ethereum | erc20tokens | cardano | iota | monero | nem | neo | ripple | stellar"
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
		{ options_apiUrl :: !String
		, options_apiUrlUserName :: !String
		, options_apiUrlPassword :: !String
		, options_blockchain :: !T.Text
		, options_beginBlock :: !BlockHeight
		, options_endBlock :: !BlockHeight
		, options_continue :: !Bool
		, options_trace :: !Bool
		, options_outputFile :: !Output
		, options_threadsCount :: !Int
		, options_ignoreMissingBlocks :: !Bool
		}
	| OptionExportIotaCommand
		{ options_apiUrl :: !String
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
		{ options_apiUrl = apiUrl
		, options_apiUrlUserName = apiUrlUserName
		, options_apiUrlPassword = apiUrlPassword
		, options_blockchain = blockchainType
		, options_beginBlock = maybeBeginBlock
		, options_endBlock = maybeEndBlock
		, options_continue = continue
		, options_trace = trace
		, options_outputFile = outputFile@Output
			{ output_postgres = maybeOutputPostgres
			, output_postgresTable = maybePostgresTable
			}
		, options_threadsCount = threadsCount
		, options_ignoreMissingBlocks = ignoreMissingBlocks
		} -> do
		httpManager <- H.newTlsManagerWith H.tlsManagerSettings
			{ H.managerConnCount = threadsCount * 2
			}
		let parseApiUrl defaultApiUrl = do
			let url = if null apiUrl then defaultApiUrl else apiUrl
			httpRequest <- H.parseRequest url
			return $ if not (null apiUrlUserName) || not (null apiUrlPassword)
				then H.applyBasicAuth (fromString apiUrlUserName) (fromString apiUrlPassword) httpRequest
				else httpRequest
		(SomeBlockChain blockChain, defaultBeginBlock, defaultEndBlock) <- case blockchainType of
			"bitcoin" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:8332/"
				return (SomeBlockChain $ newBitcoin httpManager httpRequest, 0, -1000) -- very conservative rewrite limit
			"ethereum" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:8545/"
				return (SomeBlockChain $ newEthereum httpManager httpRequest trace, 0, -1000) -- very conservative rewrite limit
			"cardano" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:8100/"
				return (SomeBlockChain $ newCardano httpManager httpRequest, 2, -1000) -- very conservative rewrite limit
			"monero" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:18081/json_rpc"
				return (SomeBlockChain $ newMonero httpManager httpRequest, 0, -60) -- conservative rewrite limit
			"nem" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:7890/"
				return (SomeBlockChain $ newNem httpManager httpRequest, 1, -360) -- actual rewrite limit
			"neo" -> do
				httpRequest <- parseApiUrl "http://127.0.0.1:10332/"
				return (SomeBlockChain $ newNeo httpManager httpRequest, 0, -1000) -- very conservative rewrite limit
			"ripple" -> do
				httpRequest <- parseApiUrl "https://data.ripple.com/"
				return (SomeBlockChain $ newRipple httpManager httpRequest, 32570, 0) -- history data, no rewrites
			"stellar" -> do
				httpRequest <- parseApiUrl "http://history.stellar.org/prd/core-live/core_live_001"
				stellar <- newStellar httpManager httpRequest (2 + threadsCount `quot` 64)
				return (SomeBlockChain stellar, 1, 0) -- history data, no rewrites
			_ -> fail "wrong blockchain specified"

		-- get begin block, from output postgres if needed
		beginBlock <- if maybeBeginBlock >= 0 then return maybeBeginBlock else
			if continue
				then case maybeOutputPostgres of
					Just outputPostgres -> do
						connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack outputPostgres
						connectionStatus <- PQ.status connection
						unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
						let query = "SELECT MAX(\"" <> blockHeightFieldName blockChain <> "\") FROM \"" <> maybe blockchainType T.pack maybePostgresTable <> "\""
						result <- maybe (fail "cannot get latest block from postgres") return =<< PQ.execParams connection (T.encodeUtf8 $ query) [] PQ.Text
						resultStatus <- PQ.resultStatus result
						unless (resultStatus == PQ.TuplesOk) $ fail $ "cannot get latest block from postgres: " <> show resultStatus
						tuplesCount <- PQ.ntuples result
						unless (tuplesCount == 1) $ fail "cannot decode tuples from postgres"
						maybeValue <- PQ.getvalue result 0 0
						beginBlock <- case maybeValue of
							Just beginBlockStr -> do
								let maxBlock = read (T.unpack $ T.decodeUtf8 beginBlockStr)
								hPutStrLn stderr $ "got latest block synchronized to postgres: " <> show maxBlock
								return $ maxBlock + 1
							Nothing -> return defaultBeginBlock
						PQ.finish connection
						return beginBlock
					Nothing -> fail "--continue requires --output-postgres"
				else return defaultBeginBlock

		let endBlock = if maybeEndBlock == 0 then defaultEndBlock else maybeEndBlock

		-- simple multithreaded pipeline
		blockIndexQueue <- newTBQueueIO (threadsCount * 2)
		blockIndexQueueEndedVar <- newTVarIO False
		nextBlockIndexVar <- newTVarIO beginBlock
		blockQueue <- newTBQueueIO (threadsCount * 2)

		-- thread adding indices to index queue
		void $ forkIO $
			if endBlock > 0 then do
				mapM_ (atomically . writeTBQueue blockIndexQueue) [beginBlock..(endBlock - 1)]
				atomically $ writeTVar blockIndexQueueEndedVar True
			-- else do infinite stream of indices
			else let
				step i = do
					-- determine current (known) block index
					currentBlockIndex <- getCurrentBlockHeight blockChain
					-- insert indices up to this index minus offset
					let endIndex = currentBlockIndex + endBlock
					hPutStrLn stderr $ "continuously syncing blocks... currently from " <> show i <> " to " <> show (endIndex - 1)
					mapM_ (atomically . writeTBQueue blockIndexQueue) [i..(endIndex - 1)]
					-- pause
					threadDelay 10000000
					-- repeat
					step $ max i endIndex
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
		let step i = if endBlock <= 0 || i < endBlock
			then unsafeInterleaveIO $ do
				(blockIndex, block) <- atomically $ readTBQueue blockQueue
				when (blockIndex `rem` 100 == 0) $ hPutStrLn stderr $ "synced up to " ++ show blockIndex
				(block :) <$> step (blockIndex + 1)
			else return []
		writeOutput outputFile blockchainType =<< step beginBlock
		hPutStrLn stderr $ "sync from " ++ show beginBlock ++ " to " ++ show (endBlock - 1) ++ " complete"

	OptionExportIotaCommand
		{ options_apiUrl = apiUrl
		, options_syncDbFile = syncDbFile
		, options_outputFile = outputFile
		, options_threadsCount = threadsCount
		} -> do
		httpManager <- H.newTlsManagerWith H.tlsManagerSettings
			{ H.managerConnCount = threadsCount * 2
			}
		httpRequest <- H.parseRequest apiUrl
		let iota = newIota httpManager httpRequest

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
			hPutStrLn stderr $ "latest milestone: " <> T.unpack latestMilestoneHash
			-- put milestone into queue
			addHash latestMilestoneHash
			-- output queue size
			queueSize <- readTVarIO queueSizeVar
			hPutStrLn stderr $ "queue size: " <> show queueSize
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
			when (i `rem` 100 == 0) $ hPutStrLn stderr $ "synced transactions: " ++ show i
			(transaction :) <$> step (i + 1)
		writeOutput outputFile "iota" =<< step (0 :: Int)

	OptionPrintSchemaCommand
		{ options_schema = schemaTypeStr
		, options_storage = storageTypeStr
		} -> case (schemaTypeStr, storageTypeStr) of
		("bitcoin", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy BitcoinVin)
				, schemaOf (Proxy :: Proxy BitcoinVout)
				, schemaOf (Proxy :: Proxy BitcoinTransaction)
				, schemaOf (Proxy :: Proxy BitcoinBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"bitcoin\" OF \"BitcoinBlock\" (PRIMARY KEY (\"height\"));"
		("bitcoin", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy BitcoinBlock)
		("ethereum", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy EthereumAction)
				, schemaOf (Proxy :: Proxy EthereumLog)
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
		("iota", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy IotaTransaction)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"iota\" OF \"IotaTransaction\" (PRIMARY KEY (\"hash\"));"
		("iota", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy IotaTransaction)
		("monero", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy MoneroTransactionInput)
				, schemaOf (Proxy :: Proxy MoneroTransactionOutput)
				, schemaOf (Proxy :: Proxy MoneroTransaction)
				, schemaOf (Proxy :: Proxy MoneroBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"monero\" OF \"MoneroBlock\" (PRIMARY KEY (\"height\"));"
		("monero", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy MoneroBlock)
		("nem", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy NemNestedTransaction)
				, schemaOf (Proxy :: Proxy NemTransaction)
				, schemaOf (Proxy :: Proxy NemBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"nem\" OF \"NemBlock\" (PRIMARY KEY (\"height\"));"
		("nem", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy NemBlock)
		("neo", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy NeoTransactionInput)
				, schemaOf (Proxy :: Proxy NeoTransactionOutput)
				, schemaOf (Proxy :: Proxy NeoTransaction)
				, schemaOf (Proxy :: Proxy NeoBlock)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"neo\" OF \"NeoBlock\" (PRIMARY KEY (\"index\"));"
		("neo", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy NeoBlock)
		("ripple", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy RippleTransaction)
				, schemaOf (Proxy :: Proxy RippleLedger)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"ripple\" OF \"RippleLedger\" (PRIMARY KEY (\"index\"));"
		("ripple", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy RippleLedger)
		("stellar", "postgres") -> do
			putStr $ T.unpack $ TL.toStrict $ TL.toLazyText $ mconcat $ map postgresSqlCreateType
				[ schemaOf (Proxy :: Proxy StellarAsset)
				, schemaOf (Proxy :: Proxy StellarOperation)
				, schemaOf (Proxy :: Proxy StellarTransaction)
				, schemaOf (Proxy :: Proxy StellarLedger)
				]
			putStrLn $ T.unpack $ "CREATE TABLE \"stellar\" OF \"StellarLedger\" (PRIMARY KEY (\"sequence\"));"
		("stellar", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy StellarLedger)
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
					Just outputPostgres -> [mapM_ (writeBlockToPostgres outputPostgres tableName) blocks]
					Nothing -> []
				]

		writeBlockToPostgres outputPostgres tableName block = do
			connection <- PQ.connectdb $ T.encodeUtf8 $ T.pack outputPostgres
			connectionStatus <- PQ.status connection
			unless (connectionStatus == PQ.ConnectionOk) $ fail $ "postgres connection failed: " <> show connectionStatus
			resultStatus <- maybe (return PQ.FatalError) PQ.resultStatus <=< PQ.exec connection $ T.encodeUtf8 $ TL.toStrict $ TL.toLazyText $ postgresSqlInsertGroup tableName block
			unless (resultStatus == PQ.CommandOk) $ fail $ "command failed: " <> show resultStatus
			PQ.finish connection

		concatFields = foldr1 $ \a b -> a <> ", " <> b
