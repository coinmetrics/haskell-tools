{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns #-}

module Main(main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL
import Data.Either
import Data.Maybe
import Data.Monoid
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Text.Lazy.Encoding as TL
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Network.HTTP.Client as H
import qualified Options.Applicative as O
import System.IO.Unsafe

import CoinMetrics.BlockChain
import CoinMetrics.Ethereum
import CoinMetrics.Ethereum.ERC20
import CoinMetrics.Schema
import CoinMetrics.Schema.BigQuery
import CoinMetrics.Schema.Postgres

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
					(OptionExportCommand
						<$> O.strOption
							(  O.long "api-host"
							<> O.metavar "API_HOST"
							<> O.value "127.0.0.1" <> O.showDefault
							<> O.help "Ethereum API host"
							)
						<*> O.option O.auto
							(  O.long "api-port"
							<> O.metavar "API_PORT"
							<> O.value 8545 <> O.showDefault
							<> O.help "Ethereum API port"
							)
						<*> O.option O.auto
							(  O.long "begin-block"
							<> O.value 0 <> O.showDefault
							<> O.metavar "BEGIN_BLOCK"
							<> O.help "Begin block number"
							)
						<*> O.option O.auto
							(  O.long "end-block"
							<> O.value (-1000) <> O.showDefault
							<> O.metavar "END_BLOCK"
							<> O.help "End block number"
							)
						<*> optionOutput
						<*> O.option O.auto
							(  O.long "threads"
							<> O.value 1 <> O.showDefault
							<> O.metavar "THREADS"
							<> O.help "Threads count"
							)
					) (O.fullDesc <> O.progDesc "Export blockchain into file")
				)
			<> O.command "print-schema"
				(  O.info
					(OptionPrintSchemaCommand
						<$> O.strOption
							(  O.long "schema"
							<> O.metavar "SCHEMA"
							<> O.help "Type of schema: ethereum, erc20tokens"
							)
						<*> O.strOption
							(  O.long "storage"
							<> O.metavar "STORAGE"
							<> O.help "Storage type: postgres, bigquery"
							)
					) (O.fullDesc <> O.progDesc "Prints schema")
				)
			<> O.command "export-erc20-info"
				(  O.info
					(OptionExportERC20InfoCommand
						<$> O.strOption
							(  O.long "input-json-file"
							<> O.metavar "INPUT_JSON_FILE"
							<> O.help "Input JSON file"
							)
						<*> optionOutput
					) (O.fullDesc <> O.progDesc "Exports ERC20 info")
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
			<> O.help "Output directly to Postgres DB"
			)
		<*> O.option O.auto
			(  O.long "block-size"
			<> O.value 100 <> O.showDefault
			<> O.metavar "BLOCK_SIZE"
			<> O.help "Number of records in exported block"
			)

data Options = Options
	{ options_command :: !OptionCommand
	}

data OptionCommand
	= OptionExportCommand
		{ options_apiHost :: !T.Text
		, options_apiPort :: !Int
		, options_beginBlock :: !BlockHeight
		, options_endBlock :: !BlockHeight
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
	, output_blockSize :: !Int
	}

run :: Options -> IO ()
run Options
	{ options_command = command
	} = case command of

	OptionExportCommand
		{ options_apiHost = apiHost
		, options_apiPort = apiPort
		, options_beginBlock = beginBlock
		, options_endBlock = endBlock
		, options_outputFile = outputFile
		, options_threadsCount = threadsCount
		} -> do
		httpManager <- H.newManager H.defaultManagerSettings
		let blockChain = newEthereum httpManager apiHost apiPort

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
					blockIndexQueueEnded <- readTVar blockIndexQueueEndedVar
					if blockIndexQueueEnded
						then return Nothing
						else Just <$> readTBQueue blockIndexQueue
				case maybeBlockIndex of
					Just blockIndex -> do
						-- get block from blockchain
						block <- getBlockByHeight blockChain blockIndex
						-- insert block into block queue ensuring order
						atomically $ do
							nextBlockIndex <- readTVar nextBlockIndexVar
							if blockIndex == nextBlockIndex then do
								writeTBQueue blockQueue block
								writeTVar nextBlockIndexVar (nextBlockIndex + 1)
							else retry
						-- repeat
						step
					Nothing -> return ()
			in forkIO step

		-- write blocks into outputs, using lazy IO
		let step i = if endBlock < 0 || i < endBlock
			then unsafeInterleaveIO $ do
				block <- atomically $ readTBQueue blockQueue
				when (i `rem` 100 == 0) $ putStrLn $ "synced up to " ++ show i
				(block :) <$> step (i + 1)
			else return []
		writeOutput outputFile "ethereum" =<< step beginBlock
		putStrLn $ "sync from " ++ show beginBlock ++ " to " ++ show (endBlock - 1) ++ " complete"

	OptionPrintSchemaCommand
		{ options_schema = schemaTypeStr
		, options_storage = storageTypeStr
		} -> case (schemaTypeStr, storageTypeStr) of
		("ethereum", "postgres") -> do
			putStrLn $ T.unpack $ "CREATE TYPE EthereumLog AS (" <> concatFields (postgresSchemaFields False $ schemaOf (Proxy :: Proxy EthereumLog)) <> ");"
			putStrLn $ T.unpack $ "CREATE TYPE EthereumTransaction AS (" <> concatFields (postgresSchemaFields False $ schemaOf (Proxy :: Proxy EthereumTransaction)) <> ");"
			putStrLn $ T.unpack $ "CREATE TABLE ethereum (" <> concatFields (postgresSchemaFields True $ schemaOf (Proxy :: Proxy EthereumBlock)) <> ", PRIMARY KEY (\"number\"));"
		("ethereum", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy EthereumBlock)
		("erc20tokens", "postgres") ->
			putStrLn $ T.unpack $ "CREATE TABLE erc20tokens (" <> concatFields (postgresSchemaFields True $ schemaOf (Proxy :: Proxy ERC20Info)) <> ");"
		("erc20tokens", "bigquery") ->
			putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy ERC20Info)
		_ -> fail "wrong pair schema+storage"

	OptionExportERC20InfoCommand
		{ options_inputJsonFile = inputJsonFile
		, options_outputFile = outputFile
		} -> do
		tokensInfos <- either fail return . J.eitherDecode' =<< BL.readFile inputJsonFile
		writeOutput outputFile "erc20tokens" (tokensInfos :: [ERC20Info])

	where
		blockSplit :: Int -> [a] -> [[a]]
		blockSplit blockSize = \case
			[] -> []
			xs -> let (a, b) = splitAt blockSize xs in a : blockSplit blockSize b

		writeOutput :: (A.ToAvro a, ToPostgresText a) => Output -> T.Text -> [a] -> IO ()
		writeOutput Output
			{ output_avroFile = maybeOutputAvroFile
			, output_postgresFile = maybeOutputPostgresFile
			, output_postgres = maybeOutputPostgres
			, output_blockSize = blockSize
			} tableName (blockSplit blockSize -> blocks) = do
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

			where outputs = concat
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
