{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns #-}

module Main(main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL
import Data.Monoid
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy.Builder as TL
import qualified Data.Text.Lazy.Encoding as TL
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
							<> O.metavar "END_BLOCK"
							<> O.help "End block number"
							)
						<*> optionOutputFile
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
						<*> optionOutputFile
					) (O.fullDesc <> O.progDesc "Exports ERC20 info")
				)
			)
	optionOutputFile = OutputFile
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
		, options_outputFile :: !OutputFile
		, options_threadsCount :: !Int
		}
	| OptionPrintSchemaCommand
		{ options_schema :: !T.Text
		, options_storage :: !T.Text
		}
	| OptionExportERC20InfoCommand
		{ options_inputJsonFile :: !String
		, options_outputFile :: !OutputFile
		}

data OutputFile = OutputFile
	{ outputFile_avroFile :: !(Maybe String)
	, outputFile_postgresFile :: !(Maybe String)
	, outputFile_blockSize :: !Int
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
		let blockIndices = [beginBlock..(endBlock - 1)]
		blockIndexQueueVar <- newTVarIO blockIndices
		nextBlockIndexVar <- newTVarIO beginBlock
		blockQueue <- newTBQueueIO (threadsCount * 2)
		forM_ [1..threadsCount] $ \_ -> let
			step = join $ atomically $ do
				blockIndexQueue <- readTVar blockIndexQueueVar
				case blockIndexQueue of
					(blockIndex : rest) -> do
						writeTVar blockIndexQueueVar rest
						return $ do
							block <- getBlockByHeight blockChain blockIndex
							atomically $ do
								-- ensure order
								nextBlockIndex <- readTVar nextBlockIndexVar
								if blockIndex == nextBlockIndex then do
									writeTBQueue blockQueue block
									writeTVar nextBlockIndexVar (nextBlockIndex + 1)
								else retry
							step
					[] -> return $ return ()
			in forkIO step

		let step i = unsafeInterleaveIO $ do
			block <- atomically $ readTBQueue blockQueue
			when (i `rem` 100 == 0) $ putStrLn $ "synced up to " ++ show i
			return block
		writeOutputFile outputFile "ethereum" =<< mapM step blockIndices
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
		writeOutputFile outputFile "erc20tokens" (tokensInfos :: [ERC20Info])

	where
		blockSplit :: Int -> [a] -> [[a]]
		blockSplit blockSize = \case
			[] -> []
			xs -> let (a, b) = splitAt blockSize xs in a : blockSplit blockSize b

		writeOutputFile :: (A.ToAvro a, ToPostgresText a) => OutputFile -> T.Text -> [a] -> IO ()
		writeOutputFile OutputFile
			{ outputFile_avroFile = maybeOutputAvroFile
			, outputFile_postgresFile = maybeOutputPostgresFile
			, outputFile_blockSize = blockSize
			} tableName (blockSplit blockSize -> blocks) = do
			vars <- forM outputs $ \output -> do
				var <- newTVarIO False
				void $ forkIO $ do
					output blocks
					atomically $ writeTVar var True
				return var
			atomically $ do
				finished <- and <$> mapM readTVar vars
				unless finished retry
			where outputs = concat
				[ case maybeOutputAvroFile of
					Just outputAvroFile -> [BL.writeFile outputAvroFile <=< A.encodeContainer]
					Nothing -> []
				, case maybeOutputPostgresFile of
					Just outputPostgresFile -> [BL.writeFile outputPostgresFile . TL.encodeUtf8 . TL.toLazyText . mconcat . map (postgresSqlInsertGroup tableName)]
					Nothing -> []
				]

		concatFields = foldr1 $ \a b -> a <> ", " <> b
