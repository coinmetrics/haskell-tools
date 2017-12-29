{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString.Lazy as BL
import Data.Monoid
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Network.HTTP.Client as H
import qualified Options.Applicative as O
import System.IO.Unsafe

import CoinMetrics.BlockChain
import CoinMetrics.Ethereum
import CoinMetrics.Export.BigQuery
import CoinMetrics.Schema

main :: IO ()
main = run =<< O.execParser parser where
	parser = O.info (O.helper <*> opts)
		(  O.fullDesc
		<> O.progDesc "Exports blocks from ethereum blockchain into files"
		<> O.header "coinmetrics-ethereum-export"
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
						<*> O.strOption
							(  O.long "output-avro-file"
							<> O.metavar "OUTPUT_AVRO_FILE"
							<> O.help "Output AVRO file"
							)
					) (O.fullDesc <> O.progDesc "Export blockchain into file")
				)
			<> O.command "print-bigquery-schema"
				(  O.info
					(pure OptionPrintBigQuerySchemaCommand)
					(O.fullDesc <> O.progDesc "Prints BigQuery schema")
				)
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
		, options_outputAvroFile :: !String
		}
	| OptionPrintBigQuerySchemaCommand

run :: Options -> IO ()
run Options
	{ options_command = command
	} = case command of

	OptionExportCommand
		{ options_apiHost = apiHost
		, options_apiPort = apiPort
		, options_beginBlock = beginBlock
		, options_endBlock = endBlock
		, options_outputAvroFile = outputAvroFile
		} -> do
		httpManager <- H.newManager H.defaultManagerSettings
		let blockChain = newEthereum httpManager apiHost apiPort
		let step i = unsafeInterleaveIO $ do
			block <- getBlockByHeight blockChain i
			when (i `rem` 100 == 0) $ putStrLn $ "synced up to " ++ show i
			return block
		BL.writeFile outputAvroFile =<< A.encodeContainer . (\a -> [a]) =<< mapM step [beginBlock..(endBlock - 1)]
		putStrLn $ "sync from " ++ show beginBlock ++ " to " ++ show (endBlock - 1) ++ " complete"

	OptionPrintBigQuerySchemaCommand ->
		putStrLn $ T.unpack $ T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ schemaOf (Proxy :: Proxy EthereumBlock)
