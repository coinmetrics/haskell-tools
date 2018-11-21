{-# LANGUAGE OverloadedStrings #-}

module Main(main) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.Default
import Data.String
import qualified Data.Text as T
import Data.Time.Clock.POSIX
import qualified Network.Connection as NC
import qualified Network.HTTP.Client as H
import qualified Network.HTTP.Client.TLS as H
import qualified Network.HTTP.Types as H
import qualified Network.Wai as W
import qualified Network.Wai.Handler.Warp as Warp
import qualified Options.Applicative as O
import qualified Prometheus as P
import System.IO

import CoinMetrics.BlockChain
import CoinMetrics.BlockChain.All

main :: IO ()
main = run =<< O.execParser parser where
  parser = O.info (O.helper <*> opts)
    (  O.fullDesc
    <> O.progDesc "Monitors blockchain nodes"
    <> O.header "coinmetrics-monitor"
    )
  opts = Options
    <$> O.strOption
      (  O.long "host"
      <> O.metavar "HOST"
      <> O.value "127.0.0.1" <> O.showDefault
      <> O.help "Host to listen on"
      )
    <*> O.option O.auto
      (  O.long "port"
      <> O.metavar "PORT"
      <> O.value 8080 <> O.showDefault
      <> O.help "Port to listen on"
      )
    <*> O.option O.auto
      (  O.long "interval"
      <> O.metavar "INTERVAL"
      <> O.value 10 <> O.showDefault
      <> O.help "Blockchain polling interval"
      )
    <*> O.some (OptionBlockchain
      <$> O.strOption
        (  O.long "blockchain"
        <> O.metavar "BLOCKCHAIN"
        <> O.help ("Type of blockchain: " <> blockchainTypesStr)
        )
      <*> O.strOption
        (  O.long "name"
        <> O.metavar "NAME"
        <> O.value ""
        <> O.help "Name of metric"
        )
      <*> O.strOption
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
      <*> O.switch
        (  O.long "api-url-insecure"
        <> O.help "Do not validate HTTPS certificate"
        )
      )

data Options = Options
  { options_host :: !String
  , options_port :: {-# UNPACK #-} !Int
  , options_interval :: {-# UNPACK #-} !Int
  , options_blockchains :: [OptionBlockchain]
  } deriving Show

data OptionBlockchain = OptionBlockchain
  { optionBlockchain_blockchain :: !T.Text
  , optionBlockchain_name :: !T.Text
  , optionBlockchain_apiUrl :: !String
  , optionBlockchain_apiUrlUserName :: !String
  , optionBlockchain_apiUrlPassword :: !String
  , optionBlockchain_apiUrlInsecure :: !Bool
  } deriving Show

run :: Options -> IO ()
run Options
  { options_host = host
  , options_port = port
  , options_interval = interval
  , options_blockchains = blockchains
  } = do

  -- register metrics
  heightMetric <- P.register $ P.vector "name" $ P.gauge P.Info
    { P.metricName = "blockchain_node_sync_height"
    , P.metricHelp = "Blockchain node's sync height"
    }
  timeMetric <- P.register $ P.vector "name" $ P.gauge P.Info
    { P.metricName = "blockchain_node_sync_time"
    , P.metricHelp = "Blockchain node's sync time"
    }

  -- init http managers
  httpManager <- H.newTlsManagerWith H.tlsManagerSettings
    { H.managerConnCount = length blockchains + 2
    }
  httpInsecureManager <- H.newTlsManagerWith (H.mkManagerSettings def
    { NC.settingDisableCertificateValidation = True
    } Nothing)
    { H.managerConnCount = length blockchains + 2
    }

  -- run continuous updates of metrics
  forM_ blockchains $ \OptionBlockchain
    { optionBlockchain_blockchain = blockchainType
    , optionBlockchain_name = redefinedName
    , optionBlockchain_apiUrl = apiUrl
    , optionBlockchain_apiUrlUserName = apiUrlUserName
    , optionBlockchain_apiUrlPassword = apiUrlPassword
    , optionBlockchain_apiUrlInsecure = apiUrlInsecure
    } -> void $ forkIO $ do

    let name = if T.null redefinedName then blockchainType else redefinedName

    -- get blockchain info by name
    SomeBlockChainInfo BlockChainInfo
      { bci_init = initBlockChain
      , bci_defaultApiUrl = defaultApiUrl
      } <- maybe (fail "wrong blockchain type") return $ getSomeBlockChainInfo blockchainType

    -- init blockchain
    blockChain <- do
      httpRequest <- do
        let url = if null apiUrl then defaultApiUrl else apiUrl
        httpRequest <- H.parseRequest url
        return $ if not (null apiUrlUserName) || not (null apiUrlPassword)
          then H.applyBasicAuth (fromString apiUrlUserName) (fromString apiUrlPassword) httpRequest
          else httpRequest
      initBlockChain BlockChainParams
        { bcp_httpManager = if apiUrlInsecure then httpInsecureManager else httpManager
        , bcp_httpRequest = httpRequest
        , bcp_trace = False
        , bcp_excludeUnaccountedActions = False
        , bcp_threadsCount = 1
        }

    forever $ do
      -- get height
      maybeBlockHeight <- errorHandler <=< try $ evaluate =<< getCurrentBlockHeight blockChain
      -- get timestamp
      maybeBlockTimestamp <- errorHandler <=< try $ case maybeBlockHeight of
        Just blockHeight -> evaluate . utcTimeToPOSIXSeconds . getBlockTimestamp =<< getBlockByHeight blockChain blockHeight
        Nothing -> fail "height is not known"

      -- update metrics
      setGauge heightMetric name $ fromIntegral <$> maybeBlockHeight
      setGauge timeMetric name $ realToFrac <$> maybeBlockTimestamp

      -- pause
      threadDelay $ interval * 1000000

  -- run web server with metrics
  Warp.runSettings (Warp.setHost (fromString host) $ Warp.setPort port Warp.defaultSettings) $ \_request respond ->
    respond . W.responseLBS H.status200
      [ (H.hContentType, "text/plain; version=0.0.4")
      ] =<< P.exportMetricsAsText

setGauge :: P.Label l => P.Vector l P.Gauge -> l -> Maybe Double -> IO ()
setGauge metric label = maybe (P.removeLabel metric label) (P.withLabel metric label . flip P.setGauge)

errorHandler :: Either SomeException a -> IO (Maybe a)
errorHandler = either printError (return . Just) where
  printError e = do
    hPrint stderr e
    return Nothing
