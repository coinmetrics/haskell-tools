{-# LANGUAGE OverloadedStrings, ViewPatterns #-}

module Main(main) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Data.Default
import Data.Maybe
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
    <*> O.strOption
      (  O.long "height-metric"
      <> O.metavar "HEIGHT_METRIC"
      <> O.value "blockchain_node_sync_height" <> O.showDefault
      <> O.help "Name of height metric"
      )
    <*> O.strOption
      (  O.long "time-metric"
      <> O.metavar "TIME_METRIC"
      <> O.value "blockchain_node_sync_time" <> O.showDefault
      <> O.help "Name of time metric"
      )
    <*> O.strOption
      (  O.long "up-metric"
      <> O.metavar "UP_METRIC"
      <> O.value "blockchain_node_up" <> O.showDefault
      <> O.help "Name of up metric"
      )
    <*> O.many (O.strOption
      (  O.long "global-label"
      <> O.metavar "GLOBAL_LABEL"
      <> O.help "Global metric label in form of NAME=VALUE"
      ))
    <*> O.some (OptionBlockchain
      <$> O.strOption
        (  O.long "blockchain"
        <> O.metavar "BLOCKCHAIN"
        <> O.help ("Type of blockchain: " <> blockchainTypesStr)
        )
      <*> O.many (O.strOption
        (  O.long "label"
        <> O.metavar "LABEL"
        <> O.help "Metric label in form of NAME=VALUE"
        ))
      <*> O.many (OptionApi
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
        )
      )

data Options = Options
  { options_host :: !String
  , options_port :: {-# UNPACK #-} !Int
  , options_interval :: {-# UNPACK #-} !Int
  , options_heightMetric :: !T.Text
  , options_timeMetric :: !T.Text
  , options_upMetric :: !T.Text
  , options_labels :: [T.Text]
  , options_blockchains :: [OptionBlockchain]
  }

data OptionBlockchain = OptionBlockchain
  { optionBlockchain_blockchain :: !T.Text
  , optionBlockchain_labels :: [T.Text]
  , optionBlockchain_apis :: [OptionApi]
  }

data OptionApi = OptionApi
  { optionApi_apiUrl :: !String
  , optionApi_apiUrlUserName :: !String
  , optionApi_apiUrlPassword :: !String
  , optionApi_apiUrlInsecure :: !Bool
  }

run :: Options -> IO ()
run Options
  { options_host = host
  , options_port = port
  , options_interval = interval
  , options_heightMetric = heightMetricName
  , options_timeMetric = timeMetricName
  , options_upMetric = upMetricName
  , options_labels = parseLabels -> globalLabels
  , options_blockchains = blockchains
  } = do
  -- init http managers
  httpManager <- H.newTlsManagerWith H.tlsManagerSettings
    { H.managerConnCount = length blockchains * 2 + 2
    }
  httpInsecureManager <- H.newTlsManagerWith (H.mkManagerSettings def
    { NC.settingDisableCertificateValidation = True
    } Nothing)
    { H.managerConnCount = length blockchains * 2 + 2
    }

  -- run continuous updates of metrics
  forM_ blockchains $ \OptionBlockchain
    { optionBlockchain_blockchain = blockchainType
    , optionBlockchain_labels = (globalLabels ++) . parseLabels -> labels
    , optionBlockchain_apis = apisByUser
    } -> do
    -- register metrics
    let register = P.register . P.vector (ArrayLabel $ "blockchain" : "url" : "version" : map fst labels)
    heightMetric <- register $ P.gauge P.Info
      { P.metricName = heightMetricName
      , P.metricHelp = "Blockchain node's sync height"
      }
    timeMetric <- register $ P.gauge P.Info
      { P.metricName = timeMetricName
      , P.metricHelp = "Blockchain node's sync time"
      }
    upMetric <- register $ P.gauge P.Info
      { P.metricName = upMetricName
      , P.metricHelp = "Blockchain node's up state"
      }

    -- get blockchain info by name
    SomeBlockChainInfo BlockChainInfo
      { bci_init = initBlockChain
      , bci_defaultApiUrls = defaultApiUrls
      } <- maybe (fail "wrong blockchain type") return $ getSomeBlockChainInfo blockchainType

    let
      apis = if null apisByUser
        then map (\defaultApiUrl -> OptionApi
          { optionApi_apiUrl = defaultApiUrl
          , optionApi_apiUrlUserName = ""
          , optionApi_apiUrlPassword = ""
          , optionApi_apiUrlInsecure = False
          }) defaultApiUrls
        else apisByUser

    forM_ apis $ \OptionApi
      { optionApi_apiUrl = apiUrl
      , optionApi_apiUrlUserName = apiUrlUserName
      , optionApi_apiUrlPassword = apiUrlPassword
      , optionApi_apiUrlInsecure = apiUrlInsecure
      } -> void $ forkIO $ do
      -- init blockchain
      blockChain <- do
        httpRequest <- do
          httpRequest <- H.parseRequest apiUrl
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

      -- labels which can change
      volatileLabelsVar <- newTVarIO []

      forever $ do
        -- get node info
        maybeNodeInfo <- errorHandler <=< try $ evaluate =<< getBlockChainNodeInfo blockChain
        let
          version = fromMaybe T.empty $ bcni_version <$> maybeNodeInfo
        -- get height
        maybeBlockHeight <- errorHandler <=< try $ evaluate =<< getCurrentBlockHeight blockChain
        -- get timestamp
        maybeBlockTimestamp <- errorHandler <=< try $ case maybeBlockHeight of
          Just blockHeight -> evaluate . utcTimeToPOSIXSeconds . bh_timestamp =<< getBlockHeaderByHeight blockChain blockHeight
          Nothing -> fail "height is not known"

        -- construct label
        let
          labelFunc volatileLabels = ArrayLabel $ blockchainType : T.pack apiUrl : volatileLabels ++ map snd labels
          curVolatileLabels = [version]
          curLabel = labelFunc curVolatileLabels

        -- process volatile labels
        maybeOldLabel <- atomically $ do
          oldVolatileLabels <- readTVar volatileLabelsVar
          let
            changed = oldVolatileLabels /= curVolatileLabels
          when changed $ writeTVar volatileLabelsVar curVolatileLabels
          return $ if changed then Just $ labelFunc oldVolatileLabels else Nothing

        -- update metrics
        setGauge heightMetric curLabel maybeOldLabel $ fromIntegral <$> maybeBlockHeight
        setGauge timeMetric curLabel maybeOldLabel $ realToFrac <$> maybeBlockTimestamp
        setGauge upMetric curLabel maybeOldLabel $ Just $ if isJust maybeBlockHeight && isJust maybeBlockTimestamp then 1 else 0

        -- pause
        threadDelay $ interval * 1000000

  -- run web server with metrics
  Warp.runSettings (Warp.setHost (fromString host) $ Warp.setPort port Warp.defaultSettings) $ \_request respond ->
    respond . W.responseLBS H.status200
      [ (H.hContentType, "text/plain; version=0.0.4")
      ] =<< P.exportMetricsAsText

parseLabels :: [T.Text] -> [(T.Text, T.Text)]
parseLabels = map $ \(T.breakOn "=" -> (key, value)) -> (key, fromMaybe "1" $ T.stripPrefix "=" value)

setGauge :: P.Label l => P.Vector l P.Gauge -> l -> Maybe l -> Maybe Double -> IO ()
setGauge metric label maybeOldLabel maybeValue = do
  when (isJust maybeOldLabel || isNothing maybeValue) $ P.removeLabel metric $ fromMaybe label maybeOldLabel
  case maybeValue of
    Just value -> P.withLabel metric label $ flip P.setGauge value
    Nothing -> return ()

errorHandler :: Either SomeException a -> IO (Maybe a)
errorHandler = either printError (return . Just) where
  printError e = do
    hPrint stderr e
    return Nothing

newtype ArrayLabel = ArrayLabel [T.Text] deriving (Eq, Ord)

instance P.Label ArrayLabel where
  labelPairs (ArrayLabel a) (ArrayLabel b) = zip a b
