{-# LANGUAGE OverloadedStrings #-}

module CoinMetrics.Prometheus
  ( OptionPrometheus(..)
  , optionPrometheus
  , forkPrometheusMetricsServer
  , measureTime
  ) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Data.String
import qualified Network.HTTP.Types as H
import qualified Network.Wai as W
import qualified Network.Wai.Handler.Warp as Warp
import qualified Options.Applicative as O
import qualified Prometheus as P
import qualified System.Clock as Clock

data OptionPrometheus = OptionPrometheus
  { optionPrometheus_host :: !(Maybe String)
  , optionPrometheus_port :: !Int
  }

optionPrometheus :: O.Parser OptionPrometheus
optionPrometheus = OptionPrometheus
  <$> O.option (O.maybeReader (Just . Just))
    (  O.long "prometheus-host"
    <> O.metavar "PROMETHEUS_HOST"
    <> O.value Nothing
    <> O.help "Prometheus metrics host"
    )
  <*> O.option O.auto
    (  O.long "prometheus-port"
    <> O.metavar "PROMETHEUS_PORT"
    <> O.value 8080
    <> O.help "Prometheus metrics port"
    )

forkPrometheusMetricsServer :: OptionPrometheus -> IO ()
forkPrometheusMetricsServer OptionPrometheus
  { optionPrometheus_host = maybeHost
  , optionPrometheus_port = port
  } = case maybeHost of
  Just host -> void $ forkIO $
    Warp.runSettings (Warp.setHost (fromString host) $ Warp.setPort port Warp.defaultSettings) $ \_request respond ->
      respond . W.responseLBS H.status200
        [ (H.hContentType, "text/plain; version=0.0.4")
        ] =<< P.exportMetricsAsText
  Nothing -> return ()

measureTime :: P.Observer o => o -> IO a -> IO a
measureTime observer = bracket start stop . const where
  start = Clock.getTime Clock.Monotonic
  stop startTime = do
    endTime <- Clock.getTime Clock.Monotonic
    P.observe observer $ fromTime $ endTime `Clock.diffTimeSpec` startTime

fromTime :: Clock.TimeSpec -> Double
fromTime Clock.TimeSpec
  { Clock.sec = sec
  , Clock.nsec = nsec
  } = fromIntegral sec + fromIntegral nsec * 0.000000001
