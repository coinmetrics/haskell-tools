{-# LANGUAGE OverloadedStrings, ViewPatterns, RankNTypes #-}

module CoinMetrics.WebCache
  ( WebCache(..)
  , initWebCache
  ) where

import Control.Exception
import Control.Monad
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Network.URI as NU
import qualified Network.HTTP.Client as H
import qualified Network.HTTP.Types.URI as HU
import System.Environment
import System.Directory

newtype WebCache = WebCache
  { requestWebCache :: forall a. Bool -> H.Request -> (Either String BL.ByteString -> IO (Bool, a)) -> IO a
  }

-- | Init web cache, using environment variables.
initWebCache :: H.Manager -> IO WebCache
initWebCache httpManager = do
  maybeVar <- lookupEnv "WEB_CACHE"
  offline <- maybe False (const True) <$> lookupEnv "WEB_CACHE_OFFLINE"
  case maybeVar of
    Just (T.pack -> var) -> case var of
      (T.stripPrefix "file:" -> Just filePath) -> do
        return WebCache
          { requestWebCache = \skipCache httpRequest f -> let
            fileName = T.unpack $ filePath <> "/" <> T.decodeUtf8 (HU.urlEncode False (T.encodeUtf8 $ T.pack $ NU.uriToString id (H.getUri httpRequest) ""))
            doRequest = do
              H.responseBody <$> H.httpLbs httpRequest httpManager
            in if skipCache
              then snd <$> (f . Right =<< doRequest)
              else do
                eitherCachedResponse <- try $ BL.readFile fileName
                case eitherCachedResponse of
                  Right cachedResponse -> do
                    snd <$> f (Right cachedResponse)
                  Left SomeException {} ->
                    if offline
                      then snd <$> f (Left "no cached response in offline mode")
                      else do
                        response <- doRequest
                        (ok, result) <- f (Right response)
                        when ok $ do
                          let tempFileName = fileName <> ".tmp"
                          BL.writeFile tempFileName response
                          renameFile tempFileName fileName
                        return result
          }
      _ -> fail "wrong WEB_CACHE"
    Nothing -> return WebCache
      { requestWebCache = \_skipCache httpRequest f -> fmap snd . f . Right . H.responseBody =<< H.httpLbs httpRequest httpManager
      }
