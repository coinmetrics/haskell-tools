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
  { requestWebCache :: forall a. Bool -> H.Request -> (BL.ByteString -> IO (Bool, a)) -> IO a
  }

-- | Init web cache, using environment variables.
initWebCache :: H.Manager -> IO WebCache
initWebCache httpManager = do
  maybeVar <- lookupEnv "WEB_CACHE"
  case maybeVar of
    Just (T.pack -> var) -> case var of
      (T.stripPrefix "file:" -> Just filePath) -> do
        return WebCache
          { requestWebCache = \skipCache httpRequest f -> do
            let fileName = T.unpack $ filePath <> "/" <> T.decodeUtf8 (HU.urlEncode False (T.encodeUtf8 $ T.pack $ NU.uriToString id (H.getUri httpRequest) ""))
            -- try cache
            eitherCachedResponse <- if skipCache
              then return $ Left (undefined :: SomeException)
              else try $ BL.readFile fileName
            case eitherCachedResponse of
              Right cachedResponse -> snd <$> f cachedResponse
              Left _ -> do
                -- perform request
                response <- H.responseBody <$> H.httpLbs httpRequest httpManager
                (ok, result) <- f response
                when (ok && not skipCache) $ do
                  let tempFileName = fileName <> ".tmp"
                  BL.writeFile tempFileName response
                  renameFile tempFileName fileName
                return result
          }
      _ -> fail "wrong WEB_CACHE"
    Nothing -> return WebCache
      { requestWebCache = \_skipCache httpRequest f -> fmap snd . f . H.responseBody =<< H.httpLbs httpRequest httpManager
      }
