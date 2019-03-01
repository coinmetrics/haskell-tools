{-# LANGUAGE OverloadedStrings, ViewPatterns, RankNTypes #-}

module CoinMetrics.WebCache
  ( WebCache(..)
  , initWebCache
  ) where

import Control.Applicative
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
import System.IO

newtype WebCache = WebCache
  { requestWebCache :: forall a. Bool -> H.Request -> (Either String BL.ByteString -> IO (Bool, a)) -> IO a
  }

-- | Init web cache, using environment variables.
initWebCache :: H.Manager -> IO WebCache
initWebCache httpManager = do
  maybeVar <- lookupEnv "WEB_CACHE"
  rewrite <- maybe False (const True) <$> lookupEnv "WEB_CACHE_REWRITE"
  offline <- maybe False (const True) <$> lookupEnv "WEB_CACHE_OFFLINE"
  debug <- maybe False (const True) <$> lookupEnv "WEB_CACHE_DEBUG"
  case maybeVar of
    Just (T.pack -> var) -> case var of
      (T.stripPrefix "file:" -> Just filePath) -> do
        return WebCache
          { requestWebCache = \skipCache httpRequest f -> let
            -- uri2 tries the same uri, but with the explicit port
            -- that's needed, because old cache may contain explicit port,
            -- due to breaking change in URLs
            (uri, uri2) = let
              u@NU.URI
                { NU.uriScheme = scheme
                , NU.uriAuthority = maybeAuthority
                } = H.getUri httpRequest
              in (u, u
                { NU.uriAuthority = (\a -> a
                  { NU.uriPort = case (scheme, NU.uriPort a) of
                    ("https:", "") -> ":443"
                    ("http:", "") -> ":80"
                    _ -> NU.uriPort a
                  }) <$> maybeAuthority
                })
            fileName = T.unpack $ filePath <> "/" <> T.decodeUtf8 (HU.urlEncode False (T.encodeUtf8 $ T.pack $ NU.uriToString id uri ""))
            fileName2 = T.unpack $ filePath <> "/" <> T.decodeUtf8 (HU.urlEncode False (T.encodeUtf8 $ T.pack $ NU.uriToString id uri2 ""))
            doRequest = do
              when debug $ hPutStrLn stderr $ "performing HTTP request: " <> NU.uriToString id uri ""
              H.responseBody <$> H.httpLbs httpRequest httpManager
            in if skipCache
              then snd <$> (f . Right =<< doRequest)
              else do
                eitherCachedResponse <- if rewrite
                  then return $ Left $ SomeException (undefined :: SomeException)
                  else try $ BL.readFile fileName <|> BL.readFile fileName2 -- try both files
                case eitherCachedResponse of
                  Right cachedResponse -> do
                    when debug $ hPutStrLn stderr $ "using cached response: " <> fileName
                    snd <$> f (Right cachedResponse)
                  Left SomeException {} ->
                    if offline
                      then do
                        when debug $ hPutStrLn stderr $ "no cached offline response: " <> fileName
                        snd <$> f (Left "no cached response in offline mode")
                      else do
                        response <- doRequest
                        (ok, result) <- f (Right response)
                        if ok
                          then do
                            when debug $ hPutStrLn stderr $ "saving to cache: " <> fileName
                            let tempFileName = fileName <> ".tmp"
                            BL.writeFile tempFileName response
                            renameFile tempFileName fileName
                          else when debug $ hPutStrLn stderr $ "bad response, not saving to cache"
                        return result
          }
      _ -> fail "wrong WEB_CACHE"
    Nothing -> return WebCache
      { requestWebCache = \_skipCache httpRequest f -> fmap snd . f . Right . H.responseBody =<< H.httpLbs httpRequest httpManager
      }
