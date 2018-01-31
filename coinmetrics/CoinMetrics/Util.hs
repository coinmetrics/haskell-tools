{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Util
	( encodeHexBytes
	, decodeHexBytes
	, encode0xHexBytes
	, decode0xHexBytes
	, encode0xHexNumber
	, decode0xHexNumber
	, tryWithRepeat
	) where

import Control.Concurrent
import Control.Exception
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Numeric

encodeHexBytes :: B.ByteString -> J.Value
encodeHexBytes = J.String . T.decodeUtf8 . BA.convertToBase BA.Base16

decodeHexBytes :: T.Text -> J.Parser B.ByteString
decodeHexBytes = \case
	(BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s) -> return s
	_ -> fail "decodeHexBytes error"

encode0xHexBytes :: B.ByteString -> J.Value
encode0xHexBytes = J.String . T.decodeUtf8 . ("0x" <>) . BA.convertToBase BA.Base16

decode0xHexBytes :: T.Text -> J.Parser B.ByteString
decode0xHexBytes = \case
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) -> return s
	_ -> fail "decode0xHexBytes error"

encode0xHexNumber :: (Integral a, Show a) => a -> J.Value
encode0xHexNumber = J.String . T.pack . ("0x" <>) . flip showHex ""

decode0xHexNumber :: Integral a => T.Text -> J.Parser a
decode0xHexNumber = \case
	(T.stripPrefix "0x" -> Just (readHex . T.unpack -> [(n, "")])) -> return n
	_ -> fail "decode0xHexNumber error"

tryWithRepeat :: IO a -> IO a
tryWithRepeat io = let
	step = do
		eitherResult <- try io
		case eitherResult of
			Right result -> return result
			Left (SomeException err) -> do
				putStrLn $ "error: " ++ show err ++ ", retrying again in 10 seconds"
				threadDelay 10000000
				step
	in step
