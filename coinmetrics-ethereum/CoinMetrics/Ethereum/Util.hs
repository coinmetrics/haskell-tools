{-# LANGUAGE OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Ethereum.Util
	( encodeHexBytes
	, decodeHexBytes
	, encodeHexNumber
	, decodeHexNumber
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import Numeric

encodeHexBytes :: B.ByteString -> J.Value
encodeHexBytes = J.String . T.decodeUtf8 . ("0x" <>) . BA.convertToBase BA.Base16

decodeHexBytes :: J.Value -> J.Parser B.ByteString
decodeHexBytes = J.withText "hex bytes" $ \str -> do
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) <- return str
	return s

encodeHexNumber :: (Integral a, Show a) => a -> J.Value
encodeHexNumber = J.String . T.pack . ("0x" <>) . flip showHex ""

decodeHexNumber :: Integral a => J.Value -> J.Parser a
decodeHexNumber = J.withText "hex number" $ \str -> do
	(T.stripPrefix "0x" -> Just (readHex . T.unpack -> [(n, "")])) <- return str
	return n
