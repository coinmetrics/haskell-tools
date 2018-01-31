{-# LANGUAGE LambdaCase, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Util
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

decodeHexBytes :: T.Text -> J.Parser B.ByteString
decodeHexBytes = \case
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) -> return s
	_ -> fail "decodeHexBytes error"

encodeHexNumber :: (Integral a, Show a) => a -> J.Value
encodeHexNumber = J.String . T.pack . ("0x" <>) . flip showHex ""

decodeHexNumber :: Integral a => T.Text -> J.Parser a
decodeHexNumber = \case
	(T.stripPrefix "0x" -> Just (readHex . T.unpack -> [(n, "")])) -> return n
	_ -> fail "decodeHexNumber error"
