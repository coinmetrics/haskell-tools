{-# LANGUAGE LambdaCase, OverloadedLists, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Util
	( encodeHexBytes
	, decodeHexBytes
	, encode0xHexBytes
	, decode0xHexBytes
	, encode0xHexNumber
	, decode0xHexNumber
	, decodeReadStr
	, tryWithRepeat
	, currentLocalTimeToUTC
	, standardBlockChainSchemas
	) where

import Control.Concurrent
import Control.Exception
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import qualified Data.Text.Lazy.Builder as TL
import Data.Time.Clock
import Data.Time.LocalTime
import Numeric
import System.IO.Unsafe

import Hanalytics.Schema
import Hanalytics.Schema.BigQuery
import Hanalytics.Schema.Postgres

encodeHexBytes :: B.ByteString -> J.Value
encodeHexBytes = J.String . T.decodeUtf8 . BA.convertToBase BA.Base16

decodeHexBytes :: T.Text -> J.Parser B.ByteString
decodeHexBytes = \case
	(BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s) -> return s
	s -> fail $ "decodeHexBytes error for: " ++ show s

encode0xHexBytes :: B.ByteString -> J.Value
encode0xHexBytes = J.String . T.decodeUtf8 . ("0x" <>) . BA.convertToBase BA.Base16

decode0xHexBytes :: T.Text -> J.Parser B.ByteString
decode0xHexBytes = \case
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) -> return s
	"" -> return B.empty
	s -> fail $ "decode0xHexBytes error for: " ++ show s

encode0xHexNumber :: (Integral a, Show a) => a -> J.Value
encode0xHexNumber = J.String . T.pack . ("0x" <>) . flip showHex ""

decode0xHexNumber :: Integral a => T.Text -> J.Parser a
decode0xHexNumber = \case
	(T.stripPrefix "0x" -> Just (readHex . T.unpack -> [(n, "")])) -> return n
	s -> fail $ "decode0xHexNumber error for: " ++ show s

decodeReadStr :: Read a => T.Text -> J.Parser a
decodeReadStr s = case reads (T.unpack s) of
	[(r, "")] -> return r
	_ -> fail $ "decodeReadStr error for: " ++ T.unpack s

tryWithRepeat :: IO a -> IO a
tryWithRepeat io = let
	step i = if i < 5
		then do
			eitherResult <- try io
			case eitherResult of
				Right result -> return result
				Left (SomeException err) -> do
					putStrLn $ "error: " ++ show err ++ ", retrying again in 10 seconds"
					threadDelay 10000000
					step (i + 1)
		else fail "repeating failed"
	in step (0 :: Int)

currentLocalTimeToUTC :: LocalTime -> UTCTime
currentLocalTimeToUTC = localTimeToUTC currentTimeZone

{-# NOINLINE currentTimeZone #-}
currentTimeZone :: TimeZone
currentTimeZone = unsafePerformIO getCurrentTimeZone

-- | Helper function for initializing blockchain schemas info.
standardBlockChainSchemas :: Schema -> [Schema] -> TL.Builder -> HM.HashMap T.Text T.Text
standardBlockChainSchemas blockSchema additionalSchemas createTableStmt =
	[ ( "postgres"
		, TL.toStrict $ TL.toLazyText $ mconcat (map postgresSqlCreateType (additionalSchemas ++ [blockSchema])) <> createTableStmt
		)
	, ( "bigquery"
		, T.decodeUtf8 $ BL.toStrict $ J.encode $ bigQuerySchema $ blockSchema
		)
	]
