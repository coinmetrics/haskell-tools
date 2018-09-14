{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedLists, OverloadedStrings, ViewPatterns #-}

module CoinMetrics.Util
	( HexString(..)
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
import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.Avro.Schema as A
import qualified Data.ByteArray.Encoding as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Short as BS
import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as HM
import qualified Data.Tagged as Tag
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

-- | ByteString which serializes to JSON as hex string.
newtype HexString = HexString
	{ unHexString :: BS.ShortByteString
	} deriving (Semigroup, Monoid)
instance SchemableField HexString where
	schemaFieldTypeOf _ = SchemaFieldType_bytes
instance A.HasAvroSchema HexString where
	schema = Tag.Tagged $ Tag.unTagged (A.schema :: Tag.Tagged B.ByteString A.Type)
instance A.ToAvro HexString where
	toAvro = A.toAvro . BS.fromShort . unHexString
instance ToPostgresText HexString where
	toPostgresText inline = toPostgresText inline . BS.fromShort . unHexString
instance J.FromJSON HexString where
	parseJSON = either fail (return . HexString . BS.toShort) . BA.convertFromBase BA.Base16 . T.encodeUtf8 <=< J.parseJSON
instance J.ToJSON HexString where
	toJSON = J.toJSON . T.decodeUtf8 . BA.convertToBase BA.Base16 . BS.fromShort . unHexString
	toEncoding = J.toEncoding . T.decodeUtf8 . BA.convertToBase BA.Base16 . BS.fromShort . unHexString

decode0xHexBytes :: T.Text -> J.Parser HexString
decode0xHexBytes = \case
	(T.stripPrefix "0x" -> Just (BA.convertFromBase BA.Base16 . T.encodeUtf8 -> Right s)) -> return $ HexString $ BS.toShort s
	"" -> return mempty
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
