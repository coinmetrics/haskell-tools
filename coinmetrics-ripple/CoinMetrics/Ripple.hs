{-# LANGUAGE DeriveGeneric, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Ripple
	( Ripple(..)
	, newRipple
	, RippleLedger(..)
	, RippleTransaction(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import Data.Monoid
import Data.Scientific
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Time.Clock.POSIX as Time
import qualified Data.Time.ISO8601 as Time
import qualified Data.Vector as V
import GHC.Generics(Generic)
import qualified Network.HTTP.Client as H
import Text.ParserCombinators.ReadP

import CoinMetrics.BlockChain
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

-- | Ripple connector.
data Ripple = Ripple
	{ ripple_httpManager :: !H.Manager
	, ripple_httpRequest :: !H.Request
	}

newRipple :: H.Manager -> T.Text -> Int -> Ripple
newRipple httpManager host port = Ripple
	{ ripple_httpManager = httpManager
	, ripple_httpRequest = H.defaultRequest
		{ H.method = "GET"
		, H.secure = True
		, H.host = T.encodeUtf8 host
		, H.port = port
		}
	}

rippleRequest :: J.FromJSON r => Ripple -> T.Text -> [(B.ByteString, Maybe B.ByteString)] -> IO r
rippleRequest Ripple
	{ ripple_httpManager = httpManager
	, ripple_httpRequest = httpRequest
	} path params = do
	body <- H.responseBody <$> tryWithRepeat (H.httpLbs (H.setQueryString params httpRequest
		{ H.path = T.encodeUtf8 path
		}) httpManager)
	either fail return $ J.eitherDecode body

instance BlockChain Ripple where
	type Block Ripple = RippleLedger
	type Transaction Ripple = RippleTransaction

	getCurrentBlockHeight ripple = either fail return
		. J.parseEither ((J..: "ledger_index") <=< (J..: "ledger"))
		=<< rippleRequest ripple "/v2/ledgers" []

	getBlockByHeight ripple blockHeight = either fail return
		. J.parseEither (J..: "ledger")
		=<< rippleRequest ripple ("/v2/ledgers/" <> T.pack (show blockHeight))
			[ ("transactions", Just "true")
			, ("expand", Just "true")
			]

-- https://ripple.com/build/data-api-v2/#ledger-objects

data RippleLedger = RippleLedger
	{ rl_index :: {-# UNPACK #-} !Int64
	, rl_hash :: !B.ByteString
	, rl_totalCoins :: {-# UNPACK #-} !Scientific
	, rl_closeTime :: {-# UNPACK #-} !Int64
	, rl_transactions :: !(V.Vector RippleTransaction)
	} deriving Generic

instance Schemable RippleLedger

instance J.FromJSON RippleLedger where
	parseJSON = J.withObject "ripple ledger" $ \fields -> RippleLedger
		<$> (fields J..: "ledger_index")
		<*> (decodeHexBytes =<< fields J..: "ledger_hash")
		<*> (decodeAmount =<< fields J..: "total_coins")
		<*> (fields J..: "close_time")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema RippleLedger where
	schema = genericAvroSchema
instance A.ToAvro RippleLedger where
	toAvro = genericToAvro
instance ToPostgresText RippleLedger

data RippleTransaction = RippleTransaction
	{ rt_hash :: !B.ByteString
	, rt_date :: {-# UNPACK #-} !Int64
	, rt_account :: !T.Text
	, rt_fee :: {-# UNPACK #-} !Scientific
	, rt_type :: !T.Text
	, rt_amount :: !(Maybe Scientific)
	, rt_currency :: !(Maybe T.Text)
	, rt_issuer :: !(Maybe T.Text)
	, rt_destination :: !(Maybe T.Text)
	} deriving Generic

instance Schemable RippleTransaction
instance SchemableField RippleTransaction

instance J.FromJSON RippleTransaction where
	parseJSON = J.withObject "ripple transaction" $ \fields -> do
		tx <- fields J..: "tx"
		maybeAmountValue <- tx J..:? "Amount"
		(maybeAmount, maybeCurrency, maybeIssuer) <-
			case maybeAmountValue of
				Just amountValue -> case amountValue of
					J.String amountStr -> do
						amount <- decodeAmount amountStr
						return (Just amount, Nothing, Nothing)
					J.Object amountObject -> do
						amount <- decodeAmount =<< amountObject J..: "value"
						currency <- amountObject J..: "currency"
						issuer <- amountObject J..: "issuer"
						return (Just amount, Just currency, Just issuer)
					_ -> fail "wrong amount"
				Nothing -> return (Nothing, Nothing, Nothing)
		RippleTransaction
			<$> (decodeHexBytes =<< fields J..: "hash")
			<*> (decodeDate =<< fields J..: "date")
			<*> (tx J..: "Account")
			<*> (decodeAmount =<< tx J..: "Fee")
			<*> (tx J..: "TransactionType")
			<*> (return maybeAmount)
			<*> (return maybeCurrency)
			<*> (return maybeIssuer)
			<*> (tx J..:? "Destination")

instance A.HasAvroSchema RippleTransaction where
	schema = genericAvroSchema
instance A.ToAvro RippleTransaction where
	toAvro = genericToAvro
instance ToPostgresText RippleTransaction

decodeAmount :: T.Text -> J.Parser Scientific
decodeAmount (T.unpack -> t) = case readP_to_S scientificP t of
	[(value, "")] -> return value
	_ -> fail $ "wrong amount: " <> t

decodeDate :: T.Text -> J.Parser Int64
decodeDate (T.unpack -> t) = case Time.parseISO8601 t of
	Just date -> return $ floor $ Time.utcTimeToPOSIXSeconds date
	Nothing -> fail $ "wrong date: " <> t
