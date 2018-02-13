{-# LANGUAGE DeriveGeneric, OverloadedLists, OverloadedStrings, TypeFamilies #-}

module CoinMetrics.Nem
	( Nem(..)
	, newNem
	, NemBlock(..)
	, NemTransaction(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import Data.Maybe
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Vector as V
import GHC.Generics(Generic)
import qualified Network.HTTP.Client as H

import CoinMetrics.BlockChain
import CoinMetrics.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

-- | Nem connector.
data Nem = Nem
	{ nem_httpManager :: !H.Manager
	, nem_httpRequest :: !H.Request
	}

newNem :: H.Manager -> H.Request -> Nem
newNem httpManager httpRequest = Nem
		{ nem_httpManager = httpManager
		, nem_httpRequest = httpRequest
			{ H.requestHeaders = [("Content-Type", "application/json")]
			}
		}

nemRequest :: J.FromJSON r => Nem -> T.Text -> Maybe J.Value -> IO r
nemRequest Nem
	{ nem_httpManager = httpManager
	, nem_httpRequest = httpRequest
	} path maybeBody = tryWithRepeat $ either fail return . J.eitherDecode' . H.responseBody =<< H.httpLbs httpRequest
	{ H.path = H.path httpRequest <> T.encodeUtf8 path
	, H.method = if isJust maybeBody then "POST" else "GET"
	, H.requestBody = maybe (H.requestBody httpRequest) (H.RequestBodyLBS . J.encode) maybeBody
	} httpManager

instance BlockChain Nem where
	type Block Nem = NemBlock
	type Transaction Nem = NemTransaction

	getCurrentBlockHeight nem = either fail return
		. (J.parseEither (J..: "height"))
		=<< nemRequest nem "/chain/height" Nothing

	getBlockByHeight nem blockHeight = either fail return . J.parseEither J.parseJSON
		=<< nemRequest nem "/block/at/public" (Just $ J.Object [("height", J.Number $ fromIntegral blockHeight)])

data NemBlock = NemBlock
	{ nb_timeStamp :: {-# UNPACK #-} !Int64
	, nb_height :: {-# UNPACK #-} !Int64
	, nb_signer :: !B.ByteString
	, nb_transactions :: !(V.Vector NemTransaction)
	} deriving Generic

instance Schemable NemBlock

instance J.FromJSON NemBlock where
	parseJSON = J.withObject "nem block" $ \fields -> NemBlock
		<$> (fields J..: "timeStamp")
		<*> (fields J..: "height")
		<*> (decodeHexBytes     =<< fields J..: "signer")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema NemBlock where
	schema = genericAvroSchema
instance A.ToAvro NemBlock where
	toAvro = genericToAvro
instance ToPostgresText NemBlock

data NemTransaction = NemTransaction
	{ nt_timeStamp :: {-# UNPACK #-} !Int64
	, nt_fee :: {-# UNPACK #-} !Int64
	, nt_type :: {-# UNPACK #-} !Int64
	, nt_deadline :: {-# UNPACK #-} !Int64
	, nt_signer :: !B.ByteString
	, nt_mode :: !(Maybe Int64)
	, nt_remoteAccount :: !(Maybe B.ByteString)
	, nt_creationFee :: !(Maybe Int64)
	, nt_creationFeeSink :: !(Maybe T.Text)
	, nt_delta :: !(Maybe Int64)
	, nt_otherAccount :: !(Maybe T.Text)
	, nt_rentalFee :: !(Maybe Int64)
	, nt_rentalFeeSink :: !(Maybe T.Text)
	, nt_newPart :: !(Maybe T.Text)
	, nt_parent :: !(Maybe T.Text)
	, nt_amount :: !(Maybe Int64)
	} deriving Generic

instance Schemable NemTransaction
instance SchemableField NemTransaction

instance J.FromJSON NemTransaction where
	parseJSON = J.withObject "nem transaction" $ \fields -> NemTransaction
		<$> (fields J..: "timeStamp")
		<*> (fields J..: "fee")
		<*> (fields J..: "type")
		<*> (fields J..: "deadline")
		<*> (decodeHexBytes =<< fields J..: "signer")
		<*> (fields J..:? "mode")
		<*> (traverse decodeHexBytes =<< fields J..:? "remoteAccount")
		<*> (fields J..:? "creationFee")
		<*> (fields J..:? "creationFeeSink")
		<*> (fields J..:? "delta")
		<*> (fields J..:? "otherAccount")
		<*> (fields J..:? "rentalFee")
		<*> (fields J..:? "rentalFeeSink")
		<*> (fields J..:? "newPart")
		<*> (fields J..:? "parent")
		<*> (fields J..:? "amount")

instance A.HasAvroSchema NemTransaction where
	schema = genericAvroSchema
instance A.ToAvro NemTransaction where
	toAvro = genericToAvro
instance ToPostgresText NemTransaction
