{-# LANGUAGE DeriveGeneric, OverloadedStrings, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Cardano
	( Cardano(..)
	, newCardano
	, CardanoBlock(..)
	, CardanoTransaction(..)
	, CardanoInput(..)
	, CardanoOutput(..)
	) where

import Control.Monad
import qualified Data.Aeson as J
import qualified Data.Aeson.Types as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import qualified Data.HashMap.Strict as HM
import Data.Int
import Data.Monoid
import Data.String
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

-- | Cardano connector.
data Cardano = Cardano
	{ cardano_httpManager :: !H.Manager
	, cardano_httpRequest :: !H.Request
	}

newCardano :: H.Manager -> H.Request -> Cardano
newCardano httpManager httpRequest = Cardano
	{ cardano_httpManager = httpManager
	, cardano_httpRequest = httpRequest
	}

cardanoRequest :: J.FromJSON r => Cardano -> T.Text -> [(B.ByteString, Maybe B.ByteString)] -> IO r
cardanoRequest Cardano
	{ cardano_httpManager = httpManager
	, cardano_httpRequest = httpRequest
	} path params = do
	body <- H.responseBody <$> tryWithRepeat (H.httpLbs (H.setQueryString params httpRequest
		{ H.path = T.encodeUtf8 path
		}) httpManager)
	either fail return $ J.eitherDecode body

instance BlockChain Cardano where
	type Block Cardano = CardanoBlock
	type Transaction Cardano = CardanoTransaction

	-- pageSize param doesn't work anymore
	-- getCurrentBlockHeight cardano = either fail return =<< cardanoRequest cardano "/api/blocks/pages/total" [("pageSize", Just "1")]
	getCurrentBlockHeight cardano = either fail (return . (+ (-8)) . (* 10)) =<< cardanoRequest cardano "/api/blocks/pages/total" []

	getBlockByHeight cardano blockHeight = do
		-- calculate page's index and block's index on page
		let
			pageIndex = (blockHeight + 8) `quot` 10
			blockIndexOnPage = blockHeight - (pageIndex * 10 - 8)
		-- get page with blocks
		pageObject <- either fail return =<< cardanoRequest cardano "/api/blocks/pages"
			[ ("page", Just $ fromString $ show pageIndex)
			, ("pageSize", Just "10")
			]
		blockObject <- either fail return $ flip J.parseEither pageObject $ J.withArray "page" $
			\((V.! 1) -> blocksObjectsObject) ->
			J.withArray "blocks" (J.parseJSON . (V.! fromIntegral blockIndexOnPage)) blocksObjectsObject
		blockHashText <- either fail return $ J.parseEither (J..: "cbeBlkHash") blockObject
		blockTxsBriefObjects <- either fail return =<< cardanoRequest cardano ("/api/blocks/txs/" <> blockHashText) [("limit", Just "1000000000000000000")]
		blockTxs <- forM blockTxsBriefObjects $ \txBriefObject -> do
			txIdText <- either fail return $ J.parseEither (J..: "ctbId") txBriefObject
			either fail return =<< cardanoRequest cardano ("/api/txs/summary/" <> txIdText) []
		either fail return $ J.parseEither (J.parseJSON . J.Object)
			$ HM.insert "height" (J.Number $ fromIntegral blockHeight)
			$ HM.insert "transactions" (J.Array blockTxs)
			blockObject

	blockHeightFieldName _ = "height"

-- API: https://cardanodocs.com/technical/explorer/api

data CardanoBlock = CardanoBlock
	{ cb_height :: {-# UNPACK #-} !Int64
	, cb_epoch :: {-# UNPACK #-} !Int64
	, cb_slot :: {-# UNPACK #-} !Int64
	, cb_hash :: !B.ByteString
	, cb_timeIssued :: {-# UNPACK #-} !Int64
	, cb_totalSent :: !Integer
	, cb_size :: {-# UNPACK #-} !Int64
	, cb_blockLead :: !B.ByteString
	, cb_fees :: !Integer
	, cb_transactions :: !(V.Vector CardanoTransaction)
	} deriving Generic

instance Schemable CardanoBlock

instance J.FromJSON CardanoBlock where
	parseJSON = J.withObject "cardano block" $ \fields -> CardanoBlock
		<$> (fields J..: "height")
		<*> (fields J..: "cbeEpoch")
		<*> (fields J..: "cbeSlot")
		<*> (decodeHexBytes =<< fields J..: "cbeBlkHash")
		<*> (fields J..: "cbeTimeIssued")
		<*> (decodeValue =<< fields J..: "cbeTotalSent")
		<*> (fields J..: "cbeSize")
		<*> (decodeHexBytes =<< fields J..: "cbeBlockLead")
		<*> (decodeValue =<< fields J..: "cbeFees")
		<*> (fields J..: "transactions")

instance A.HasAvroSchema CardanoBlock where
	schema = genericAvroSchema
instance A.ToAvro CardanoBlock where
	toAvro = genericToAvro
instance ToPostgresText CardanoBlock

data CardanoTransaction = CardanoTransaction
	{ ct_id :: !B.ByteString
	, ct_timeIssued :: {-# UNPACK #-} !Int64
	, ct_fees :: !Integer
	, ct_inputs :: !(V.Vector CardanoInput)
	, ct_outputs :: !(V.Vector CardanoOutput)
	} deriving Generic

instance Schemable CardanoTransaction
instance SchemableField CardanoTransaction

instance J.FromJSON CardanoTransaction where
	parseJSON = J.withObject "cardano transaction" $ \fields -> CardanoTransaction
		<$> (decodeHexBytes =<< fields J..: "ctsId")
		<*> (fields J..: "ctsTxTimeIssued")
		<*> (decodeValue =<< fields J..: "ctsFees")
		<*> (fields J..: "ctsInputs")
		<*> (fields J..: "ctsOutputs")

instance A.HasAvroSchema CardanoTransaction where
	schema = genericAvroSchema
instance A.ToAvro CardanoTransaction where
	toAvro = genericToAvro
instance ToPostgresText CardanoTransaction

data CardanoInput = CardanoInput
	{ ci_address :: !T.Text
	, ci_value :: !Integer
	} deriving Generic

instance Schemable CardanoInput
instance SchemableField CardanoInput

instance J.FromJSON CardanoInput where
	parseJSON = J.withArray "cardano input" $ \fields -> do
		unless (V.length fields == 2) $ fail "wrong cardano input array"
		CardanoInput
			<$> (J.parseJSON $ fields V.! 0)
			<*> (decodeValue $ fields V.! 1)

instance A.HasAvroSchema CardanoInput where
	schema = genericAvroSchema
instance A.ToAvro CardanoInput where
	toAvro = genericToAvro
instance ToPostgresText CardanoInput

data CardanoOutput = CardanoOutput
	{ co_address :: !T.Text
	, co_value :: !Integer
	} deriving Generic

instance Schemable CardanoOutput
instance SchemableField CardanoOutput

instance J.FromJSON CardanoOutput where
	parseJSON = J.withArray "cardano output" $ \fields -> do
		unless (V.length fields == 2) $ fail "wrong cardano output array"
		CardanoOutput
			<$> (J.parseJSON $ fields V.! 0)
			<*> (decodeValue $ fields V.! 1)

instance A.HasAvroSchema CardanoOutput where
	schema = genericAvroSchema
instance A.ToAvro CardanoOutput where
	toAvro = genericToAvro
instance ToPostgresText CardanoOutput

decodeValue :: J.Value -> J.Parser Integer
decodeValue = J.withObject "cardano value" $ \fields -> (read . T.unpack) <$> fields J..: "getCoin"
