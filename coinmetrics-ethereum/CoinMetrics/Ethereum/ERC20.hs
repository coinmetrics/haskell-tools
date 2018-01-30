{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}

module CoinMetrics.Ethereum.ERC20
	( ERC20Info(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import Data.Int
import qualified Data.Text as T
import GHC.Generics(Generic)

import CoinMetrics.Ethereum.Util
import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

data ERC20Info = ERC20Info
	{ ei_contractAddress :: !B.ByteString
	, ei_name :: !T.Text
	, ei_symbol :: !T.Text
	, ei_decimals :: !Int64
	} deriving Generic

instance Schemable ERC20Info

instance J.FromJSON ERC20Info where
	parseJSON = J.withObject "ERC20Info" $ \fields -> ERC20Info
		<$> (decodeHexBytes  =<< fields J..: "contractAddress")
		<*> (J.parseJSON     =<< fields J..: "name")
		<*> (J.parseJSON     =<< fields J..: "symbol")
		<*> (J.parseJSON     =<< fields J..: "decimals")

instance A.HasAvroSchema ERC20Info where
	schema = genericAvroSchema
instance A.ToAvro ERC20Info where
	toAvro = genericToAvro
instance ToPostgresText ERC20Info
