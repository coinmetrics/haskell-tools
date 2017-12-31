{-# LANGUAGE OverloadedStrings, DeriveGeneric #-}

module CoinMetrics.Ethereum.ERC20
	( ERC20Info(..)
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import qualified Data.ByteString as B
import qualified Data.Text as T
import GHC.Generics(Generic)

import CoinMetrics.Ethereum.Util
import CoinMetrics.Schema
import CoinMetrics.Schema.Avro

data ERC20Info = ERC20Info
	{ ei_contractAddress :: !B.ByteString
	, ei_name :: !T.Text
	} deriving Generic

instance Schemable ERC20Info

instance J.FromJSON ERC20Info where
	parseJSON = J.withObject "ERC20Info" $ \fields -> ERC20Info
		<$> (decodeHexBytes  =<< fields J..: "contractAddress")
		<*> (J.parseJSON     =<< fields J..: "name")

instance A.HasAvroSchema ERC20Info where
	schema = genericAvroSchema
instance A.ToAvro ERC20Info where
	toAvro = genericToAvro
