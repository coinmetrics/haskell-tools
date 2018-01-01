{-# LANGUAGE FlexibleContexts, FlexibleInstances, OverloadedStrings, TypeOperators #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module CoinMetrics.Schema.Avro
	( genericAvroSchema
	, genericToAvro
	) where

import qualified Data.Avro as A
import qualified Data.Avro.Schema as AS
import qualified Data.Avro.Types as AT
import qualified Data.HashMap.Lazy as HML
import Data.Proxy
import Data.Tagged
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified GHC.Generics as G

import CoinMetrics.Schema.Util

-- Generic schema

genericAvroSchema :: (G.Generic a, GenericAvroSchemableDatatype (G.Rep a)) => Tagged a AS.Type
genericAvroSchema = f Proxy where
	f :: (G.Generic a, GenericAvroSchemableDatatype (G.Rep a)) => Proxy a -> Tagged a AS.Type
	f = Tagged . genericAvroSchemaOfDatatype . fmap G.from

genericToAvro :: (G.Generic a, GenericAvroSchemableDatatype (G.Rep a)) => a -> AT.Value AS.Type
genericToAvro = genericToAvroDatatype . G.from

class GenericAvroSchemableDatatype f where
	genericAvroSchemaOfDatatype :: Proxy (f p) -> AS.Type
	genericToAvroDatatype :: f p -> AT.Value AS.Type

class GenericAvroSchemableConstructor f where
	genericAvroSchemaOfConstructor :: Proxy (f p) -> AS.Type
	genericToAvroConstructor :: f p -> AT.Value AS.Type

class GenericAvroSchemableSelector f where
	genericAvroSchemaOfSelector :: Proxy (f p) -> [AS.Field]
	genericToAvroSelector :: f p -> HML.HashMap T.Text (AT.Value AS.Type)

class GenericAvroSchemableValue f where
	genericAvroSchemaOfValue :: Proxy (f p) -> AS.Type
	genericToAvroValue :: f p -> AT.Value AS.Type

instance (G.Datatype c, GenericAvroSchemableConstructor f) => GenericAvroSchemableDatatype (G.M1 G.D c f) where
	genericAvroSchemaOfDatatype = genericAvroSchemaOfConstructor . fmap G.unM1
	genericToAvroDatatype = genericToAvroConstructor . G.unM1

instance (G.Constructor c, GenericAvroSchemableSelector f) => GenericAvroSchemableConstructor (G.M1 G.C c f) where
	genericAvroSchemaOfConstructor c = AS.Record
		{ AS.name = AS.TN $ T.pack $ G.conName $ asProxyTypeOf undefined c
		, AS.namespace = Just "CoinMetrics"
		, AS.aliases = []
		, AS.doc = Nothing
		, AS.order = Nothing
		, AS.fields = genericAvroSchemaOfSelector $ fmap G.unM1 c
		}
	genericToAvroConstructor c = AT.Record (genericAvroSchemaOfConstructor $ pc c) (genericToAvroSelector $ G.unM1 c) where
		pc :: a -> Proxy a
		pc _ = Proxy

instance (G.Selector c, GenericAvroSchemableValue f) => GenericAvroSchemableSelector (G.M1 G.S c f) where
	genericAvroSchemaOfSelector s = [AS.Field
		{ AS.fldName = T.pack $ stripBeforeUnderscore $ G.selName $ asProxyTypeOf undefined s
		, AS.fldAliases = []
		, AS.fldDoc = Nothing
		, AS.fldOrder = Nothing
		, AS.fldType = genericAvroSchemaOfValue $ fmap G.unM1 s
		, AS.fldDefault = Nothing
		}]
	genericToAvroSelector s = HML.singleton (T.pack $ stripBeforeUnderscore $ G.selName s) (genericToAvroValue $ G.unM1 s)

instance (GenericAvroSchemableSelector a, GenericAvroSchemableSelector b) => GenericAvroSchemableSelector (a G.:*: b) where
	genericAvroSchemaOfSelector = f Proxy Proxy where
		f :: (GenericAvroSchemableSelector a, GenericAvroSchemableSelector b) => Proxy (a p) -> Proxy (b p) -> Proxy ((a G.:*: b) p) -> [AS.Field]
		f pa pb _ = genericAvroSchemaOfSelector pa ++ genericAvroSchemaOfSelector pb
	genericToAvroSelector (a G.:*: b) = HML.union (genericToAvroSelector a) (genericToAvroSelector b)

instance A.ToAvro a => GenericAvroSchemableValue (G.K1 G.R a) where
	genericAvroSchemaOfValue = unTagged . f . fmap G.unK1 where
		f :: A.HasAvroSchema a => Proxy a -> Tagged a AS.Type
		f Proxy = A.schema
	genericToAvroValue = A.toAvro . G.unK1

-- Schema for some types

instance A.HasAvroSchema a => A.HasAvroSchema (V.Vector a) where
	schema = f A.schema where
		f :: A.HasAvroSchema a => Tagged a AS.Type -> Tagged (V.Vector a) AS.Type
		f g = Tagged AS.Array
			{ AS.item = unTagged g
			}

instance A.ToAvro a => A.ToAvro (V.Vector a) where
	toAvro = AT.Array . V.map A.toAvro

-- unfortunately have to use double for Integer
instance A.HasAvroSchema Integer where
	schema = Tagged $ unTagged (A.schema :: Tagged Double AS.Type)

instance A.ToAvro Integer where
	toAvro = A.toAvro . (fromIntegral :: Integer -> Double)
