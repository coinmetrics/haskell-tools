{-# LANGUAGE DefaultSignatures, DeriveGeneric, FlexibleContexts, FlexibleInstances, GeneralizedNewtypeDeriving, LambdaCase, StandaloneDeriving, TypeOperators, ViewPatterns #-}

module CoinMetrics.Schema
	( Schema(..)
	, SchemaField(..)
	, SchemaFieldMode(..)
	, SchemaFieldType(..)
	, Schemable(..)
	, SchemableField(..)
	) where

import qualified Data.ByteString as B
import Data.Int
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T
import qualified Data.Vector as V
import qualified GHC.Generics as G

import CoinMetrics.Schema.Util

data Schema = Schema
	{ schema_name :: !T.Text
	, schema_fields :: !(V.Vector SchemaField)
	} deriving Show

data SchemaField = SchemaField
	{ schemaField_mode :: !SchemaFieldMode
	, schemaField_name :: !T.Text
	, schemaField_type :: !SchemaFieldType
	} deriving Show

data SchemaFieldMode
	= SchemaFieldMode_required
	| SchemaFieldMode_optional
	| SchemaFieldMode_repeated
	deriving Show

data SchemaFieldType
	= SchemaFieldType_bytes
	| SchemaFieldType_string
	| SchemaFieldType_int64
	| SchemaFieldType_integer
	| SchemaFieldType_float
	| SchemaFieldType_bool
	| SchemaFieldType_record
		{ schemaFieldType_schema :: !Schema
		}
	deriving Show

class Schemable a where
	schemaOf :: Proxy a -> Schema
	default schemaOf :: (G.Generic a, GenericSchemableDatatype (G.Rep a)) => Proxy a -> Schema
	schemaOf = genericSchemaOfDatatype . fmap G.from

class GenericSchemableDatatype f where
	genericSchemaOfDatatype :: Proxy (f p) -> Schema

class GenericSchemableConstructor f where
	genericSchemaOfConstructor :: Proxy (f p) -> Schema

class GenericSchemableSelector f where
	genericSchemaOfSelector :: Proxy (f p) -> [SchemaField]

class GenericSchemableValue f where
	genericSchemaFieldTypeOfValue :: Proxy (f p) -> SchemaFieldType
	genericSchemaFieldModeOfValue :: Proxy (f p) -> SchemaFieldMode

instance (G.Datatype c, GenericSchemableConstructor f) => GenericSchemableDatatype (G.M1 G.D c f) where
	genericSchemaOfDatatype = genericSchemaOfConstructor . fmap G.unM1

instance (G.Constructor c, GenericSchemableSelector f) => GenericSchemableConstructor (G.M1 G.C c f) where
	genericSchemaOfConstructor c@(fmap G.unM1 -> p) = Schema
		{ schema_name = T.pack $ G.conName $ asProxyTypeOf undefined c
		, schema_fields = V.fromList $ genericSchemaOfSelector p
		}

instance (G.Selector c, GenericSchemableValue f) => GenericSchemableSelector (G.M1 G.S c f) where
	genericSchemaOfSelector s@(fmap G.unM1 -> p) = [SchemaField
		{ schemaField_mode = genericSchemaFieldModeOfValue p
		, schemaField_name = T.pack $ stripBeforeUnderscore $ G.selName $ asProxyTypeOf undefined s
		, schemaField_type = genericSchemaFieldTypeOfValue p
		}]

instance (GenericSchemableSelector a, GenericSchemableSelector b) => GenericSchemableSelector (a G.:*: b) where
	genericSchemaOfSelector = f Proxy Proxy where
		f :: (GenericSchemableSelector a, GenericSchemableSelector b) => Proxy (a p) -> Proxy (b p) -> Proxy ((a G.:*: b) p) -> [SchemaField]
		f pa pb _ = genericSchemaOfSelector pa ++ genericSchemaOfSelector pb

instance SchemableField a => GenericSchemableValue (G.K1 G.R a) where
	genericSchemaFieldTypeOfValue = schemaFieldTypeOf . fmap G.unK1
	genericSchemaFieldModeOfValue = schemaFieldModeOf . fmap G.unK1

class SchemableField a where
	schemaFieldTypeOf :: Proxy a -> SchemaFieldType
	default schemaFieldTypeOf :: Schemable a => Proxy a -> SchemaFieldType
	schemaFieldTypeOf = SchemaFieldType_record . schemaOf
	schemaFieldModeOf :: Proxy a -> SchemaFieldMode
	schemaFieldModeOf _ = SchemaFieldMode_required

instance SchemableField B.ByteString where
	schemaFieldTypeOf _ = SchemaFieldType_bytes

instance SchemableField T.Text where
	schemaFieldTypeOf _ = SchemaFieldType_string

instance SchemableField Int64 where
	schemaFieldTypeOf _ = SchemaFieldType_int64

instance SchemableField Integer where
	schemaFieldTypeOf _ = SchemaFieldType_integer

instance SchemableField Double where
	schemaFieldTypeOf _ = SchemaFieldType_float

instance SchemableField Bool where
	schemaFieldTypeOf _ = SchemaFieldType_bool

instance SchemableField a => SchemableField (Maybe a) where
	schemaFieldTypeOf = schemaFieldTypeOf . fmap (fromMaybe undefined)
	schemaFieldModeOf _ = SchemaFieldMode_optional

instance SchemableField a => SchemableField [a] where
	schemaFieldTypeOf = schemaFieldTypeOf . fmap head
	schemaFieldModeOf _ = SchemaFieldMode_repeated

instance SchemableField a => SchemableField (V.Vector a) where
	schemaFieldTypeOf = schemaFieldTypeOf . fmap V.head
	schemaFieldModeOf _ = SchemaFieldMode_repeated

deriving instance SchemableField Base64ByteString
deriving instance SchemableField Base16ByteString
