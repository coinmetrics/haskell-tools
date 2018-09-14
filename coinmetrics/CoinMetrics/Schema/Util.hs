{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings, TemplateHaskell #-}

module CoinMetrics.Schema.Util
	( genSchemaInstances
	, schemaInstancesDecs
	) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import Language.Haskell.TH

import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

schemaJsonOptions :: J.Options
schemaJsonOptions = J.defaultOptions
	{ J.fieldLabelModifier = stripBeforeUnderscore
	, J.constructorTagModifier = stripBeforeUnderscore
	}

genSchemaInstances :: [Name] -> Q [Dec]
genSchemaInstances = fmap mconcat . mapM schemaInstancesDecs

schemaInstancesDecs :: Name -> Q [Dec]
schemaInstancesDecs typeName = sequence
	[ instanceD (pure []) [t| Schemable $(conT typeName) |] []
	, instanceD (pure []) [t| SchemableField $(conT typeName) |] []
	, instanceD (pure []) [t| J.FromJSON $(conT typeName) |]
		[ funD 'J.parseJSON [clause [] (normalB [| J.genericParseJSON schemaJsonOptions |] ) []]
		]
	, instanceD (pure []) [t| J.ToJSON $(conT typeName) |]
		[ funD 'J.toJSON [clause [] (normalB [| J.genericToJSON schemaJsonOptions |] ) []]
		, funD 'J.toEncoding [clause [] (normalB [| J.genericToEncoding schemaJsonOptions |] ) []]
		]
	, instanceD (pure []) [t| A.HasAvroSchema $(conT typeName) |]
		[ funD 'A.schema [clause [] (normalB [| genericAvroSchema |] ) []]
		]
	, instanceD (pure []) [t| A.ToAvro $(conT typeName) |]
		[ funD 'A.toAvro [clause [] (normalB [| genericToAvro |] ) []]
		]
	, instanceD (pure []) [t| ToPostgresText $(conT typeName) |] []
	]
