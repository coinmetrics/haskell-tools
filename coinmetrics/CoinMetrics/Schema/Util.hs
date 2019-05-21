{-# LANGUAGE GeneralizedNewtypeDeriving, LambdaCase, OverloadedStrings, TemplateHaskell #-}

module CoinMetrics.Schema.Util
  ( genSchemaInstances
  , schemaInstancesDecs
  , schemaInstancesCtxDecs
  , genNewtypeSchemaInstances
  , genJsonInstances
  , schemaJsonOptions
  ) where

import qualified Data.Aeson as J
import qualified Data.Avro as A
import GHC.Generics(Generic)
import Language.Haskell.TH

import Hanalytics.Schema
import Hanalytics.Schema.Avro
import Hanalytics.Schema.Postgres

schemaJsonOptions :: J.Options
schemaJsonOptions = J.defaultOptions
  { J.fieldLabelModifier = stripBeforeUnderscore
  , J.constructorTagModifier = stripBeforeUnderscore
  , J.sumEncoding = J.TaggedObject
    { J.tagFieldName = "type"
    , J.contentsFieldName = "data"
    }
  }

genSchemaInstances :: [Name] -> Q [Dec]
genSchemaInstances = fmap mconcat . mapM (schemaInstancesDecs . conT)

schemaInstancesDecs :: TypeQ -> Q [Dec]
schemaInstancesDecs = schemaInstancesCtxDecs []

schemaInstancesCtxDecs :: [TypeQ] -> TypeQ -> Q [Dec]
schemaInstancesCtxDecs preds con = sequence
  [ standaloneDerivD (pure []) [t| Generic $con |]
  , instanceD (context [ [t| SchemableField |] ]) [t| Schemable $con |] []
  , instanceD (context [ [t| SchemableField |] ]) [t| SchemableField $con |] []
  , instanceD (context [ [t| J.FromJSON |] ]) [t| J.FromJSON $con |]
    [ funD 'J.parseJSON [clause [] (normalB [| J.genericParseJSON schemaJsonOptions |] ) []]
    ]
  , instanceD (context [ [t| J.ToJSON |] ]) [t| J.ToJSON $con |]
    [ funD 'J.toJSON [clause [] (normalB [| J.genericToJSON schemaJsonOptions |] ) []]
    , funD 'J.toEncoding [clause [] (normalB [| J.genericToEncoding schemaJsonOptions |] ) []]
    ]
  , instanceD (context [ [t| A.ToAvro |], [t| A.HasAvroSchema |] ]) [t| A.HasAvroSchema $con |]
    [ funD 'A.schema [clause [] (normalB [| genericAvroSchema |] ) []]
    ]
  , instanceD (context [ [t| A.ToAvro |] ]) [t| A.ToAvro $con |]
    [ funD 'A.toAvro [clause [] (normalB [| genericToAvro |] ) []]
    ]
  , instanceD (context [ [t| SchemableField |], [t| ToPostgresText |] ]) [t| ToPostgresText $con |] []
  ]
  where
    context classes = sequence [cls `appT` prd | cls <- classes, prd <- preds]

genNewtypeSchemaInstances :: [Name] -> Q [Dec]
genNewtypeSchemaInstances = fmap mconcat . mapM (newtypeSchemaInstancesDecs . conT)

newtypeSchemaInstancesDecs :: TypeQ -> Q [Dec]
newtypeSchemaInstancesDecs con = sequence
  [ standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| Schemable $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| SchemableField $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| J.FromJSON $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| J.ToJSON $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| A.HasAvroSchema $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| A.ToAvro $con |]
  , standaloneDerivWithStrategyD (Just NewtypeStrategy) (pure []) [t| ToPostgresText $con |]
  ]

genJsonInstances :: [Name] -> Q [Dec]
genJsonInstances = fmap mconcat . mapM (jsonInstancesDecs . conT)

jsonInstancesDecs :: TypeQ -> Q [Dec]
jsonInstancesDecs con = sequence
  [ standaloneDerivD (pure []) [t| Generic $con |]
  , instanceD (pure []) [t| J.FromJSON $con |]
    [ funD 'J.parseJSON [clause [] (normalB [| J.genericParseJSON schemaJsonOptions |] ) []]
    ]
  , instanceD (pure []) [t| J.ToJSON $con |]
    [ funD 'J.toJSON [clause [] (normalB [| J.genericToJSON schemaJsonOptions |] ) []]
    , funD 'J.toEncoding [clause [] (normalB [| J.genericToEncoding schemaJsonOptions |] ) []]
    ]
  ]
