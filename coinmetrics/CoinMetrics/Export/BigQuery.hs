{-# LANGUAGE OverloadedStrings #-}

module CoinMetrics.Export.BigQuery
	( bigQuerySchema
	) where

import qualified Data.Aeson as A
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V

import CoinMetrics.Schema

bigQuerySchema :: Schema -> A.Value
bigQuerySchema (Schema fields) = A.Array $ fieldsSchema fields where
	fieldsSchema = V.map $ \SchemaField
		{ schemaField_mode = fieldMode
		, schemaField_name = fieldName
		, schemaField_type = fieldType
		} -> let
		obj = HM.fromList $
			[ ("mode", A.String $ case fieldMode of
				SchemaFieldMode_required -> "REQUIRED"
				SchemaFieldMode_optional -> "NULLABLE"
				SchemaFieldMode_repeated -> "REPEATED"
				)
			, ("name", A.String fieldName)
			, ("type", A.String $ case fieldType of
				SchemaFieldType_bytes -> "BYTES"
				SchemaFieldType_string -> "STRING"
				SchemaFieldType_integer -> "INTEGER"
				SchemaFieldType_float -> "FLOAT"
				SchemaFieldType_record _ -> "RECORD"
				)
			] ++ (case fieldType of
			SchemaFieldType_record (Schema recordSchema) -> [("fields", A.Array $ fieldsSchema recordSchema)]
			_ -> []
			)
		in A.Object obj
