{-# LANGUAGE OverloadedStrings #-}

module CoinMetrics.Schema.BigQuery
	( bigQuerySchema
	) where

import qualified Data.Aeson as J
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V

import CoinMetrics.Schema

bigQuerySchema :: Schema -> J.Value
bigQuerySchema Schema
	{ schema_fields = fields
	} = J.Array $ fieldsSchema fields where
	fieldsSchema = V.map $ \SchemaField
		{ schemaField_mode = fieldMode
		, schemaField_name = fieldName
		, schemaField_type = fieldType
		} -> let
		obj = HM.fromList $
			[ ("mode", J.String $ case fieldMode of
				SchemaFieldMode_required -> "REQUIRED"
				SchemaFieldMode_optional -> "NULLABLE"
				SchemaFieldMode_repeated -> "REPEATED"
				)
			, ("name", J.String fieldName)
			, ("type", J.String $ case fieldType of
				SchemaFieldType_bytes -> "BYTES"
				SchemaFieldType_string -> "STRING"
				SchemaFieldType_int64 -> "INTEGER"
				SchemaFieldType_integer -> "FLOAT" -- unfortunately
				SchemaFieldType_float -> "FLOAT"
				SchemaFieldType_bool -> "BOOLEAN"
				SchemaFieldType_record _ -> "RECORD"
				)
			] ++ (case fieldType of
			SchemaFieldType_record Schema
				{ schema_fields = recordSchemaFields
				} -> [("fields", J.Array $ fieldsSchema recordSchemaFields)]
			_ -> []
			)
		in J.Object obj
