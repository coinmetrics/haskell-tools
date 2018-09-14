{-# LANGUAGE FlexibleContexts, TemplateHaskell, TupleSections, TypeFamilies, ViewPatterns #-}

module CoinMetrics.Schema.Flatten
	( Flattenable(..)
	, genFlattenedTypes
	) where

import Data.Int
import Data.List
import qualified Data.Vector as V
import GHC.Generics(Generic)
import Language.Haskell.TH
import Language.Haskell.TH.Syntax

import CoinMetrics.Schema.Util

class Monoid (Flattened a) => Flattenable a where
	type Flattened a :: *
	flatten :: a -> Flattened a

-- | Generates datas and functions for flattened versions of types.
{-
-- decls
data Block { height :: Int64, b :: B, transactions :: Vector Transaction }
data Transaction { t :: T, logs :: Vector Log }
data Log { l :: L }

-- TH
genFlattenedTypes [''Block, ''Transaction, ''Log]

-- produces

data Block_flattened { height :: Int64, b :: B }
data Transaction_flattened { block_height :: Int64, _index :: Int64, t :: T }
data Transaction_flattened { block_height :: Int64, transaction_index :: Int64, _index :: Int64, l :: L }
instance Flattenable Block where
	type Flattened Block = ([Block_flattened], ([Transaction_flattened], [Log_flattened]))
	flatten Block { height = height', b = b', transactions = transactions' } =
		( [Block_flattened { height = height', b = b' }]
		, mconcat $ zipWith (flattenTransaction height') [0..] (V.toList transactions')
		)
flattenTransaction blockHeight i Transaction { t = t', logs = logs' } =
	( [Transaction_flattened { block_height = blockHeight, _index = i, t = t' }]
	, mconcat $ zipWith (flattenLog blockHeight i) [0..] (V.toList logs')
	)
flattenLog blockHeight ti i Log { l = l' } = [Log_flattened { block_height = blockHeight, transaction_index = ti, _index = i, l = l' }]
-}
genFlattenedTypes :: String -> ExpQ -> [(String, Name)] -> Q [Dec]
genFlattenedTypes rootKeyName rootKeyExp types@(map snd -> typesNames) = do
	(rootDecs, _, _) <- flattenType [] (snd rootType)
	return rootDecs
	where

		-- generate declarations, lambda and type for given source type
		flattenType :: [(String, Name)] -> Name -> Q ([Dec], ExpQ, TypeQ)
		flattenType parentIndexNamesAndVars@(unzip -> (parentIndexNames, parentIndexVars)) typeName = do

			-- get information about source type
			TyConI (DataD _cxt _dataName _tyVars _kind [RecC typeConName fields] _deriv) <- reify typeName

			-- split fields into ones need flattening and others
			let (fieldsToFlatten, fieldsToKeep) = mconcat . flip map fields $ \field@(_, _, fieldType) -> case fieldType of
				AppT (ConT (Name (OccName "Vector") _)) (ConT (flip elem typesNames -> True)) ->
					([field], [])
				_ -> ([], [field])

			-- generate some stuff
			let Just (typeTitle, _) = find ((== typeName) . snd) types
			let flattenedTypeName = suffixName "_flattened" typeName
			fieldsToFlattenVars <- mapM genFieldVar fieldsToFlatten
			fieldsToKeepVars <- mapM genFieldVar fieldsToKeep

			-- find prefix used
			let (fst . break (== '_') . nameStr -> fieldPrefix, _, _) = head fieldsToKeep

			-- vars and flags
			let isRoot = null parentIndexNamesAndVars
			indexVar <- newName "index"
			let indexFieldName = prefixName flatFieldPrefix (mkName $ fieldPrefix <> "__index")
			rootKeyVar <- newName rootKeyName
			rootVar <- newName $ fst rootType
			let parentIndexFieldNames = map (prefixName flatFieldPrefix . mkName . (fieldPrefix <>)) parentIndexNames

			-- generate children declarations, lambdas and types
			(mconcat -> childrenDecs, childrenLambdas, childrenFlattenedTypes) <- unzip3 <$> sequence
				[flattenType (if isRoot then [("_" <> typeTitle <> "_" <> rootKeyName, rootKeyVar)] else parentIndexNamesAndVars <> [("_" <> typeTitle <> "_index", indexVar)]) fieldSubTypeName | (_, _, AppT _ (ConT fieldSubTypeName)) <- fieldsToFlatten]

			-- our lambda
			let lambda =
				lamE
					( (if isRoot then [] else [varP indexVar])
					<> [(if isRoot then asP rootVar else id) $ recP typeConName (fieldsPats fieldsToKeep fieldsToKeepVars <> fieldsPats fieldsToFlatten fieldsToFlattenVars)]
					) $
				( if isRoot
					then letE [valD (varP rootKeyVar) (normalB $ appE rootKeyExp (varE rootVar)) []]
					else id
				) $
				(if null fieldsToFlatten then head else tupE)
				( listE
					[ recConE flattenedTypeName
						(  zipWith (\fieldName var -> (fieldName, ) <$> varE var) parentIndexFieldNames parentIndexVars
						<> (if isRoot
							then []
							else [(indexFieldName, ) <$> varE indexVar]
							)
						<> fieldsAssigns fieldsToKeep fieldsToKeepVars
						)
					]
				: zipWith (\childVar childLambda -> [| mconcat (zipWith $(childLambda) [0..] (V.toList $(varE childVar))) |]) fieldsToFlattenVars childrenLambdas
				)

			-- our flattened type
			let flattenedType =
				(if null fieldsToFlatten then head else foldl appT (tupleT (1 + length fieldsToFlatten)))
				(appT listT (conT flattenedTypeName) : childrenFlattenedTypes)

			-- our declarations
			instancesDecs <- schemaInstancesDecs flattenedTypeName
			decs <- sequence $
				[ dataD (pure []) flattenedTypeName [] Nothing
					[ recC flattenedTypeName
						( map (\parentIndexFieldName -> varBangType parentIndexFieldName (bangType unpackBang [t| Int64 |])) parentIndexFieldNames
						<> ( if isRoot then [] else
								[ varBangType indexFieldName (bangType unpackBang [t| Int64 |])
								]
							)
						<> map (pure . prefixFlatField) fieldsToKeep
						)
					]
					[derivClause Nothing [ [t| Generic |] ]]
				]
				<> (if isRoot
					then
						[ instanceD (pure []) [t| Flattenable $(conT typeName) |]
							[ tySynInstD ''Flattened (tySynEqn [conT typeName] flattenedType)
							, funD 'flatten [clause [] (normalB lambda) []]
							]
						]
					else []
					)

			return (decs <> instancesDecs <> childrenDecs, lambda, flattenedType)

		rootType = head types

		-- name functions
		nameStr (Name (OccName name) _) = name
		recreateName f (Name (OccName name) _) = mkName (f name)
		prefixName prefix = recreateName (prefix <>)
		suffixName suffix = recreateName (<> suffix)

		flatFieldPrefix = "flat'"

		prefixFlatField (fieldName, fieldBang, fieldType) = (prefixName flatFieldPrefix fieldName, fieldBang, fieldType)

		fieldsPats = zipWith $ \(fieldName, _, _) fieldVar -> fieldPat fieldName (varP fieldVar)
		fieldsAssigns = zipWith $ \(fieldName, _, _) fieldVar -> pure (prefixName flatFieldPrefix fieldName, VarE fieldVar)
		genFieldVar (fieldName, _, _) = newName $ nameStr fieldName
		unpackBang = bang sourceUnpack sourceStrict
