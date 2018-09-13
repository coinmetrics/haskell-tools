{-# LANGUAGE DeriveGeneric, GeneralizedNewtypeDeriving, LambdaCase #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module CoinMetrics.Unified
	( UnifiedBlock(..)
	, UnifiedTransaction(..)
	, UnifiedAction(..)
	, UnifiedEffect(..)
	, UnifiedAccount(..)
	, UnifiedValue(..)
	, UnifiedAsset(..)
	, IsUnifiedBlock(..)
	) where

import qualified Data.Aeson as J
import Data.Int
import qualified Data.Text as T
import qualified Data.Vector as V
import GHC.Generics(Generic)
import Data.Scientific
import Data.Time.Clock

import Hanalytics.Schema

-- | Block is a atomic piece of information generated in blockchain.
data UnifiedBlock = UnifiedBlock
	{ ub_time :: !(Maybe UTCTime)
	, ub_height :: {-# UNPACK #-} !Int64
	, ub_hash :: !T.Text
	, ub_size :: !(Maybe Int64)
	, ub_transactions :: !(V.Vector UnifiedTransaction)
	} deriving Generic
instance J.FromJSON UnifiedBlock where
	parseJSON = J.genericParseJSON jsonOptions
instance J.ToJSON UnifiedBlock where
	toJSON = J.genericToJSON jsonOptions
	toEncoding = J.genericToEncoding jsonOptions

-- | Transaction is an atomic change in blockchain.
data UnifiedTransaction = UnifiedTransaction
	{ ut_time :: !(Maybe UTCTime)
	, ut_hash :: {-# UNPACK #-} !T.Text
	, ut_fee :: !(Maybe UnifiedValue)
	, ut_actions :: !(V.Vector UnifiedAction)
	} deriving Generic
instance J.FromJSON UnifiedTransaction where
	parseJSON = J.genericParseJSON jsonOptions
instance J.ToJSON UnifiedTransaction where
	toJSON = J.genericToJSON jsonOptions
	toEncoding = J.genericToEncoding jsonOptions

-- | Action ordered as part of transaction.
data UnifiedAction = UnifiedAction
	{ ua_time :: !(Maybe UTCTime)
	, ua_from :: !(Maybe UnifiedAccount)
	, ua_to :: !(Maybe UnifiedAccount)
	, ua_value :: !(Maybe UnifiedValue)
	, ua_effects :: !(V.Vector UnifiedEffect)
	} deriving Generic
instance J.FromJSON UnifiedAction where
	parseJSON = J.genericParseJSON jsonOptions
instance J.ToJSON UnifiedAction where
	toJSON = J.genericToJSON jsonOptions
	toEncoding = J.genericToEncoding jsonOptions

-- | Resulting effect of single action.
data UnifiedEffect
	-- | Balance change.
	= UnifiedEffect_balance
		{ ue_account :: !UnifiedAccount
		, ue_value :: !UnifiedValue
		}
	-- | Contract creation.
	| UnifiedEffect_createContract
		{ ue_contract :: !UnifiedAccount
		}
	-- | Contract destruction.
	| UnifiedEffect_destroyContract
		{ ue_contract :: !UnifiedAccount
		}
	-- | Contract call.
	| UnifiedEffect_callContract
		{ ue_contract :: !UnifiedAccount
		}
	deriving Generic
instance J.FromJSON UnifiedEffect where
	parseJSON = J.genericParseJSON jsonOptions
instance J.ToJSON UnifiedEffect where
	toJSON = J.genericToJSON jsonOptions
	toEncoding = J.genericToEncoding jsonOptions

-- | Account or address.
newtype UnifiedAccount = UnifiedAccount T.Text deriving (J.FromJSON, J.ToJSON)

-- | Value.
data UnifiedValue = UnifiedValue
	{ uv_amount :: {-# UNPACK #-} !Scientific
	, uv_asset :: !(Maybe UnifiedAsset)
	} deriving Generic
instance J.FromJSON UnifiedValue where
	parseJSON = J.genericParseJSON jsonOptions
instance J.ToJSON UnifiedValue where
	toJSON = J.genericToJSON jsonOptions
	toEncoding = J.genericToEncoding jsonOptions

instance Num UnifiedValue where
	UnifiedValue { uv_amount = a1, uv_asset = s1 } + UnifiedValue { uv_amount = a2, uv_asset = s2 } | s1 == s2 = UnifiedValue { uv_amount = a1 + a2, uv_asset = s1 }
	_ + _ = error "unequal unified assets"
	UnifiedValue { uv_amount = a1, uv_asset = s1 } - UnifiedValue { uv_amount = a2, uv_asset = s2 } | s1 == s2 = UnifiedValue { uv_amount = a1 - a2, uv_asset = s1 }
	_ - _ = error "unequal unified assets"
	UnifiedValue { uv_amount = a1, uv_asset = s1 } * UnifiedValue { uv_amount = a2, uv_asset = s2 } | s1 == s2 = UnifiedValue { uv_amount = a1 * a2, uv_asset = s1 }
	_ * _ = error "unequal unified assets"
	negate v@(UnifiedValue { uv_amount = a }) = v { uv_amount = negate a }
	abs v@(UnifiedValue { uv_amount = a }) = v { uv_amount = abs a }
	signum v@(UnifiedValue { uv_amount = a }) = v { uv_amount = signum a }
	fromInteger i = UnifiedValue { uv_amount = fromInteger i, uv_asset = Nothing }

-- | Asset.
newtype UnifiedAsset = UnifiedAsset T.Text deriving (Eq, J.FromJSON, J.ToJSON)

class IsUnifiedBlock a where
	unifyBlock :: a -> UnifiedBlock

jsonOptions :: J.Options
jsonOptions = jsonOptionsWithTag "type"

jsonOptionsWithTag :: String -> J.Options
jsonOptionsWithTag tag = J.defaultOptions
	{ J.fieldLabelModifier = stripBeforeUnderscore
	, J.constructorTagModifier = stripBeforeUnderscore
	, J.sumEncoding = J.TaggedObject
		{ J.tagFieldName = tag
		, J.contentsFieldName = "contents"
		}
	}
