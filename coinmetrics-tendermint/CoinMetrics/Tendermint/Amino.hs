{-# LANGUAGE LambdaCase, OverloadedLists #-}

module CoinMetrics.Tendermint.Amino
  ( ReadMsg(..)
  , withFields
  , readSubStruct
  , readRepeated
  , readRequired
  , readOptional
  , withPrefix
  , withLen
  ) where

import Control.Applicative
import Control.Monad
import qualified Data.ByteString.Short as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.ProtocolBuffers.Internal as P
import qualified Data.Serialize as S
import qualified Data.Vector as V
import Data.Word

import CoinMetrics.Util

class ReadMsg a where
  readMsg :: S.Get a

withFields :: String -> (HM.HashMap P.Tag [P.WireField] -> S.Get a) -> S.Get a
withFields _name g = g . HM.map reverse =<< step HM.empty where
  step h = do
    f <- Just <$> P.getWireField <|> return Nothing
    case f of
      Just v -> step $ HM.insertWith (\[x] xs -> x : xs) (P.wireFieldTag v) [v] h
      Nothing -> return h

readSubStruct :: ReadMsg a => P.WireField -> S.Get a
readSubStruct = \case
  P.DelimitedField _ bytes -> either fail return $ S.runGet readMsg bytes
  f -> fail $ show ("wrong field", f)

readRepeated :: (P.WireField -> S.Get a) -> P.Tag -> HM.HashMap P.Tag [P.WireField] -> S.Get (V.Vector a)
readRepeated f t h = mapM f $ maybe [] V.fromList $ HM.lookup t h

readRequired :: (P.WireField -> S.Get a) -> P.Tag -> HM.HashMap P.Tag [P.WireField] -> S.Get a
readRequired f t h = case HM.lookup t h of
  Just [a] -> f a
  _ -> fail $ show ("can't read required value", t, h)

readOptional :: (P.WireField -> S.Get a) -> P.Tag -> HM.HashMap P.Tag [P.WireField] -> S.Get (Maybe a)
readOptional f t h = case HM.lookup t h of
  Just [a] -> Just <$> f a
  Nothing -> return Nothing
  _ -> fail $ show ("can't read optional value", t, h)

withPrefix :: Word32 -> S.Get a -> S.Get a
withPrefix prefix get = do
  realPrefix <- S.getWord32be
  unless (prefix == realPrefix) $ fail $ show ("wrong prefix", prefix, realPrefix)
  get

withLen :: S.Get a -> S.Get a
withLen get = do
  len <- P.getVarInt
  S.isolate len get

instance P.DecodeWire HexString where
  decodeWire = fmap (HexString . BS.toShort) . P.decodeWire
