{-# LANGUAGE OverloadedStrings, TupleSections, MultiWayIf #-}
{--
Copyright (c) 2015 Tim Düsterhus

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
--}

import qualified Codec.Compression.Zlib as Zlib (compress)
import Control.Applicative
import Control.Monad
import Control.Monad.Trans.Maybe
import qualified Crypto.Hash.SHA1 as SHA1
import qualified Data.Aeson as JSON
import Data.Binary
import Data.Bits
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Char (toLower)
import Data.Hex
import Data.Int
import Data.List
import Data.List.Split (splitOn)
import Data.Maybe
import Data.Text.Encoding
import Prelude hiding (readFile)
import System.Directory (copyFile, createDirectoryIfMissing, doesFileExist)
import System.Environment
import System.IO hiding (readFile)

import qualified Data.Map as Map 
import Control.Concurrent.MVar
import System.IO.Unsafe

cache f = do
    r <- newMVar Map.empty
    return $ \a b c -> do
        cacheMap <- takeMVar r
        case Map.lookup (a, b, c) cacheMap of
            Just y -> do
                putMVar r cacheMap
                return y
            Nothing -> do
                y <- f a b c
                putMVar r (Map.insert (a, b, c) y cacheMap)
                return y

version :: Int
version = 1

type FileSize = Int64
type BlockID = B.ByteString

data FSType = File | Dir deriving (Show, Eq)
data FS =
        FSFile { fileSize  :: FileSize
               , block_ids :: [BlockID]
               }
        |
        FSDir { dirEntries :: [DirEntry]
              }
        deriving (Show)

instance JSON.ToJSON FS where
        toJSON (FSFile fileSize block_ids) =
                JSON.object [ "block_ids" JSON..= map (bsToHex) block_ids
                            , "size"      JSON..= fileSize
                            , "type"      JSON..= (fromIntegral $ int32FromFsType File :: Int)
                            , "version"   JSON..= version
                            ]
        toJSON (FSDir dirEntries) =
                JSON.object [ "dirents"   JSON..= dirEntries
                            , "type"      JSON..= (fromIntegral $ int32FromFsType Dir :: Int)
                            , "version"   JSON..= version
                            ]
                
instance JSON.ToJSON DirEntry where
        toJSON (DirEntry mode entry_id name (Just commit) (Just fs))
                -- dir
                | mode .&. 0o0040000 > 0 = JSON.object [ "id"       JSON..= B.unpack entry_id
                                                       , "mode"     JSON..= (fromIntegral mode :: Int)
                                                       , "mtime"    JSON..= ctime commit
                                                       , "name"     JSON..= decodeUtf8 name
                                                       ]
                -- file
                | mode .&. 0o0100000 > 0 = JSON.object [ "id"       JSON..= B.unpack entry_id
                                                       , "mode"     JSON..= (fromIntegral mode :: Int)
                                                       , "modifier" JSON..= creator_name commit
                                                       , "mtime"    JSON..= ctime commit
                                                       , "size"     JSON..= fileSize fs
                                                       , "name"     JSON..= decodeUtf8 name
                                                       ]

data DirEntry = DirEntry { mode     :: Int32
                         , entry_id :: BlockID
                         , name     :: B.ByteString
                         , commit   :: Maybe Commit
                         , fs       :: Maybe FS
                         } deriving (Show)

data Commit = Commit { commit_id        :: String
                     , root_id          :: String
                     , repo_id          :: String
                     , creator_name     :: String
                     , creator          :: String
                     , description      :: String
                     , ctime            :: Int64
                     , parent_id        :: Maybe String
                     , second_parent_id :: Maybe String
                     , repo_name        :: String
                     , repo_desc        :: String
                     , repo_category    :: Maybe String
                     , no_local_history :: Int
                     } deriving (Show)

instance Eq Commit where
        a == b = commit_id a == commit_id b

instance JSON.FromJSON Commit where
        parseJSON (JSON.Object v) =
                Commit                                      <$>
                (v JSON..:  "commit_id")                    <*>
                (v JSON..:  "root_id")                      <*>
                (v JSON..:  "repo_id")                      <*>
                (v JSON..:  "creator_name")                 <*>
                (v JSON..:  "creator")                      <*>
                (v JSON..:  "description")                  <*>
                (v JSON..:  "ctime")                        <*>
                (fmap join $ v JSON..:? "parent_id")        <*>
                (fmap join $ v JSON..:? "second_parent_id") <*>
                (v JSON..:  "repo_name")                    <*>
                (v JSON..:  "repo_desc")                    <*>
                (fmap join $ v JSON..:? "repo_category")    <*>
                (v JSON..:  "no_local_history")
instance JSON.ToJSON Commit where
        toJSON c =
                JSON.object [ "commit_id"        JSON..= commit_id c
                            , "root_id"          JSON..= root_id c
                            , "repo_id"          JSON..= repo_id c
                            , "creator_name"     JSON..= creator_name c
                            , "creator"          JSON..= creator c
                            , "description"      JSON..= description c
                            , "ctime"            JSON..= ctime c
                            , "parent_id"        JSON..= parent_id c
                            , "second_parent_id" JSON..= second_parent_id c
                            , "repo_name"        JSON..= repo_name c
                            , "repo_desc"        JSON..= repo_desc c
                            , "repo_category"    JSON..= repo_category c
                            , "no_local_history" JSON..= no_local_history c
                            , "version"          JSON..= version
                            ]

fsTypeFromInt32 :: Int32 -> FSType
fsTypeFromInt32 1 = File
fsTypeFromInt32 3 = Dir

int32FromFsType :: FSType -> Int32
int32FromFsType Dir = 3
int32FromFsType File = 1

main = do
        (base_dir:target:library:hash:_) <- getArgs
        putStrLn $ "Attempting to convert library " ++ library ++ " with root commit " ++ hash
        transformCommit base_dir target library hash >>= putStrLn . (++) "Done, your new root is: "

hashToFile :: String -> String
hashToFile (h:a:sh) = h:a:'/':sh

calculateHash :: FS -> B.ByteString
calculateHash (FSDir dirEntries) = SHA1.finalize . (foldl (\ctx (DirEntry mode entry_id name _ _) -> foldl (SHA1.update) ctx [ entry_id, name, (B.reverse . B.concat $ L.toChunks $ encode mode) ]) SHA1.init) $ dirEntries
calculateHash (FSFile fileSize block_ids) = SHA1.finalize . (foldl (SHA1.update) SHA1.init) $ block_ids

calculateCommitId :: Commit -> B.ByteString
calculateCommitId commit = SHA1.finalize $ SHA1.init `SHA1.updates` (
                map ($ commit) [ B.concat . flip (:) ["\0"] . B.pack . root_id
                               , B.concat . flip (:) ["\0"] . B.pack . creator
                               , B.concat . flip (:) ["\0"] . B.pack . creator_name
                               , B.concat . flip (:) ["\0"] . B.pack . description
                               , B.concat . L.toChunks . encode . ctime
                               ]
        )

-- | Converts a byte stream to a string of it's lower hexadecimal representation
bsToHex :: B.ByteString -> String
bsToHex = map toLower . hex . B.unpack

-- | Copies over the block to it's new storage location after checking it
copyBlock :: String -> String -> String -> String -> IO ()
copyBlock source_dir target_dir library hash = do
        createDirectoryIfMissing True $ target_dir ++ "/blocks/" ++ library ++ "/" ++ take 2 hash
        exists <- doesFileExist (target_dir ++ "/blocks/" ++ library ++ "/" ++ hashToFile hash)
        if exists
                then
                        return ()
                else do
                        checkBlock source_dir library hash
                        copyFile (source_dir ++ "/blocks/" ++ library ++ "/" ++ hashToFile hash)
                                 (target_dir ++ "/blocks/" ++ library ++ "/" ++ hashToFile hash)

-- | Checks the given block for consistency (whether it's hash matches the contents)
checkBlock :: String -> String -> String -> IO ()
checkBlock base_dir library hash = do
        withFile (base_dir ++ "/blocks/" ++ library ++ "/" ++ hashToFile hash) ReadMode $ \handle -> do
                contents <- liftM (L.concat) $ readUntilEOF (flip L.hGet 1024) handle
                let calculatedHash = bsToHex $ SHA1.hashlazy contents 
                if | hash == calculatedHash -> return ()
                   | otherwise              -> error $ "Hash of block " ++ hash ++ " of library " ++ library ++ " does not match contents"

readCommit :: String -> String -> String -> IO (Maybe Commit)
readCommit base_dir library hash = do
        let filename = (base_dir ++ "/commits/" ++ library ++ "/" ++ hashToFile hash)
        exists <- doesFileExist filename
        if exists
                then
                        withFile filename ReadMode $ \handle -> do
                                contents <- liftM (L.concat) $ readUntilEOF (flip L.hGet 1024) handle
                                
                                case JSON.eitherDecode contents of
                                        Left message -> error message
                                        Right commit  -> return $ Just commit
                else
                        return Nothing
                

findCommit foundCommits readCommit readFS base_dir library commit_hash filename = runMaybeT $ do
        let folders = filter (not.null) $ splitOn "/" filename
        commit        <- MaybeT $ readCommit base_dir library commit_hash
        (fs_hash, fs) <- MaybeT $ findFsByFilename readCommit readFS base_dir library commit_hash filename
        
        MaybeT $ findCommit' foundCommits readCommit readFS base_dir library commit_hash filename fs_hash

findCommit' doneMap readCommit readFS base_dir library commit_hash filename fs_hash = do
	let key = (commit_hash, filename, fs_hash)

        doneMap' <- takeMVar doneMap
        case Map.lookup key doneMap' of
                Just hash -> do
                        putMVar doneMap doneMap'
                        return hash
                Nothing -> do
                        putMVar doneMap doneMap'
			let folders = filter (not.null) $ splitOn "/" filename
			
			commit <- readCommit base_dir library commit_hash
			result <- case commit of
				Nothing -> return Nothing
				Just commit -> do
					-- Read FS of parent commit
					case (parent_id commit) of
						Nothing -> return $ Just commit_hash
						Just parent -> do
							parentFs <- findFsByFilename readCommit readFS base_dir library parent filename
							case parentFs of
								Nothing             -> return $ Just commit_hash
								Just (fs_hash', fs) -> do
									if fs_hash /= fs_hash'
										then
											-- Parent FS differs, return commit_hash
											return $ Just commit_hash
										else
											-- Otherwise recurse into parent
											findCommit' doneMap readCommit readFS base_dir library parent filename fs_hash
			doneMap' <- takeMVar doneMap
			if (Map.size doneMap') > 6000000
				then do
					putStrLn "Throwing away findCommit cache"
					putMVar doneMap Map.empty
				else
					putMVar doneMap (Map.insert key result doneMap')
			return result

findFsByFilename readCommit readFS base_dir library commit_hash filename = runMaybeT $ do
        let parts   = filter (not.null) $ splitOn "/" filename
        commit <- MaybeT $ readCommit base_dir library commit_hash
        let fs_hash = root_id commit
        fs     <- MaybeT $ readFS base_dir library fs_hash
        let root    = (fs_hash, fs)
        if null filename
                then
                        return root
                else
                        foldM (\(_, fs) part -> do
                                let dirEntry = liftM (B.unpack.entry_id) $ findDirEntry part (dirEntries fs)
                                MaybeT $ case dirEntry of
                                        Nothing -> return Nothing
                                        (Just entryHash) -> do
                                                fs <- readFS base_dir library entryHash
                                                return $ liftM (entryHash,) fs
                        ) root parts

findDirEntry :: String -> [DirEntry] -> Maybe DirEntry
findDirEntry filename = find (\x -> B.unpack (name x) == filename)

readFS :: String -> String -> String -> IO (Maybe FS)
readFS base_dir library hash = do
        let filename = (base_dir ++ "/fs/" ++ library ++ "/" ++ hashToFile hash)
        exists <- doesFileExist filename
        if exists
                then
                        withFile filename ReadMode $ \handle -> do
                                fsType <- liftM (fsTypeFromInt32 . decode) $ L.hGet handle 4
                                fs     <- case fsType of
                                        Dir  -> readDirEntries handle
                                        File -> do
                                                fs <- readFile handle
                                                let blocks = map (bsToHex) $ block_ids fs
                                        --        mapM (checkBlock base_dir library) blocks
                                                return fs
                                let calculatedHash = bsToHex . calculateHash $ fs
                                
                                if | hash == calculatedHash -> return $ Just fs
                                   | otherwise              -> error  $ "Hash of " ++ (show fsType) ++ " " ++ hash ++ " of library " ++ library ++ " does not match contents"
                else
                        return Nothing

-- | Writes the given FS to the appropriate new storage location and returns the hash.
writeFS :: String -> String -> FS -> IO (String, FS)
writeFS base_dir library fs = do
        let json       = JSON.encode fs
        let compressed = Zlib.compress json
        let hash       = bsToHex $ SHA1.hashlazy json
        exists <- doesFileExist (base_dir ++ "/fs/" ++ library ++ "/" ++ (hashToFile hash))
        if exists
                then
                        return (hash, fs)
                else do
                        createDirectoryIfMissing True $ base_dir ++ "/fs/" ++ library ++ "/" ++ take 2 hash
                        withFile (base_dir ++ "/fs/" ++ library ++ "/" ++ (hashToFile hash)) WriteMode $ flip L.hPut compressed
                        return (hash, fs)

transformFS _            _          _      _          _          _       _           _        "0000000000000000000000000000000000000000" = return ("0000000000000000000000000000000000000000", FSFile 0 [ ])
transformFS foundCommits readCommit readFS source_dir target_dir library commit_hash filename hash = do
        (Just fs) <- readFS source_dir library hash
        
        case fs of
                (FSFile _ blocks) -> do
                --        mapM (copyBlock source_dir target_dir library) $ map (bsToHex) blocks
                        return fs
                (FSDir dirEntries) -> do
                        dirEntries' <- mapM findDirEntryInformation dirEntries
                        return $ FSDir dirEntries'
        >>= writeFS target_dir library
        where
        findDirEntryInformation :: DirEntry -> IO DirEntry
        findDirEntryInformation (DirEntry mode entry_id name _ _) = do 
                changed_in_hash  <- findCommit foundCommits readCommit readFS source_dir library commit_hash $ filename ++ "/" ++ B.unpack name
                changed_in <- case changed_in_hash of
                        Nothing -> readCommit source_dir library commit_hash
                        (Just changed_in_hash) -> readCommit source_dir library changed_in_hash
                (entry_id', fs) <- transformFS foundCommits readCommit readFS source_dir target_dir library commit_hash (filename ++ "/" ++ B.unpack name) $ B.unpack entry_id
                
                return $ DirEntry mode (B.pack entry_id') name changed_in (Just fs)

transformCommit :: String -> String -> String -> String -> IO String
transformCommit source_dir target_dir library hash = do
        readCommit   <- cache readCommit
        readFS       <- cache readFS
        doneMap      <- newMVar Map.empty
	foundCommits <- newMVar Map.empty
        transformCommit' doneMap foundCommits readCommit readFS source_dir target_dir library hash

transformCommit' doneMap foundCommits readCommit readFS source_dir target_dir library hash = do
        doneMap' <- takeMVar doneMap
        case Map.lookup hash doneMap' of
                Just hash -> do
                        putMVar doneMap doneMap'
                        return hash
                Nothing -> do
                        putMVar doneMap doneMap'
                        (Just commit) <- readCommit source_dir library hash
                        newParent <- parentHash $ parent_id commit
                        newParent2 <- parentHash $ second_parent_id commit
                        let commit' = commit { parent_id = newParent, second_parent_id = newParent2 }
                        (root_id', _) <- transformFS foundCommits readCommit readFS source_dir target_dir library hash "" $ root_id commit'
                        let commit'' = commit' { root_id = root_id' }
                        (hash', _) <- writeCommit target_dir library commit''
                        doneMap' <- takeMVar doneMap
                        putMVar doneMap (Map.insert hash hash' doneMap')
			foundCommits' <- takeMVar foundCommits
			putStrLn $ "findCommit cache size is " ++ (show $ Map.size foundCommits')
			putMVar foundCommits foundCommits'
                        return hash'
                        where
                        parentHash Nothing = return Nothing
                        parentHash (Just parent) = fmap (Just) $ transformCommit' doneMap foundCommits readCommit readFS  source_dir target_dir library parent

writeCommit :: String -> String -> Commit -> IO (String, Commit)
writeCommit base_dir library commit = do
        let hash       = bsToHex $ calculateCommitId commit
        let commit'    = commit { commit_id = hash }
        putStrLn $ (commit_id commit) ++ " -> " ++ (commit_id commit')
        let json       = JSON.encode commit'
        exists <- doesFileExist (base_dir ++ "/commits/" ++ library ++ "/" ++ (hashToFile hash))
        if exists
                then
                        return (hash, commit')
                else do
                        createDirectoryIfMissing True $ base_dir ++ "/commits/" ++ library ++ "/" ++ take 2 hash
                        withFile (base_dir ++ "/commits/" ++ library ++ "/" ++ (hashToFile hash)) WriteMode $ flip L.hPut json
                        return (hash, commit')

-- | Reads an FSDir from the given handle
readDirEntries :: Handle -> IO FS
readDirEntries = liftM (FSDir) . readUntilEOF (readDirEntry)
        where
        readDirEntry :: Handle -> IO DirEntry
        readDirEntry handle = do
                mode     <- liftM (decode) $ L.hGet handle 4
                entry_id <-                  B.hGet handle 40
                name_len <- liftM (decode) $ L.hGet handle 4
                name     <-                  B.hGet handle $ fromIntegral (name_len :: Int32)
                return $ DirEntry mode entry_id name Nothing Nothing

-- | Reads a FSFile from the given handle
readFile :: Handle -> IO FS
readFile handle = do
        fileSize  <- liftM (decode) $ L.hGet handle 8
        block_ids <- readUntilEOF (flip B.hGet 20) handle
        return $ FSFile fileSize block_ids

-- | Repeatedly calls the given function until EOF of the handle is reached
readUntilEOF :: (Handle -> IO a) -> Handle -> IO [a]
readUntilEOF reader handle = do
        eof <- hIsEOF handle
        if | eof       -> return []
           | otherwise -> do
                current <- reader handle
                next    <- readUntilEOF reader handle
                return $ current : next
