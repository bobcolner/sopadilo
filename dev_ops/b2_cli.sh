# b2 cli

b2 ls polygon-equities bars/

b2 sync [-h] [--noProgress] [--dryRun] [--allowEmptySource] [--excludeAllSymlinks] [--threads THREADS] \
 		[--compareVersions {none,modTime,size}] [--compareThreshold MILLIS] [--excludeRegex REGEX] \
 		[--includeRegex REGEX] [--excludeDirRegex REGEX] [--excludeIfModifiedAfter TIMESTAMP] \
 		[--destinationServerSideEncryption {SSE-B2,SSE-C}] [--destinationServerSideEncryptionAlgorithm {AES256}] \
 		[--sourceServerSideEncryption {SSE-C}] [--sourceServerSideEncryptionAlgorithm {AES256}] \
 		[--skipNewer | --replaceNewer] [--delete | --keepDays DAYS] source destination

b2 sync --threads 44 --compareVersions=none \
    b2://polygon-equities \
    /Users/bobcolner/QuantClarity/sopadilo/tmp/local_data


b2 sync --threads 33 \
    b2://polygon-equities/bars/renko_v3 \
    /Users/bobcolner/QuantClarity/sopadilo/tmp/local_data/bars/renko_v3
